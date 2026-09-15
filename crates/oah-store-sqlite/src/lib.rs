//! SQLite store. Dev default `.oah/dev.db`; `--fresh` wipes it.

#![forbid(unsafe_code)]

use async_trait::async_trait;
use oah_core::{
    AttemptId, ConversationId, OwnerId, Principal, Record, RecordBatch, SessionKey,
    STORE_FORMAT_VERSION, StreamOffset, SubmissionId, UnixMillis,
};
use oah_store::{
    AdmitReceipt, AdmitRequest, Claim, DeliveryKind, Result, Store, StoreError, SubmissionRow,
    SubmissionStatus, Suspension,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS oah_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS oah_submissions (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  submission_id TEXT NOT NULL UNIQUE,
  session_key TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL,
  accepted_at INTEGER NOT NULL,
  attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 10,
  owner_id TEXT,
  lease_expires_at INTEGER,
  principal TEXT NOT NULL,
  idempotency_key TEXT,
  abort_requested INTEGER NOT NULL DEFAULT 0,
  input_applied_at INTEGER,
  error TEXT,
  settled_at INTEGER
);
CREATE INDEX IF NOT EXISTS oah_submissions_session
  ON oah_submissions(session_key, status, sequence);
CREATE TABLE IF NOT EXISTS oah_streams (
  path TEXT PRIMARY KEY,
  identity TEXT NOT NULL,
  uid TEXT NOT NULL,
  next_seq INTEGER NOT NULL DEFAULT 1,
  incarnation INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS oah_stream_batches (
  path TEXT NOT NULL,
  seq INTEGER NOT NULL,
  data TEXT NOT NULL,
  submission_id TEXT,
  attempt_id TEXT,
  PRIMARY KEY (path, seq)
);
CREATE TABLE IF NOT EXISTS oah_suspensions (
  submission_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  tool TEXT NOT NULL,
  effective_args TEXT NOT NULL,
  reason TEXT NOT NULL,
  kind TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  answered_at INTEGER,
  approved INTEGER,
  answered_by TEXT,
  PRIMARY KEY (submission_id, tool_call_id)
);
"#;

pub struct SqliteStore {
    conn: Mutex<Connection>,
    epoch: AtomicU64,
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
    path: PathBuf,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
        let conn = Connection::open(&path).map_err(|e| StoreError::Backend(e.to_string()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let (tx, rx) = watch::channel(0);
        Ok(Self {
            conn: Mutex::new(conn),
            epoch: AtomicU64::new(0),
            tx,
            rx,
            path,
        })
    }

    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| StoreError::Backend(e.to_string()))?;
        let (tx, rx) = watch::channel(0);
        Ok(Self {
            conn: Mutex::new(conn),
            epoch: AtomicU64::new(0),
            tx,
            rx,
            path: PathBuf::from(":memory:"),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ping(&self) {
        let n = self.epoch.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        let _ = self.tx.send(n);
    }
}

fn map_sql(err: rusqlite::Error) -> StoreError {
    StoreError::Backend(err.to_string())
}

fn row_from_query(row: &rusqlite::Row<'_>) -> rusqlite::Result<SubmissionRow> {
    let payload: String = row.get(5)?;
    let principal: String = row.get(13)?;
    Ok(SubmissionRow {
        sequence: row.get(0)?,
        submission_id: SubmissionId::parse(row.get::<_, String>(1)?)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(e.to_string()))))?,
        session_key: SessionKey::parse(row.get::<_, String>(2)?)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(e.to_string()))))?,
        conversation_id: ConversationId::parse(row.get::<_, String>(3)?)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(e.to_string()))))?,
        kind: if row.get::<_, String>(4)? == "signal" {
            DeliveryKind::Signal
        } else {
            DeliveryKind::User
        },
        payload: serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null),
        status: SubmissionStatus::parse(&row.get::<_, String>(6)?).unwrap_or(SubmissionStatus::Queued),
        accepted_at: UnixMillis(row.get(7)?),
        attempt_id: row
            .get::<_, Option<String>>(8)?
            .and_then(|s| AttemptId::parse(s).ok()),
        attempt_count: row.get::<_, i64>(9)? as u32,
        max_attempts: row.get::<_, i64>(10)? as u32,
        owner_id: row
            .get::<_, Option<String>>(11)?
            .and_then(|s| OwnerId::parse(s).ok()),
        lease_expires_at: row.get::<_, Option<i64>>(12)?.map(UnixMillis),
        principal: serde_json::from_str(&principal).unwrap_or_else(|_| Principal::anonymous()),
        idempotency_key: row.get(14)?,
        abort_requested: row.get::<_, i64>(15)? != 0,
        input_applied_at: row.get::<_, Option<i64>>(16)?.map(UnixMillis),
        error: row.get(17)?,
        settled_at: row.get::<_, Option<i64>>(18)?.map(UnixMillis),
    })
}

const SELECT_SUB: &str = "SELECT sequence, submission_id, session_key, conversation_id, kind, payload, status, accepted_at, attempt_id, attempt_count, max_attempts, owner_id, lease_expires_at, principal, idempotency_key, abort_requested, input_applied_at, error, settled_at FROM oah_submissions";

#[async_trait]
impl Store for SqliteStore {
    async fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute_batch(SCHEMA).map_err(map_sql)?;
        let found: Option<String> = conn
            .query_row(
                "SELECT value FROM oah_meta WHERE key = 'format_version'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_sql)?;
        match found {
            None => {
                conn.execute(
                    "INSERT INTO oah_meta(key, value) VALUES ('format_version', ?1)",
                    params![STORE_FORMAT_VERSION.to_string()],
                )
                .map_err(map_sql)?;
            }
            Some(v) => {
                let parsed: u32 = v.parse().unwrap_or(0);
                if parsed != STORE_FORMAT_VERSION {
                    return Err(StoreError::Format(format!(
                        "found {parsed}, expected {STORE_FORMAT_VERSION}"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn admit(&self, req: AdmitRequest, now: UnixMillis) -> Result<AdmitReceipt> {
        let conn = self.conn.lock().await;
        if let Some(key) = &req.idempotency_key {
            if key.len() > 256 {
                return Err(StoreError::Conflict("idempotencyKey longer than 256 chars".into()));
            }
            let existing: Option<(String, String)> = conn
                .query_row(
                    "SELECT submission_id, payload FROM oah_submissions WHERE conversation_id = ?1 AND idempotency_key = ?2",
                    params![req.conversation_id.as_str(), key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(map_sql)?;
            if let Some((id, payload)) = existing {
                let stored: serde_json::Value =
                    serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
                let same = stored == req.payload;
                if !same {
                    return Err(StoreError::Conflict("submission_conflict".into()));
                }
                let uid: String = conn
                    .query_row(
                        "SELECT uid FROM oah_streams WHERE path = ?1",
                        params![req.conversation_id.as_str()],
                        |r| r.get(0),
                    )
                    .map_err(map_sql)?;
                return Ok(AdmitReceipt {
                    submission_id: SubmissionId::parse(id)
                        .map_err(|e| StoreError::Backend(e.to_string()))?,
                    offset: StreamOffset::ORIGIN,
                    uid,
                    deduplicated: true,
                });
            }
        }

        let created = conn
            .query_row(
                "SELECT uid FROM oah_streams WHERE path = ?1",
                params![req.conversation_id.as_str()],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(map_sql)?;
        let uid = if let Some(uid) = created {
            if req.uid.as_deref() == Some("") {
                // uid:null means create-only; already exists.
                return Err(StoreError::Conflict("agent_instance_exists".into()));
            }
            if let Some(want) = &req.uid {
                if want != &uid {
                    return Err(StoreError::NotFound("agent_instance_not_found".into()));
                }
            }
            uid
        } else {
            let uid = req.uid.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| {
                format!("uid_{}", ulid_now())
            });
            conn.execute(
                "INSERT INTO oah_streams(path, identity, uid) VALUES (?1, ?2, ?3)",
                params![
                    req.conversation_id.as_str(),
                    req.conversation_id.as_str(),
                    uid
                ],
            )
            .map_err(map_sql)?;
            uid
        };

        let submission_id = match &req.idempotency_key {
            Some(key) => {
                let agent = req
                    .conversation_id
                    .agent()
                    .map_err(|e| StoreError::Backend(e.to_string()))?;
                let instance = req
                    .conversation_id
                    .instance()
                    .map_err(|e| StoreError::Backend(e.to_string()))?;
                SubmissionId::from_idempotency_key(&agent, &instance, key)
            }
            None => SubmissionId::new(),
        };
        let principal = serde_json::to_string(&req.principal)
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        conn.execute(
            "INSERT INTO oah_submissions(
                submission_id, session_key, conversation_id, kind, payload, status, accepted_at,
                max_attempts, principal, idempotency_key
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6, ?7, ?8, ?9)",
            params![
                submission_id.as_str(),
                req.session_key.as_str(),
                req.conversation_id.as_str(),
                req.kind.as_str(),
                req.payload.to_string(),
                now.as_millis(),
                req.max_attempts as i64,
                principal,
                req.idempotency_key,
            ],
        )
        .map_err(map_sql)?;
        drop(conn);
        self.ping();
        Ok(AdmitReceipt {
            submission_id,
            offset: StreamOffset::ORIGIN,
            uid,
            deduplicated: false,
        })
    }

    async fn claim_runnable(
        &self,
        owner: &OwnerId,
        now: UnixMillis,
        lease_ms: i64,
    ) -> Result<Option<Claim>> {
        let conn = self.conn.lock().await;
        let candidate: Option<(i64, String, i64, i64)> = conn
            .query_row(
                "SELECT sequence, submission_id, attempt_count, max_attempts FROM oah_submissions
                 WHERE status = 'queued'
                 AND sequence = (
                   SELECT MIN(s.sequence) FROM oah_submissions s
                   WHERE s.session_key = oah_submissions.session_key
                     AND s.status NOT IN ('settled', 'joined')
                 )
                 ORDER BY sequence ASC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .map_err(map_sql)?;
        let Some((seq, sid, count, _max)) = candidate else {
            return Ok(None);
        };
        let attempt = AttemptId::new();
        let n = conn
            .execute(
                "UPDATE oah_submissions SET status = 'running', attempt_id = ?1, owner_id = ?2,
                    lease_expires_at = ?3, attempt_count = attempt_count + 1
                 WHERE sequence = ?4 AND status = 'queued'",
                params![
                    attempt.as_str(),
                    owner.as_str(),
                    now.as_millis().saturating_add(lease_ms),
                    seq
                ],
            )
            .map_err(map_sql)?;
        if n != 1 {
            return Ok(None);
        }
        let row = conn
            .query_row(
                &format!("{SELECT_SUB} WHERE submission_id = ?1"),
                params![sid],
                row_from_query,
            )
            .map_err(map_sql)?;
        let _ = count;
        Ok(Some(Claim {
            attempt_id: attempt,
            row,
        }))
    }

    async fn mark_input_applied(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE oah_submissions SET input_applied_at = COALESCE(input_applied_at, ?1) WHERE submission_id = ?2",
            params![now.as_millis(), id.as_str()],
        )
        .map_err(map_sql)?;
        Ok(())
    }

    async fn request_abort(&self, session: &SessionKey, now: UnixMillis) -> Result<u32> {
        let conn = self.conn.lock().await;
        let n = conn
            .execute(
                "UPDATE oah_submissions SET abort_requested = 1 WHERE session_key = ?1 AND status NOT IN ('settled')",
                params![session.as_str()],
            )
            .map_err(map_sql)?;
        let _ = now;
        self.ping();
        Ok(n as u32)
    }

    async fn reserve_settlement(&self, id: &SubmissionId) -> Result<()> {
        let conn = self.conn.lock().await;
        let n = conn
            .execute(
                "UPDATE oah_submissions SET status = 'terminalizing' WHERE submission_id = ?1 AND status IN ('running','suspended','queued')",
                params![id.as_str()],
            )
            .map_err(map_sql)?;
        if n != 1 {
            return Err(StoreError::Conflict("could not reserve settlement".into()));
        }
        Ok(())
    }

    async fn finalize_settlement(
        &self,
        id: &SubmissionId,
        now: UnixMillis,
        error: Option<String>,
    ) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE oah_submissions SET status = 'settled', settled_at = ?1, error = ?2, owner_id = NULL, lease_expires_at = NULL
             WHERE submission_id = ?3",
            params![now.as_millis(), error, id.as_str()],
        )
        .map_err(map_sql)?;
        drop(conn);
        self.ping();
        Ok(())
    }

    async fn suspend(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE oah_submissions SET status = 'suspended', owner_id = NULL, lease_expires_at = NULL
             WHERE submission_id = ?1 AND status = 'running'",
            params![id.as_str()],
        )
        .map_err(map_sql)?;
        let _ = now;
        drop(conn);
        self.ping();
        Ok(())
    }

    async fn put_suspension(&self, item: Suspension) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT OR REPLACE INTO oah_suspensions(
                submission_id, tool_call_id, tool, effective_args, reason, kind, created_at,
                answered_at, approved, answered_by
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                item.submission_id.as_str(),
                item.tool_call_id,
                item.tool,
                item.effective_args.to_string(),
                item.reason,
                item.kind,
                item.created_at.as_millis(),
                item.answered_at.map(|t| t.as_millis()),
                item.approved.map(|b| if b { 1 } else { 0 }),
                item.answered_by,
            ],
        )
        .map_err(map_sql)?;
        Ok(())
    }

    async fn answer_suspension(
        &self,
        submission: &SubmissionId,
        tool_call_id: &str,
        approved: bool,
        by: &str,
        now: UnixMillis,
    ) -> Result<Suspension> {
        let conn = self.conn.lock().await;
        let status: String = conn
            .query_row(
                "SELECT status FROM oah_submissions WHERE submission_id = ?1",
                params![submission.as_str()],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_sql)?
            .ok_or_else(|| StoreError::NotFound("submission".into()))?;
        if status == "settled" {
            return Err(StoreError::SubmissionSettled);
        }
        let answered: Option<i64> = conn
            .query_row(
                "SELECT answered_at FROM oah_suspensions WHERE submission_id = ?1 AND tool_call_id = ?2",
                params![submission.as_str(), tool_call_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_sql)?
            .ok_or_else(|| StoreError::NotFound("approval".into()))?;
        if answered.is_some() {
            return Err(StoreError::AlreadyAnswered);
        }
        conn.execute(
            "UPDATE oah_suspensions SET answered_at = ?1, approved = ?2, answered_by = ?3
             WHERE submission_id = ?4 AND tool_call_id = ?5 AND answered_at IS NULL",
            params![
                now.as_millis(),
                if approved { 1 } else { 0 },
                by,
                submission.as_str(),
                tool_call_id
            ],
        )
        .map_err(map_sql)?;
        conn.execute(
            "UPDATE oah_submissions SET status = 'queued' WHERE submission_id = ?1 AND status = 'suspended'",
            params![submission.as_str()],
        )
        .map_err(map_sql)?;
        drop(conn);
        self.ping();
        let items = self.list_suspensions(submission).await?;
        items
            .into_iter()
            .find(|s| s.tool_call_id == tool_call_id)
            .ok_or_else(|| StoreError::NotFound("approval".into()))
    }

    async fn list_suspensions(&self, submission: &SubmissionId) -> Result<Vec<Suspension>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT submission_id, tool_call_id, tool, effective_args, reason, kind, created_at, answered_at, approved, answered_by
                 FROM oah_suspensions WHERE submission_id = ?1",
            )
            .map_err(map_sql)?;
        let rows = stmt
            .query_map(params![submission.as_str()], map_suspension)
            .map_err(map_sql)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(map_sql)?);
        }
        Ok(out)
    }

    async fn list_pending_approvals(&self, conversation: &ConversationId) -> Result<Vec<Suspension>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT s.submission_id, s.tool_call_id, s.tool, s.effective_args, s.reason, s.kind, s.created_at, s.answered_at, s.approved, s.answered_by
                 FROM oah_suspensions s
                 JOIN oah_submissions sub ON sub.submission_id = s.submission_id
                 WHERE sub.conversation_id = ?1 AND s.answered_at IS NULL",
            )
            .map_err(map_sql)?;
        let rows = stmt
            .query_map(params![conversation.as_str()], map_suspension)
            .map_err(map_sql)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(map_sql)?);
        }
        Ok(out)
    }

    async fn get_submission(&self, id: &SubmissionId) -> Result<SubmissionRow> {
        let conn = self.conn.lock().await;
        conn.query_row(
            &format!("{SELECT_SUB} WHERE submission_id = ?1"),
            params![id.as_str()],
            row_from_query,
        )
        .optional()
        .map_err(map_sql)?
        .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    async fn list_expired(&self, now: UnixMillis) -> Result<Vec<SubmissionRow>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(&format!(
                "{SELECT_SUB} WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?1"
            ))
            .map_err(map_sql)?;
        let rows = stmt
            .query_map(params![now.as_millis()], row_from_query)
            .map_err(map_sql)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(map_sql)?);
        }
        Ok(out)
    }

    async fn list_pending_settlements(&self) -> Result<Vec<SubmissionRow>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(&format!("{SELECT_SUB} WHERE status = 'terminalizing'"))
            .map_err(map_sql)?;
        let rows = stmt.query_map([], row_from_query).map_err(map_sql)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(map_sql)?);
        }
        Ok(out)
    }

    async fn requeue(&self, id: &SubmissionId) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE oah_submissions SET status = 'queued', owner_id = NULL, lease_expires_at = NULL
             WHERE submission_id = ?1",
            params![id.as_str()],
        )
        .map_err(map_sql)?;
        drop(conn);
        self.ping();
        Ok(())
    }

    async fn replace_attempt(
        &self,
        id: &SubmissionId,
        owner: &OwnerId,
        now: UnixMillis,
        lease_ms: i64,
    ) -> Result<Claim> {
        let conn = self.conn.lock().await;
        let attempt = AttemptId::new();
        let n = conn
            .execute(
                "UPDATE oah_submissions SET status = 'running', attempt_id = ?1, owner_id = ?2,
                    lease_expires_at = ?3, attempt_count = attempt_count + 1
                 WHERE submission_id = ?4 AND status IN ('running','queued')",
                params![
                    attempt.as_str(),
                    owner.as_str(),
                    now.as_millis().saturating_add(lease_ms),
                    id.as_str()
                ],
            )
            .map_err(map_sql)?;
        if n != 1 {
            return Err(StoreError::Conflict("replace_attempt lost the CAS".into()));
        }
        let row = conn
            .query_row(
                &format!("{SELECT_SUB} WHERE submission_id = ?1"),
                params![id.as_str()],
                row_from_query,
            )
            .map_err(map_sql)?;
        Ok(Claim {
            attempt_id: attempt,
            row,
        })
    }

    async fn expire_owner_leases(&self, owner: &OwnerId) -> Result<u32> {
        let conn = self.conn.lock().await;
        let n = conn
            .execute(
                "UPDATE oah_submissions SET status = 'queued', owner_id = NULL, lease_expires_at = NULL
                 WHERE owner_id = ?1 AND status = 'running'",
                params![owner.as_str()],
            )
            .map_err(map_sql)?;
        drop(conn);
        self.ping();
        Ok(n as u32)
    }

    async fn create_stream(
        &self,
        path: &str,
        identity: &str,
        uid: &str,
        _now: UnixMillis,
    ) -> Result<(StreamOffset, String, bool)> {
        let conn = self.conn.lock().await;
        let existing: Option<String> = conn
            .query_row(
                "SELECT uid FROM oah_streams WHERE path = ?1",
                params![path],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_sql)?;
        if let Some(uid) = existing {
            return Ok((StreamOffset::ORIGIN, uid, false));
        }
        conn.execute(
            "INSERT INTO oah_streams(path, identity, uid) VALUES (?1, ?2, ?3)",
            params![path, identity, uid],
        )
        .map_err(map_sql)?;
        Ok((StreamOffset::ORIGIN, uid.to_string(), true))
    }

    async fn append(
        &self,
        path: &str,
        records: Vec<Record>,
        submission: Option<&SubmissionId>,
        attempt: Option<&AttemptId>,
    ) -> Result<RecordBatch> {
        let conn = self.conn.lock().await;
        if let (Some(sid), Some(aid)) = (submission, attempt) {
            let row = conn
                .query_row(
                    "SELECT attempt_id, status FROM oah_submissions WHERE submission_id = ?1",
                    params![sid.as_str()],
                    |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(map_sql)?
                .ok_or_else(|| StoreError::NotFound("submission".into()))?;
            if row.1 == "settled" || row.0.as_deref() != Some(aid.as_str()) {
                return Err(StoreError::Conflict("stale_append_fence".into()));
            }
        }
        let seq: i64 = conn
            .query_row(
                "SELECT next_seq FROM oah_streams WHERE path = ?1",
                params![path],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_sql)?
            .ok_or_else(|| StoreError::NotFound(format!("stream {path}")))?;
        let data = serde_json::to_string(&records).map_err(|e| StoreError::Backend(e.to_string()))?;
        conn.execute(
            "INSERT INTO oah_stream_batches(path, seq, data, submission_id, attempt_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                path,
                seq,
                data,
                submission.map(ToString::to_string),
                attempt.map(ToString::to_string)
            ],
        )
        .map_err(map_sql)?;
        conn.execute(
            "UPDATE oah_streams SET next_seq = next_seq + 1 WHERE path = ?1",
            params![path],
        )
        .map_err(map_sql)?;
        drop(conn);
        self.ping();
        Ok(RecordBatch {
            path: path.to_string(),
            seq,
            records,
            submission_id: submission.cloned(),
            attempt_id: attempt.cloned(),
        })
    }

    async fn read_after(
        &self,
        path: &str,
        after: StreamOffset,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let conn = self.conn.lock().await;
        let min_seq = if after.is_origin() { 0 } else { after.batch };
        let mut stmt = conn
            .prepare(
                "SELECT seq, data, submission_id, attempt_id FROM oah_stream_batches
                 WHERE path = ?1 AND seq > ?2 ORDER BY seq ASC LIMIT ?3",
            )
            .map_err(map_sql)?;
        let rows = stmt
            .query_map(params![path, min_seq, limit as i64], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(map_sql)?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, data, sid, aid) = row.map_err(map_sql)?;
            let records: Vec<Record> =
                serde_json::from_str(&data).map_err(|e| StoreError::Backend(e.to_string()))?;
            out.push(RecordBatch {
                path: path.to_string(),
                seq,
                records,
                submission_id: sid.and_then(|s| SubmissionId::parse(s).ok()),
                attempt_id: aid.and_then(|s| AttemptId::parse(s).ok()),
            });
        }
        Ok(out)
    }

    async fn read_all(&self, path: &str) -> Result<Vec<Record>> {
        let batches = self.read_after(path, StreamOffset::ORIGIN, 10_000).await?;
        Ok(batches.into_iter().flat_map(|b| b.records).collect())
    }

    async fn stream_head(&self, path: &str) -> Result<(StreamOffset, u64, String)> {
        let conn = self.conn.lock().await;
        let (next, inc, uid): (i64, i64, String) = conn
            .query_row(
                "SELECT next_seq, incarnation, uid FROM oah_streams WHERE path = ?1",
                params![path],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(map_sql)?
            .ok_or_else(|| StoreError::NotFound(format!("stream {path}")))?;
        let head = if next <= 1 {
            StreamOffset::ORIGIN
        } else {
            StreamOffset::new(next - 1, 0)
        };
        Ok((head, inc as u64, uid))
    }

    fn notify(&self) -> watch::Receiver<u64> {
        self.rx.clone()
    }

    fn wake(&self) {
        self.ping();
    }
}

fn map_suspension(row: &rusqlite::Row<'_>) -> rusqlite::Result<Suspension> {
    let args: String = row.get(3)?;
    Ok(Suspension {
        submission_id: SubmissionId::parse(row.get::<_, String>(0)?).map_err(|e| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(e.to_string())))
        })?,
        tool_call_id: row.get(1)?,
        tool: row.get(2)?,
        effective_args: serde_json::from_str(&args).unwrap_or(serde_json::Value::Null),
        reason: row.get(4)?,
        kind: row.get(5)?,
        created_at: UnixMillis(row.get(6)?),
        answered_at: row.get::<_, Option<i64>>(7)?.map(UnixMillis),
        approved: row.get::<_, Option<i64>>(8)?.map(|v| v != 0),
        answered_by: row.get(9)?,
    })
}

fn ulid_now() -> String {
    oah_core::RecordId::new().to_string().replacen("rec_", "", 1)
}

impl SqliteStore {
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use oah_core::{AgentName, InstanceId};
    use serde_json::json;

    #[tokio::test]
    async fn admit_claim_settle() {
        let store = SqliteStore::memory().unwrap();
        store.migrate().await.unwrap();
        let agent = AgentName::parse("support-desk").unwrap();
        let instance = InstanceId::parse("t1").unwrap();
        let cid = ConversationId::new(&agent, &instance);
        let receipt = store
            .admit(
                AdmitRequest {
                    conversation_id: cid.clone(),
                    session_key: SessionKey::root(&cid),
                    kind: DeliveryKind::User,
                    payload: json!({"body": "hello"}),
                    principal: Principal::anonymous(),
                    idempotency_key: Some("k1".into()),
                    uid: None,
                    max_attempts: 10,
                },
                UnixMillis(1),
            )
            .await
            .unwrap();
        assert!(!receipt.deduplicated);
        let again = store
            .admit(
                AdmitRequest {
                    conversation_id: cid.clone(),
                    session_key: SessionKey::root(&cid),
                    kind: DeliveryKind::User,
                    payload: json!({"body": "hello"}),
                    principal: Principal::anonymous(),
                    idempotency_key: Some("k1".into()),
                    uid: None,
                    max_attempts: 10,
                },
                UnixMillis(2),
            )
            .await
            .unwrap();
        assert!(again.deduplicated);
        let owner = OwnerId::new();
        let claim = store
            .claim_runnable(&owner, UnixMillis(3), 30_000)
            .await
            .unwrap()
            .expect("claim");
        assert_eq!(claim.row.submission_id, receipt.submission_id);
        store
            .reserve_settlement(&receipt.submission_id)
            .await
            .unwrap();
        store
            .finalize_settlement(&receipt.submission_id, UnixMillis(4), None)
            .await
            .unwrap();
        let row = store.get_submission(&receipt.submission_id).await.unwrap();
        assert_eq!(row.status, SubmissionStatus::Settled);
    }

    #[tokio::test]
    async fn conformance() {
        async fn fresh() -> SqliteStore {
            let store = SqliteStore::memory().unwrap();
            store.migrate().await.unwrap();
            store
        }
        oah_store_conformance::admit_idempotent_same_payload(&fresh().await)
            .await
            .unwrap();
        oah_store_conformance::admit_idempotent_conflict(&fresh().await)
            .await
            .unwrap();
        oah_store_conformance::claim_fifo_and_session_fence(&fresh().await)
            .await
            .unwrap();
        oah_store_conformance::suspend_answer_once(&fresh().await)
            .await
            .unwrap();
        oah_store_conformance::create_only_uid_conflict(&fresh().await)
            .await
            .unwrap();
        oah_store_conformance::append_rejects_stale_attempt(&fresh().await)
            .await
            .unwrap();
    }
}
