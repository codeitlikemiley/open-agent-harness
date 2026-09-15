//! Postgres adapter. Enable with `OAH_STORE__BACKEND=postgres` and
//! `OAH_POSTGRES__URL`. LISTEN/NOTIFY wakes the coordinator.
//!
//! Tests skip unless `OAH_POSTGRES__URL` is set.

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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio_postgres::{Client, NoTls};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS oah_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS oah_submissions (
  sequence BIGSERIAL PRIMARY KEY,
  submission_id TEXT NOT NULL UNIQUE,
  session_key TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL,
  accepted_at BIGINT NOT NULL,
  attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 10,
  owner_id TEXT,
  lease_expires_at BIGINT,
  principal JSONB NOT NULL,
  idempotency_key TEXT,
  abort_requested BOOLEAN NOT NULL DEFAULT FALSE,
  input_applied_at BIGINT,
  error TEXT,
  settled_at BIGINT
);
CREATE INDEX IF NOT EXISTS oah_submissions_session
  ON oah_submissions(session_key, status, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS oah_submissions_idem
  ON oah_submissions(conversation_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS oah_streams (
  path TEXT PRIMARY KEY,
  identity TEXT NOT NULL,
  uid TEXT NOT NULL,
  next_seq BIGINT NOT NULL DEFAULT 1,
  incarnation BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS oah_stream_batches (
  path TEXT NOT NULL,
  seq BIGINT NOT NULL,
  data JSONB NOT NULL,
  submission_id TEXT,
  attempt_id TEXT,
  PRIMARY KEY (path, seq)
);
CREATE TABLE IF NOT EXISTS oah_suspensions (
  submission_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  tool TEXT NOT NULL,
  effective_args JSONB NOT NULL,
  reason TEXT NOT NULL,
  kind TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  answered_at BIGINT,
  approved BOOLEAN,
  answered_by TEXT,
  PRIMARY KEY (submission_id, tool_call_id)
);
"#;

pub struct PostgresStore {
    client: Mutex<Client>,
    epoch: AtomicU64,
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
}

impl PostgresStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let (tx, rx) = watch::channel(0);
        Ok(Self {
            client: Mutex::new(client),
            epoch: AtomicU64::new(0),
            tx,
            rx,
        })
    }

    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    fn ping(&self) {
        let n = self.epoch.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        let _ = self.tx.send(n);
    }

    /// Drop ledger rows so conformance can reuse one database.
    pub async fn reset_schema(&self) -> Result<()> {
        let client = self.client.lock().await;
        client
            .batch_execute(
                "TRUNCATE TABLE oah_submissions, oah_stream_batches, oah_suspensions, oah_streams CASCADE",
            )
            .await
            .map_err(map_pg)?;
        Ok(())
    }

    async fn notify_sql(client: &Client) {
        let _ = client.batch_execute("NOTIFY oah_wake").await;
    }
}

fn map_pg(err: tokio_postgres::Error) -> StoreError {
    StoreError::Backend(err.to_string())
}

fn row_status(raw: &str) -> SubmissionStatus {
    SubmissionStatus::parse(raw).unwrap_or(SubmissionStatus::Queued)
}

fn parse_sid(raw: &str) -> Result<SubmissionId> {
    SubmissionId::parse(raw).map_err(|e| StoreError::Backend(e.to_string()))
}

const SELECT_SUB: &str = "SELECT sequence, submission_id, session_key, conversation_id, kind, payload, status, accepted_at, attempt_id, attempt_count, max_attempts, owner_id, lease_expires_at, principal, idempotency_key, abort_requested, input_applied_at, error, settled_at FROM oah_submissions";

fn submission_from_row(row: &tokio_postgres::Row) -> Result<SubmissionRow> {
    let kind = if row.get::<_, String>(4) == "signal" {
        DeliveryKind::Signal
    } else {
        DeliveryKind::User
    };
    let payload: serde_json::Value = row.get(5);
    let principal: serde_json::Value = row.get(13);
    let principal: Principal =
        serde_json::from_value(principal).unwrap_or_else(|_| Principal::anonymous());
    Ok(SubmissionRow {
        sequence: row.get::<_, i64>(0),
        submission_id: parse_sid(&row.get::<_, String>(1))?,
        session_key: SessionKey::parse(row.get::<_, String>(2))
            .map_err(|e| StoreError::Backend(e.to_string()))?,
        conversation_id: ConversationId::parse(row.get::<_, String>(3))
            .map_err(|e| StoreError::Backend(e.to_string()))?,
        kind,
        payload,
        status: row_status(&row.get::<_, String>(6)),
        accepted_at: UnixMillis(row.get(7)),
        attempt_id: row
            .get::<_, Option<String>>(8)
            .and_then(|s| AttemptId::parse(s).ok()),
        attempt_count: row.get::<_, i32>(9) as u32,
        max_attempts: row.get::<_, i32>(10) as u32,
        owner_id: row
            .get::<_, Option<String>>(11)
            .and_then(|s| OwnerId::parse(s).ok()),
        lease_expires_at: row.get::<_, Option<i64>>(12).map(UnixMillis),
        principal,
        idempotency_key: row.get(14),
        abort_requested: row.get::<_, bool>(15),
        input_applied_at: row.get::<_, Option<i64>>(16).map(UnixMillis),
        error: row.get(17),
        settled_at: row.get::<_, Option<i64>>(18).map(UnixMillis),
    })
}

fn sus_from_row(row: &tokio_postgres::Row) -> Result<Suspension> {
    Ok(Suspension {
        submission_id: parse_sid(&row.get::<_, String>(0))?,
        tool_call_id: row.get(1),
        tool: row.get(2),
        effective_args: row.get(3),
        reason: row.get(4),
        kind: row.get(5),
        created_at: UnixMillis(row.get(6)),
        answered_at: row.get::<_, Option<i64>>(7).map(UnixMillis),
        approved: row.get(8),
        answered_by: row.get(9),
    })
}

#[async_trait]
impl Store for PostgresStore {
    async fn migrate(&self) -> Result<()> {
        let client = self.client.lock().await;
        client.batch_execute(SCHEMA).await.map_err(map_pg)?;
        let found = client
            .query_opt(
                "SELECT value FROM oah_meta WHERE key = 'format_version'",
                &[],
            )
            .await
            .map_err(map_pg)?;
        match found {
            None => {
                client
                    .execute(
                        "INSERT INTO oah_meta(key, value) VALUES ('format_version', $1)",
                        &[&STORE_FORMAT_VERSION.to_string()],
                    )
                    .await
                    .map_err(map_pg)?;
            }
            Some(row) => {
                let v: String = row.get(0);
                let parsed: u32 = v.parse().unwrap_or(0);
                if parsed != STORE_FORMAT_VERSION {
                    return Err(StoreError::Format(format!(
                        "found {parsed}, expected {STORE_FORMAT_VERSION}"
                    )));
                }
            }
        }
        let _ = client.batch_execute("LISTEN oah_wake").await;
        Ok(())
    }

    async fn admit(&self, req: AdmitRequest, now: UnixMillis) -> Result<AdmitReceipt> {
        let client = self.client.lock().await;
        if let Some(key) = &req.idempotency_key {
            if key.len() > 256 {
                return Err(StoreError::Conflict(
                    "idempotencyKey longer than 256 chars".into(),
                ));
            }
            if let Some(row) = client
                .query_opt(
                    "SELECT submission_id, payload FROM oah_submissions WHERE conversation_id = $1 AND idempotency_key = $2",
                    &[&req.conversation_id.as_str(), key],
                )
                .await
                .map_err(map_pg)?
            {
                let id: String = row.get(0);
                let stored: serde_json::Value = row.get(1);
                if stored != req.payload {
                    return Err(StoreError::Conflict("submission_conflict".into()));
                }
                let uid: String = client
                    .query_one(
                        "SELECT uid FROM oah_streams WHERE path = $1",
                        &[&req.conversation_id.as_str()],
                    )
                    .await
                    .map_err(map_pg)?
                    .get(0);
                return Ok(AdmitReceipt {
                    submission_id: parse_sid(&id)?,
                    offset: StreamOffset::ORIGIN,
                    uid,
                    deduplicated: true,
                });
            }
        }

        let existing = client
            .query_opt(
                "SELECT uid FROM oah_streams WHERE path = $1",
                &[&req.conversation_id.as_str()],
            )
            .await
            .map_err(map_pg)?;
        let uid = if let Some(row) = existing {
            if req.uid.as_deref() == Some("") {
                return Err(StoreError::Conflict("agent_instance_exists".into()));
            }
            let uid: String = row.get(0);
            if let Some(want) = &req.uid {
                if want != &uid {
                    return Err(StoreError::NotFound("agent_instance_not_found".into()));
                }
            }
            uid
        } else {
            let uid = req
                .uid
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| {
                    format!(
                        "uid_{}",
                        oah_core::RecordId::new()
                            .to_string()
                            .replacen("rec_", "", 1)
                    )
                });
            client
                .execute(
                    "INSERT INTO oah_streams(path, identity, uid) VALUES ($1, $2, $3)",
                    &[
                        &req.conversation_id.as_str(),
                        &req.conversation_id.as_str(),
                        &uid,
                    ],
                )
                .await
                .map_err(map_pg)?;
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
        let principal = serde_json::to_value(&req.principal)
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        client
            .execute(
                "INSERT INTO oah_submissions(
                    submission_id, session_key, conversation_id, kind, payload, status, accepted_at,
                    max_attempts, principal, idempotency_key
                ) VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7, $8, $9)",
                &[
                    &submission_id.as_str(),
                    &req.session_key.as_str(),
                    &req.conversation_id.as_str(),
                    &req.kind.as_str(),
                    &req.payload,
                    &now.as_millis(),
                    &(req.max_attempts as i32),
                    &principal,
                    &req.idempotency_key,
                ],
            )
            .await
            .map_err(map_pg)?;
        Self::notify_sql(&client).await;
        drop(client);
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
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT sequence, submission_id FROM oah_submissions
                 WHERE status = 'queued'
                 AND sequence = (
                   SELECT MIN(s.sequence) FROM oah_submissions s
                   WHERE s.session_key = oah_submissions.session_key
                     AND s.status NOT IN ('settled', 'joined')
                 )
                 ORDER BY sequence ASC LIMIT 1",
                &[],
            )
            .await
            .map_err(map_pg)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let seq: i64 = row.get(0);
        let sid: String = row.get(1);
        let attempt = AttemptId::new();
        let n = client
            .execute(
                "UPDATE oah_submissions SET status = 'running', attempt_id = $1, owner_id = $2,
                    lease_expires_at = $3, attempt_count = attempt_count + 1
                 WHERE sequence = $4 AND status = 'queued'",
                &[
                    &attempt.as_str(),
                    &owner.as_str(),
                    &now.as_millis().saturating_add(lease_ms),
                    &seq,
                ],
            )
            .await
            .map_err(map_pg)?;
        if n != 1 {
            return Ok(None);
        }
        let row = client
            .query_one(&format!("{SELECT_SUB} WHERE submission_id = $1"), &[&sid])
            .await
            .map_err(map_pg)?;
        Ok(Some(Claim {
            attempt_id: attempt,
            row: submission_from_row(&row)?,
        }))
    }

    async fn mark_input_applied(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE oah_submissions SET input_applied_at = COALESCE(input_applied_at, $1) WHERE submission_id = $2",
                &[&now.as_millis(), &id.as_str()],
            )
            .await
            .map_err(map_pg)?;
        Ok(())
    }

    async fn request_abort(&self, session: &SessionKey, now: UnixMillis) -> Result<u32> {
        let client = self.client.lock().await;
        let n = client
            .execute(
                "UPDATE oah_submissions SET abort_requested = TRUE WHERE session_key = $1 AND status <> 'settled'",
                &[&session.as_str()],
            )
            .await
            .map_err(map_pg)?;
        let _ = now;
        Self::notify_sql(&client).await;
        drop(client);
        self.ping();
        Ok(n as u32)
    }

    async fn reserve_settlement(&self, id: &SubmissionId) -> Result<()> {
        let client = self.client.lock().await;
        let n = client
            .execute(
                "UPDATE oah_submissions SET status = 'terminalizing' WHERE submission_id = $1 AND status IN ('running','suspended','queued')",
                &[&id.as_str()],
            )
            .await
            .map_err(map_pg)?;
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
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE oah_submissions SET status = 'settled', settled_at = $1, error = $2, owner_id = NULL, lease_expires_at = NULL
                 WHERE submission_id = $3",
                &[&now.as_millis(), &error, &id.as_str()],
            )
            .await
            .map_err(map_pg)?;
        Self::notify_sql(&client).await;
        drop(client);
        self.ping();
        Ok(())
    }

    async fn suspend(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE oah_submissions SET status = 'suspended', owner_id = NULL, lease_expires_at = NULL
                 WHERE submission_id = $1 AND status = 'running'",
                &[&id.as_str()],
            )
            .await
            .map_err(map_pg)?;
        let _ = now;
        Self::notify_sql(&client).await;
        drop(client);
        self.ping();
        Ok(())
    }

    async fn put_suspension(&self, item: Suspension) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "INSERT INTO oah_suspensions(
                    submission_id, tool_call_id, tool, effective_args, reason, kind, created_at,
                    answered_at, approved, answered_by
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                 ON CONFLICT (submission_id, tool_call_id) DO UPDATE SET
                    tool = EXCLUDED.tool,
                    effective_args = EXCLUDED.effective_args,
                    reason = EXCLUDED.reason,
                    kind = EXCLUDED.kind,
                    created_at = EXCLUDED.created_at,
                    answered_at = EXCLUDED.answered_at,
                    approved = EXCLUDED.approved,
                    answered_by = EXCLUDED.answered_by",
                &[
                    &item.submission_id.as_str(),
                    &item.tool_call_id,
                    &item.tool,
                    &item.effective_args,
                    &item.reason,
                    &item.kind,
                    &item.created_at.as_millis(),
                    &item.answered_at.map(|t| t.as_millis()),
                    &item.approved,
                    &item.answered_by,
                ],
            )
            .await
            .map_err(map_pg)?;
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
        let client = self.client.lock().await;
        let status: String = client
            .query_opt(
                "SELECT status FROM oah_submissions WHERE submission_id = $1",
                &[&submission.as_str()],
            )
            .await
            .map_err(map_pg)?
            .ok_or_else(|| StoreError::NotFound("submission".into()))?
            .get(0);
        if status == "settled" {
            return Err(StoreError::SubmissionSettled);
        }
        let answered: Option<i64> = client
            .query_opt(
                "SELECT answered_at FROM oah_suspensions WHERE submission_id = $1 AND tool_call_id = $2",
                &[&submission.as_str(), &tool_call_id],
            )
            .await
            .map_err(map_pg)?
            .ok_or_else(|| StoreError::NotFound("approval".into()))?
            .get(0);
        if answered.is_some() {
            return Err(StoreError::AlreadyAnswered);
        }
        client
            .execute(
                "UPDATE oah_suspensions SET answered_at = $1, approved = $2, answered_by = $3
                 WHERE submission_id = $4 AND tool_call_id = $5 AND answered_at IS NULL",
                &[
                    &now.as_millis(),
                    &approved,
                    &by,
                    &submission.as_str(),
                    &tool_call_id,
                ],
            )
            .await
            .map_err(map_pg)?;
        client
            .execute(
                "UPDATE oah_submissions SET status = 'queued' WHERE submission_id = $1 AND status = 'suspended'",
                &[&submission.as_str()],
            )
            .await
            .map_err(map_pg)?;
        Self::notify_sql(&client).await;
        drop(client);
        self.ping();
        let items = self.list_suspensions(submission).await?;
        items
            .into_iter()
            .find(|s| s.tool_call_id == tool_call_id)
            .ok_or_else(|| StoreError::NotFound("approval".into()))
    }

    async fn list_suspensions(&self, submission: &SubmissionId) -> Result<Vec<Suspension>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT submission_id, tool_call_id, tool, effective_args, reason, kind, created_at, answered_at, approved, answered_by
                 FROM oah_suspensions WHERE submission_id = $1",
                &[&submission.as_str()],
            )
            .await
            .map_err(map_pg)?;
        rows.iter().map(sus_from_row).collect()
    }

    async fn list_pending_approvals(&self, conversation: &ConversationId) -> Result<Vec<Suspension>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT s.submission_id, s.tool_call_id, s.tool, s.effective_args, s.reason, s.kind, s.created_at, s.answered_at, s.approved, s.answered_by
                 FROM oah_suspensions s
                 JOIN oah_submissions sub ON sub.submission_id = s.submission_id
                 WHERE sub.conversation_id = $1 AND s.answered_at IS NULL",
                &[&conversation.as_str()],
            )
            .await
            .map_err(map_pg)?;
        rows.iter().map(sus_from_row).collect()
    }

    async fn get_submission(&self, id: &SubmissionId) -> Result<SubmissionRow> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!("{SELECT_SUB} WHERE submission_id = $1"),
                &[&id.as_str()],
            )
            .await
            .map_err(map_pg)?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        submission_from_row(&row)
    }

    async fn list_expired(&self, now: UnixMillis) -> Result<Vec<SubmissionRow>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!("{SELECT_SUB} WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < $1"),
                &[&now.as_millis()],
            )
            .await
            .map_err(map_pg)?;
        rows.iter().map(submission_from_row).collect()
    }

    async fn list_pending_settlements(&self) -> Result<Vec<SubmissionRow>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!("{SELECT_SUB} WHERE status = 'terminalizing'"),
                &[],
            )
            .await
            .map_err(map_pg)?;
        rows.iter().map(submission_from_row).collect()
    }

    async fn requeue(&self, id: &SubmissionId) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE oah_submissions SET status = 'queued', owner_id = NULL, lease_expires_at = NULL
                 WHERE submission_id = $1",
                &[&id.as_str()],
            )
            .await
            .map_err(map_pg)?;
        Self::notify_sql(&client).await;
        drop(client);
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
        let client = self.client.lock().await;
        let attempt = AttemptId::new();
        let n = client
            .execute(
                "UPDATE oah_submissions SET status = 'running', attempt_id = $1, owner_id = $2,
                    lease_expires_at = $3, attempt_count = attempt_count + 1
                 WHERE submission_id = $4 AND status IN ('running','queued')",
                &[
                    &attempt.as_str(),
                    &owner.as_str(),
                    &now.as_millis().saturating_add(lease_ms),
                    &id.as_str(),
                ],
            )
            .await
            .map_err(map_pg)?;
        if n != 1 {
            return Err(StoreError::Conflict("replace_attempt lost the CAS".into()));
        }
        let row = client
            .query_one(
                &format!("{SELECT_SUB} WHERE submission_id = $1"),
                &[&id.as_str()],
            )
            .await
            .map_err(map_pg)?;
        Ok(Claim {
            attempt_id: attempt,
            row: submission_from_row(&row)?,
        })
    }

    async fn expire_owner_leases(&self, owner: &OwnerId) -> Result<u32> {
        let client = self.client.lock().await;
        let n = client
            .execute(
                "UPDATE oah_submissions SET status = 'queued', owner_id = NULL, lease_expires_at = NULL
                 WHERE owner_id = $1 AND status = 'running'",
                &[&owner.as_str()],
            )
            .await
            .map_err(map_pg)?;
        Self::notify_sql(&client).await;
        drop(client);
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
        let client = self.client.lock().await;
        if let Some(row) = client
            .query_opt("SELECT uid FROM oah_streams WHERE path = $1", &[&path])
            .await
            .map_err(map_pg)?
        {
            let uid: String = row.get(0);
            return Ok((StreamOffset::ORIGIN, uid, false));
        }
        client
            .execute(
                "INSERT INTO oah_streams(path, identity, uid) VALUES ($1, $2, $3)",
                &[&path, &identity, &uid],
            )
            .await
            .map_err(map_pg)?;
        Ok((StreamOffset::ORIGIN, uid.to_string(), true))
    }

    async fn append(
        &self,
        path: &str,
        records: Vec<Record>,
        submission: Option<&SubmissionId>,
        attempt: Option<&AttemptId>,
    ) -> Result<RecordBatch> {
        let client = self.client.lock().await;
        if let (Some(sid), Some(aid)) = (submission, attempt) {
            let row = client
                .query_opt(
                    "SELECT attempt_id, status FROM oah_submissions WHERE submission_id = $1",
                    &[&sid.as_str()],
                )
                .await
                .map_err(map_pg)?
                .ok_or_else(|| StoreError::NotFound("submission".into()))?;
            let live: Option<String> = row.get(0);
            let status: String = row.get(1);
            if status == "settled" || live.as_deref() != Some(aid.as_str()) {
                return Err(StoreError::Conflict("stale_append_fence".into()));
            }
        }
        let seq: i64 = client
            .query_opt("SELECT next_seq FROM oah_streams WHERE path = $1", &[&path])
            .await
            .map_err(map_pg)?
            .ok_or_else(|| StoreError::NotFound(format!("stream {path}")))?
            .get(0);
        let data = serde_json::to_value(&records).map_err(|e| StoreError::Backend(e.to_string()))?;
        let sid = submission.map(ToString::to_string);
        let aid = attempt.map(ToString::to_string);
        client
            .execute(
                "INSERT INTO oah_stream_batches(path, seq, data, submission_id, attempt_id)
                 VALUES ($1, $2, $3, $4, $5)",
                &[&path, &seq, &data, &sid, &aid],
            )
            .await
            .map_err(map_pg)?;
        client
            .execute(
                "UPDATE oah_streams SET next_seq = next_seq + 1 WHERE path = $1",
                &[&path],
            )
            .await
            .map_err(map_pg)?;
        Self::notify_sql(&client).await;
        drop(client);
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
        let client = self.client.lock().await;
        let min_seq = if after.is_origin() { 0 } else { after.batch };
        let rows = client
            .query(
                "SELECT seq, data, submission_id, attempt_id FROM oah_stream_batches
                 WHERE path = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3",
                &[&path, &min_seq, &(limit as i64)],
            )
            .await
            .map_err(map_pg)?;
        let mut out = Vec::new();
        for row in rows {
            let seq: i64 = row.get(0);
            let data: serde_json::Value = row.get(1);
            let records: Vec<Record> =
                serde_json::from_value(data).map_err(|e| StoreError::Backend(e.to_string()))?;
            let sid: Option<String> = row.get(2);
            let aid: Option<String> = row.get(3);
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
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT next_seq, incarnation, uid FROM oah_streams WHERE path = $1",
                &[&path],
            )
            .await
            .map_err(map_pg)?
            .ok_or_else(|| StoreError::NotFound(format!("stream {path}")))?;
        let next: i64 = row.get(0);
        let inc: i64 = row.get(1);
        let uid: String = row.get(2);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use oah_store::Store;

    fn dsn() -> Option<String> {
        std::env::var("OAH_POSTGRES__URL").ok().filter(|s| !s.is_empty())
    }

    #[tokio::test]
    async fn conformance() {
        let Some(url) = dsn() else {
            eprintln!("skipping postgres conformance: set OAH_POSTGRES__URL to run it");
            return;
        };
        let store = super::PostgresStore::connect(&url).await.unwrap();
        store.migrate().await.unwrap();
        store.reset_schema().await.unwrap();
        oah_store_conformance::admit_idempotent_same_payload(&store)
            .await
            .unwrap();
        store.reset_schema().await.unwrap();
        oah_store_conformance::admit_idempotent_conflict(&store)
            .await
            .unwrap();
        store.reset_schema().await.unwrap();
        oah_store_conformance::claim_fifo_and_session_fence(&store)
            .await
            .unwrap();
        store.reset_schema().await.unwrap();
        oah_store_conformance::suspend_answer_once(&store)
            .await
            .unwrap();
        store.reset_schema().await.unwrap();
        oah_store_conformance::create_only_uid_conflict(&store)
            .await
            .unwrap();
        store.reset_schema().await.unwrap();
        oah_store_conformance::append_rejects_stale_attempt(&store)
            .await
            .unwrap();
    }
}
