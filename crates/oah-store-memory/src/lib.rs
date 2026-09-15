//! In-memory `Store`. Same admit/claim/fence/suspend contracts as SQLite.

#![forbid(unsafe_code)]

use async_trait::async_trait;
use oah_core::{
    AttemptId, ConversationId, OwnerId, Record, RecordBatch, SessionKey, StreamOffset,
    SubmissionId, UnixMillis,
};
use oah_store::{
    AdmitReceipt, AdmitRequest, Claim, Result, Store, StoreError, SubmissionRow,
    SubmissionStatus, Suspension,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

#[derive(Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
    epoch: Arc<AtomicU64>,
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
}

struct Inner {
    next_sequence: i64,
    submissions: BTreeMap<i64, SubmissionRow>,
    by_id: HashMap<String, i64>,
    streams: HashMap<String, StreamState>,
    suspensions: HashMap<(String, String), Suspension>,
}

struct StreamState {
    uid: String,
    next_seq: i64,
    incarnation: u64,
    batches: BTreeMap<i64, RecordBatch>,
}

impl MemoryStore {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(0);
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_sequence: 1,
                submissions: BTreeMap::new(),
                by_id: HashMap::new(),
                streams: HashMap::new(),
                suspensions: HashMap::new(),
            })),
            epoch: Arc::new(AtomicU64::new(0)),
            tx,
            rx,
        }
    }

    fn ping(&self) {
        let n = self.epoch.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        let _ = self.tx.send(n);
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

fn uid_now() -> String {
    format!(
        "uid_{}",
        oah_core::RecordId::new()
            .to_string()
            .replacen("rec_", "", 1)
    )
}

fn session_head_sequence(inner: &Inner, session: &str) -> Option<i64> {
    inner
        .submissions
        .values()
        .filter(|r| {
            r.session_key.as_str() == session
                && !matches!(
                    r.status,
                    SubmissionStatus::Settled | SubmissionStatus::Joined
                )
        })
        .map(|r| r.sequence)
        .min()
}

#[async_trait]
impl Store for MemoryStore {
    async fn migrate(&self) -> Result<()> {
        Ok(())
    }

    async fn admit(&self, req: AdmitRequest, now: UnixMillis) -> Result<AdmitReceipt> {
        let mut inner = self.inner.lock().await;
        if let Some(key) = &req.idempotency_key {
            if key.len() > 256 {
                return Err(StoreError::Conflict(
                    "idempotencyKey longer than 256 chars".into(),
                ));
            }
            for row in inner.submissions.values() {
                if row.conversation_id == req.conversation_id
                    && row.idempotency_key.as_deref() == Some(key.as_str())
                {
                    if row.payload != req.payload {
                        return Err(StoreError::Conflict("submission_conflict".into()));
                    }
                    let uid = inner
                        .streams
                        .get(req.conversation_id.as_str())
                        .map(|s| s.uid.clone())
                        .unwrap_or_else(uid_now);
                    return Ok(AdmitReceipt {
                        submission_id: row.submission_id.clone(),
                        offset: StreamOffset::ORIGIN,
                        uid,
                        deduplicated: true,
                    });
                }
            }
        }

        let path = req.conversation_id.as_str().to_string();
        let uid = if let Some(stream) = inner.streams.get(&path) {
            if req.uid.as_deref() == Some("") {
                return Err(StoreError::Conflict("agent_instance_exists".into()));
            }
            if let Some(want) = &req.uid {
                if want != &stream.uid {
                    return Err(StoreError::NotFound("agent_instance_not_found".into()));
                }
            }
            stream.uid.clone()
        } else {
            let uid = req
                .uid
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(uid_now);
            inner.streams.insert(
                path,
                StreamState {
                    uid: uid.clone(),
                    next_seq: 1,
                    incarnation: 0,
                    batches: BTreeMap::new(),
                },
            );
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

        let sequence = inner.next_sequence;
        inner.next_sequence = inner.next_sequence.saturating_add(1);
        let row = SubmissionRow {
            sequence,
            submission_id: submission_id.clone(),
            session_key: req.session_key,
            conversation_id: req.conversation_id,
            kind: req.kind,
            payload: req.payload,
            status: SubmissionStatus::Queued,
            accepted_at: now,
            attempt_id: None,
            attempt_count: 0,
            max_attempts: req.max_attempts,
            owner_id: None,
            lease_expires_at: None,
            principal: req.principal,
            idempotency_key: req.idempotency_key,
            abort_requested: false,
            input_applied_at: None,
            error: None,
            settled_at: None,
        };
        inner.by_id.insert(submission_id.to_string(), sequence);
        inner.submissions.insert(sequence, row);
        drop(inner);
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
        let mut inner = self.inner.lock().await;
        let candidate = inner
            .submissions
            .values()
            .filter(|r| r.status == SubmissionStatus::Queued)
            .filter(|r| session_head_sequence(&inner, r.session_key.as_str()) == Some(r.sequence))
            .min_by_key(|r| r.sequence)
            .map(|r| r.sequence);
        let Some(seq) = candidate else {
            return Ok(None);
        };
        let attempt = AttemptId::new();
        let row = match inner.submissions.get_mut(&seq) {
            Some(row) if row.status == SubmissionStatus::Queued => {
                row.status = SubmissionStatus::Running;
                row.attempt_id = Some(attempt.clone());
                row.owner_id = Some(owner.clone());
                row.lease_expires_at = Some(now.saturating_add_ms(lease_ms));
                row.attempt_count = row.attempt_count.saturating_add(1);
                row.clone()
            }
            _ => return Ok(None),
        };
        Ok(Some(Claim {
            attempt_id: attempt,
            row,
        }))
    }

    async fn mark_input_applied(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if let Some(seq) = inner.by_id.get(id.as_str()).copied() {
            if let Some(row) = inner.submissions.get_mut(&seq) {
                if row.input_applied_at.is_none() {
                    row.input_applied_at = Some(now);
                }
            }
        }
        Ok(())
    }

    async fn request_abort(&self, session: &SessionKey, now: UnixMillis) -> Result<u32> {
        let mut inner = self.inner.lock().await;
        let mut n = 0u32;
        for row in inner.submissions.values_mut() {
            if row.session_key.as_str() == session.as_str()
                && row.status != SubmissionStatus::Settled
            {
                row.abort_requested = true;
                n = n.saturating_add(1);
            }
        }
        let _ = now;
        drop(inner);
        self.ping();
        Ok(n)
    }

    async fn reserve_settlement(&self, id: &SubmissionId) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let seq = *inner
            .by_id
            .get(id.as_str())
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let row = inner
            .submissions
            .get_mut(&seq)
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        if !matches!(
            row.status,
            SubmissionStatus::Running | SubmissionStatus::Suspended | SubmissionStatus::Queued
        ) {
            return Err(StoreError::Conflict("could not reserve settlement".into()));
        }
        row.status = SubmissionStatus::Terminalizing;
        Ok(())
    }

    async fn finalize_settlement(
        &self,
        id: &SubmissionId,
        now: UnixMillis,
        error: Option<String>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let seq = *inner
            .by_id
            .get(id.as_str())
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        if let Some(row) = inner.submissions.get_mut(&seq) {
            row.status = SubmissionStatus::Settled;
            row.settled_at = Some(now);
            row.error = error;
            row.owner_id = None;
            row.lease_expires_at = None;
        }
        drop(inner);
        self.ping();
        Ok(())
    }

    async fn suspend(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let seq = *inner
            .by_id
            .get(id.as_str())
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        if let Some(row) = inner.submissions.get_mut(&seq) {
            if row.status == SubmissionStatus::Running {
                row.status = SubmissionStatus::Suspended;
                row.owner_id = None;
                row.lease_expires_at = None;
            }
        }
        let _ = now;
        drop(inner);
        self.ping();
        Ok(())
    }

    async fn put_suspension(&self, item: Suspension) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.suspensions.insert(
            (item.submission_id.to_string(), item.tool_call_id.clone()),
            item,
        );
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
        let mut inner = self.inner.lock().await;
        let seq = *inner
            .by_id
            .get(submission.as_str())
            .ok_or_else(|| StoreError::NotFound("submission".into()))?;
        let status = inner
            .submissions
            .get(&seq)
            .map(|r| r.status)
            .ok_or_else(|| StoreError::NotFound("submission".into()))?;
        if status == SubmissionStatus::Settled {
            return Err(StoreError::SubmissionSettled);
        }
        let key = (submission.to_string(), tool_call_id.to_string());
        let item = inner
            .suspensions
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound("approval".into()))?;
        if item.answered_at.is_some() {
            return Err(StoreError::AlreadyAnswered);
        }
        item.answered_at = Some(now);
        item.approved = Some(approved);
        item.answered_by = Some(by.to_string());
        let out = item.clone();
        if let Some(row) = inner.submissions.get_mut(&seq) {
            if row.status == SubmissionStatus::Suspended {
                row.status = SubmissionStatus::Queued;
            }
        }
        drop(inner);
        self.ping();
        Ok(out)
    }

    async fn list_suspensions(&self, submission: &SubmissionId) -> Result<Vec<Suspension>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .suspensions
            .values()
            .filter(|s| s.submission_id.as_str() == submission.as_str())
            .cloned()
            .collect())
    }

    async fn list_pending_approvals(&self, conversation: &ConversationId) -> Result<Vec<Suspension>> {
        let inner = self.inner.lock().await;
        let ids: Vec<String> = inner
            .submissions
            .values()
            .filter(|r| r.conversation_id.as_str() == conversation.as_str())
            .map(|r| r.submission_id.to_string())
            .collect();
        Ok(inner
            .suspensions
            .values()
            .filter(|s| ids.iter().any(|id| id == s.submission_id.as_str()) && s.answered_at.is_none())
            .cloned()
            .collect())
    }

    async fn get_submission(&self, id: &SubmissionId) -> Result<SubmissionRow> {
        let inner = self.inner.lock().await;
        let seq = *inner
            .by_id
            .get(id.as_str())
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        inner
            .submissions
            .get(&seq)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    async fn list_expired(&self, now: UnixMillis) -> Result<Vec<SubmissionRow>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .submissions
            .values()
            .filter(|r| {
                r.status == SubmissionStatus::Running
                    && r.lease_expires_at
                        .map(|t| t.as_millis() < now.as_millis())
                        .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn list_pending_settlements(&self) -> Result<Vec<SubmissionRow>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .submissions
            .values()
            .filter(|r| r.status == SubmissionStatus::Terminalizing)
            .cloned()
            .collect())
    }

    async fn requeue(&self, id: &SubmissionId) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if let Some(seq) = inner.by_id.get(id.as_str()).copied() {
            if let Some(row) = inner.submissions.get_mut(&seq) {
                row.status = SubmissionStatus::Queued;
                row.owner_id = None;
                row.lease_expires_at = None;
            }
        }
        drop(inner);
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
        let mut inner = self.inner.lock().await;
        let seq = *inner
            .by_id
            .get(id.as_str())
            .ok_or_else(|| StoreError::Conflict("replace_attempt lost the CAS".into()))?;
        let attempt = AttemptId::new();
        let row = match inner.submissions.get_mut(&seq) {
            Some(row)
                if matches!(
                    row.status,
                    SubmissionStatus::Running | SubmissionStatus::Queued
                ) =>
            {
                row.status = SubmissionStatus::Running;
                row.attempt_id = Some(attempt.clone());
                row.owner_id = Some(owner.clone());
                row.lease_expires_at = Some(now.saturating_add_ms(lease_ms));
                row.attempt_count = row.attempt_count.saturating_add(1);
                row.clone()
            }
            _ => return Err(StoreError::Conflict("replace_attempt lost the CAS".into())),
        };
        Ok(Claim {
            attempt_id: attempt,
            row,
        })
    }

    async fn expire_owner_leases(&self, owner: &OwnerId) -> Result<u32> {
        let mut inner = self.inner.lock().await;
        let mut n = 0u32;
        for row in inner.submissions.values_mut() {
            if row.owner_id.as_ref() == Some(owner) && row.status == SubmissionStatus::Running {
                row.status = SubmissionStatus::Queued;
                row.owner_id = None;
                row.lease_expires_at = None;
                n = n.saturating_add(1);
            }
        }
        drop(inner);
        self.ping();
        Ok(n)
    }

    async fn create_stream(
        &self,
        path: &str,
        _identity: &str,
        uid: &str,
        _now: UnixMillis,
    ) -> Result<(StreamOffset, String, bool)> {
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.streams.get(path) {
            return Ok((StreamOffset::ORIGIN, existing.uid.clone(), false));
        }
        inner.streams.insert(
            path.to_string(),
            StreamState {
                uid: uid.to_string(),
                next_seq: 1,
                incarnation: 0,
                batches: BTreeMap::new(),
            },
        );
        Ok((StreamOffset::ORIGIN, uid.to_string(), true))
    }

    async fn append(
        &self,
        path: &str,
        records: Vec<Record>,
        submission: Option<&SubmissionId>,
        attempt: Option<&AttemptId>,
    ) -> Result<RecordBatch> {
        let mut inner = self.inner.lock().await;
        if let (Some(sid), Some(aid)) = (submission, attempt) {
            let seq = inner
                .by_id
                .get(sid.as_str())
                .copied()
                .ok_or_else(|| StoreError::NotFound("submission".into()))?;
            let row = inner
                .submissions
                .get(&seq)
                .ok_or_else(|| StoreError::NotFound("submission".into()))?;
            oah_store::assert_append_fence(row, aid)?;
        }
        let stream = inner
            .streams
            .get_mut(path)
            .ok_or_else(|| StoreError::NotFound(format!("stream {path}")))?;
        let seq = stream.next_seq;
        stream.next_seq = stream.next_seq.saturating_add(1);
        let batch = RecordBatch {
            path: path.to_string(),
            seq,
            records,
            submission_id: submission.cloned(),
            attempt_id: attempt.cloned(),
        };
        stream.batches.insert(seq, batch.clone());
        drop(inner);
        self.ping();
        Ok(batch)
    }

    async fn read_after(
        &self,
        path: &str,
        after: StreamOffset,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let inner = self.inner.lock().await;
        let Some(stream) = inner.streams.get(path) else {
            return Ok(Vec::new());
        };
        let min_seq = if after.is_origin() { 0 } else { after.batch };
        Ok(stream
            .batches
            .values()
            .filter(|b| b.seq > min_seq)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn read_all(&self, path: &str) -> Result<Vec<Record>> {
        let batches = self.read_after(path, StreamOffset::ORIGIN, 10_000).await?;
        Ok(batches.into_iter().flat_map(|b| b.records).collect())
    }

    async fn stream_head(&self, path: &str) -> Result<(StreamOffset, u64, String)> {
        let inner = self.inner.lock().await;
        let stream = inner
            .streams
            .get(path)
            .ok_or_else(|| StoreError::NotFound(format!("stream {path}")))?;
        let head = if stream.next_seq <= 1 {
            StreamOffset::ORIGIN
        } else {
            StreamOffset::new(stream.next_seq - 1, 0)
        };
        Ok((head, stream.incarnation, stream.uid.clone()))
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
    use super::*;

    #[tokio::test]
    async fn migrate_is_noop() {
        let store = MemoryStore::new();
        store.migrate().await.unwrap();
    }

    #[tokio::test]
    async fn conformance() {
        // Fresh store per case: claim_runnable is process-global.
        oah_store_conformance::admit_idempotent_same_payload(&MemoryStore::new())
            .await
            .unwrap();
        oah_store_conformance::admit_idempotent_conflict(&MemoryStore::new())
            .await
            .unwrap();
        oah_store_conformance::claim_fifo_and_session_fence(&MemoryStore::new())
            .await
            .unwrap();
        oah_store_conformance::suspend_answer_once(&MemoryStore::new())
            .await
            .unwrap();
        oah_store_conformance::create_only_uid_conflict(&MemoryStore::new())
            .await
            .unwrap();
        oah_store_conformance::append_rejects_stale_attempt(&MemoryStore::new())
            .await
            .unwrap();
    }
}
