//! Crash-injection wrapper. Fails *before* a durable write so the inner
//! store is not mutated. Used by recovery tests (PRD: inject at every write).

use crate::{
    AdmitReceipt, AdmitRequest, Claim, Result, Store, StoreError, SubmissionRow, Suspension,
};
use async_trait::async_trait;
use oah_core::{
    AttemptId, ConversationId, OwnerId, Record, RecordBatch, SessionKey, StreamOffset, SubmissionId,
    UnixMillis,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// 1-based write index to fail on. `0` disables injection.
pub struct CrashInjectStore {
    inner: Arc<dyn Store>,
    fail_at: AtomicU64,
    writes: AtomicU64,
}

impl CrashInjectStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            fail_at: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        }
    }

    pub fn fail_at(&self, n: u64) {
        self.fail_at.store(n, Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.fail_at.store(0, Ordering::SeqCst);
    }

    pub fn write_count(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }

    fn hit(&self, op: &'static str) -> Result<()> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        let fail_at = self.fail_at.load(Ordering::SeqCst);
        if fail_at != 0 && n == fail_at {
            return Err(StoreError::Backend(format!("injected crash at write #{n} ({op})")));
        }
        Ok(())
    }
}

#[async_trait]
impl Store for CrashInjectStore {
    async fn migrate(&self) -> Result<()> {
        self.inner.migrate().await
    }

    async fn admit(&self, req: AdmitRequest, now: UnixMillis) -> Result<AdmitReceipt> {
        self.hit("admit")?;
        self.inner.admit(req, now).await
    }

    async fn claim_runnable(
        &self,
        owner: &OwnerId,
        now: UnixMillis,
        lease_ms: i64,
    ) -> Result<Option<Claim>> {
        self.inner.claim_runnable(owner, now, lease_ms).await
    }

    async fn mark_input_applied(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        self.hit("mark_input_applied")?;
        self.inner.mark_input_applied(id, now).await
    }

    async fn request_abort(&self, session: &SessionKey, now: UnixMillis) -> Result<u32> {
        self.hit("request_abort")?;
        self.inner.request_abort(session, now).await
    }

    async fn reserve_settlement(&self, id: &SubmissionId) -> Result<()> {
        self.hit("reserve_settlement")?;
        self.inner.reserve_settlement(id).await
    }

    async fn finalize_settlement(
        &self,
        id: &SubmissionId,
        now: UnixMillis,
        error: Option<String>,
    ) -> Result<()> {
        self.hit("finalize_settlement")?;
        self.inner.finalize_settlement(id, now, error).await
    }

    async fn suspend(&self, id: &SubmissionId, now: UnixMillis) -> Result<()> {
        self.hit("suspend")?;
        self.inner.suspend(id, now).await
    }

    async fn answer_suspension(
        &self,
        submission: &SubmissionId,
        tool_call_id: &str,
        approved: bool,
        by: &str,
        now: UnixMillis,
    ) -> Result<Suspension> {
        self.hit("answer_suspension")?;
        self.inner
            .answer_suspension(submission, tool_call_id, approved, by, now)
            .await
    }

    async fn put_suspension(&self, item: Suspension) -> Result<()> {
        self.hit("put_suspension")?;
        self.inner.put_suspension(item).await
    }

    async fn list_suspensions(&self, submission: &SubmissionId) -> Result<Vec<Suspension>> {
        self.inner.list_suspensions(submission).await
    }

    async fn list_pending_approvals(&self, conversation: &ConversationId) -> Result<Vec<Suspension>> {
        self.inner.list_pending_approvals(conversation).await
    }

    async fn get_submission(&self, id: &SubmissionId) -> Result<SubmissionRow> {
        self.inner.get_submission(id).await
    }

    async fn list_expired(&self, now: UnixMillis) -> Result<Vec<SubmissionRow>> {
        self.inner.list_expired(now).await
    }

    async fn list_pending_settlements(&self) -> Result<Vec<SubmissionRow>> {
        self.inner.list_pending_settlements().await
    }

    async fn requeue(&self, id: &SubmissionId) -> Result<()> {
        self.hit("requeue")?;
        self.inner.requeue(id).await
    }

    async fn replace_attempt(
        &self,
        id: &SubmissionId,
        owner: &OwnerId,
        now: UnixMillis,
        lease_ms: i64,
    ) -> Result<Claim> {
        self.hit("replace_attempt")?;
        self.inner.replace_attempt(id, owner, now, lease_ms).await
    }

    async fn expire_owner_leases(&self, owner: &OwnerId) -> Result<u32> {
        self.hit("expire_owner_leases")?;
        self.inner.expire_owner_leases(owner).await
    }

    async fn create_stream(
        &self,
        path: &str,
        identity: &str,
        uid: &str,
        now: UnixMillis,
    ) -> Result<(StreamOffset, String, bool)> {
        self.hit("create_stream")?;
        self.inner.create_stream(path, identity, uid, now).await
    }

    async fn append(
        &self,
        path: &str,
        records: Vec<Record>,
        submission: Option<&SubmissionId>,
        attempt: Option<&AttemptId>,
    ) -> Result<RecordBatch> {
        self.hit("append")?;
        self.inner.append(path, records, submission, attempt).await
    }

    async fn read_after(
        &self,
        path: &str,
        after: StreamOffset,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        self.inner.read_after(path, after, limit).await
    }

    async fn read_all(&self, path: &str) -> Result<Vec<Record>> {
        self.inner.read_all(path).await
    }

    async fn stream_head(&self, path: &str) -> Result<(StreamOffset, u64, String)> {
        self.inner.stream_head(path).await
    }

    fn notify(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.notify()
    }

    fn wake(&self) {
        self.inner.wake();
    }
}
