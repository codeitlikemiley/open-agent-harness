//! Storage traits. The ledger and the log must share a database so the
//! append fence can read the submission row in the same transaction.

#![forbid(unsafe_code)]

use async_trait::async_trait;
use oah_core::{
    AttemptId, ConversationId, OwnerId, Principal, Record, RecordBatch, SessionKey, StreamOffset,
    SubmissionId, UnixMillis,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    NotFound(String),
    #[error("already answered")]
    AlreadyAnswered,
    #[error("submission already settled")]
    SubmissionSettled,
    #[error("format version unsupported: {0}")]
    Format(String),
    #[error("{0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionStatus {
    Queued,
    Running,
    Joining,
    Joined,
    Suspended,
    Terminalizing,
    Settled,
}

impl SubmissionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Joining => "joining",
            Self::Joined => "joined",
            Self::Suspended => "suspended",
            Self::Terminalizing => "terminalizing",
            Self::Settled => "settled",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "joining" => Some(Self::Joining),
            "joined" => Some(Self::Joined),
            "suspended" => Some(Self::Suspended),
            "terminalizing" => Some(Self::Terminalizing),
            "settled" => Some(Self::Settled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryKind {
    User,
    Signal,
}

impl DeliveryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Signal => "signal",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmissionRow {
    pub sequence: i64,
    pub submission_id: SubmissionId,
    pub session_key: SessionKey,
    pub conversation_id: ConversationId,
    pub kind: DeliveryKind,
    pub payload: Value,
    pub status: SubmissionStatus,
    pub accepted_at: UnixMillis,
    pub attempt_id: Option<AttemptId>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub owner_id: Option<OwnerId>,
    pub lease_expires_at: Option<UnixMillis>,
    pub principal: Principal,
    pub idempotency_key: Option<String>,
    pub abort_requested: bool,
    pub input_applied_at: Option<UnixMillis>,
    pub error: Option<String>,
    pub settled_at: Option<UnixMillis>,
}

#[derive(Debug, Clone)]
pub struct AdmitRequest {
    pub conversation_id: ConversationId,
    pub session_key: SessionKey,
    pub kind: DeliveryKind,
    pub payload: Value,
    pub principal: Principal,
    pub idempotency_key: Option<String>,
    pub uid: Option<String>,
    pub max_attempts: u32,
}

#[derive(Debug, Clone)]
pub struct AdmitReceipt {
    pub submission_id: SubmissionId,
    pub offset: StreamOffset,
    pub uid: String,
    pub deduplicated: bool,
}

#[derive(Debug, Clone)]
pub struct Claim {
    pub row: SubmissionRow,
    pub attempt_id: AttemptId,
}

#[derive(Debug, Clone)]
pub struct Suspension {
    pub submission_id: SubmissionId,
    pub tool_call_id: String,
    pub tool: String,
    pub effective_args: Value,
    pub reason: String,
    pub kind: String,
    pub created_at: UnixMillis,
    pub answered_at: Option<UnixMillis>,
    pub approved: Option<bool>,
    pub answered_by: Option<String>,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn migrate(&self) -> Result<()>;

    async fn admit(&self, req: AdmitRequest, now: UnixMillis) -> Result<AdmitReceipt>;

    async fn claim_runnable(
        &self,
        owner: &OwnerId,
        now: UnixMillis,
        lease_ms: i64,
    ) -> Result<Option<Claim>>;

    async fn mark_input_applied(&self, id: &SubmissionId, now: UnixMillis) -> Result<()>;

    async fn request_abort(&self, session: &SessionKey, now: UnixMillis) -> Result<u32>;

    async fn reserve_settlement(&self, id: &SubmissionId) -> Result<()>;

    async fn finalize_settlement(
        &self,
        id: &SubmissionId,
        now: UnixMillis,
        error: Option<String>,
    ) -> Result<()>;

    async fn suspend(&self, id: &SubmissionId, now: UnixMillis) -> Result<()>;

    async fn answer_suspension(
        &self,
        submission: &SubmissionId,
        tool_call_id: &str,
        approved: bool,
        by: &str,
        now: UnixMillis,
    ) -> Result<Suspension>;

    async fn put_suspension(&self, item: Suspension) -> Result<()>;

    async fn list_suspensions(&self, submission: &SubmissionId) -> Result<Vec<Suspension>>;

    async fn list_pending_approvals(&self, conversation: &ConversationId) -> Result<Vec<Suspension>>;

    async fn get_submission(&self, id: &SubmissionId) -> Result<SubmissionRow>;

    async fn list_expired(&self, now: UnixMillis) -> Result<Vec<SubmissionRow>>;

    async fn list_pending_settlements(&self) -> Result<Vec<SubmissionRow>>;

    async fn requeue(&self, id: &SubmissionId) -> Result<()>;

    async fn replace_attempt(
        &self,
        id: &SubmissionId,
        owner: &OwnerId,
        now: UnixMillis,
        lease_ms: i64,
    ) -> Result<Claim>;

    async fn expire_owner_leases(&self, owner: &OwnerId) -> Result<u32>;

    async fn create_stream(
        &self,
        path: &str,
        identity: &str,
        uid: &str,
        now: UnixMillis,
    ) -> Result<(StreamOffset, String, bool)>;

    async fn append(
        &self,
        path: &str,
        records: Vec<Record>,
        submission: Option<&SubmissionId>,
        attempt: Option<&AttemptId>,
    ) -> Result<RecordBatch>;

    async fn read_after(
        &self,
        path: &str,
        after: StreamOffset,
        limit: usize,
    ) -> Result<Vec<RecordBatch>>;

    async fn read_all(&self, path: &str) -> Result<Vec<Record>>;

    async fn stream_head(&self, path: &str) -> Result<(StreamOffset, u64, String)>;

    fn notify(&self) -> tokio::sync::watch::Receiver<u64>;

    fn wake(&self);
}

/// Reject an append from a producer whose attempt is no longer live.
pub fn assert_append_fence(row: &SubmissionRow, attempt: &AttemptId) -> Result<()> {
    if row.status == SubmissionStatus::Settled {
        return Err(StoreError::Conflict("stale_append_fence".into()));
    }
    match &row.attempt_id {
        Some(live) if live == attempt => Ok(()),
        _ => Err(StoreError::Conflict("stale_append_fence".into())),
    }
}

#[async_trait]
pub trait StoreExt: Store {}

pub mod crash;
pub use crash::CrashInjectStore;
