//! No-I/O core of the open-agent-harness.
//!
//! This crate owns newtype identifiers, the append-only conversation record
//! vocabulary, the pure reducer, and the recovery classifier. Nothing here
//! talks to a database, a model, or the network.

#![forbid(unsafe_code)]

pub mod classifier;
pub mod constants;
pub mod error;
pub mod id;
pub mod json;
pub mod offset;
pub mod principal;
pub mod record;
pub mod reducer;
pub mod time;

pub use classifier::{classify, recover_action, ClassifierInput, RecoverAction, RecoveryClass};
pub use constants::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_TIMEOUT_MS, EXPIRED_LEASE_SCAN_MS, FOLD_CHECKPOINT_INTERVAL,
    HEARTBEAT_MS, LEASE_MS, MAX_AGENT_FINISH_CYCLES, MAX_BATCH_BYTES, MAX_DELEGATION_DEPTH,
    SETTLE_GRACE_MS, SPILL_CHUNK_BYTES, SPILL_THRESHOLD_BYTES, TRANSIENT_MODEL_RETRIES,
};
pub use error::{CoreError, Result};
pub use id::{
    AgentName, AttemptId, AttachmentId, ConversationId, InstanceId, OperationId, OwnerId,
    RecordId, SandboxKey, ScheduleId, SessionKey, SubmissionId, ToolCallId, TurnId,
};
pub use json::{canonical_string, canonical_value};
pub use offset::StreamOffset;
pub use principal::Principal;
pub use record::{
    harness_info, HarnessInfo, Record, RecordBatch, RecordBody, SettlementError, SettlementOutcome,
    RECORD_FORMAT_VERSION, STORE_FORMAT_VERSION,
};
pub use reducer::{reduce_batch, FoldState, ProjectedMessage, ProjectedPart, ProjectedSettlement};
pub use time::UnixMillis;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
