//! Normative constants from the harness PRD (Appendix B).

/// Default submission attempt budget.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 10;

/// Default submission wall-clock budget (ms), excluding suspended time.
pub const DEFAULT_TIMEOUT_MS: u64 = 3_600_000;

/// Coordinator lease duration (ms).
pub const LEASE_MS: u64 = 30_000;

/// Coordinator heartbeat interval (ms).
pub const HEARTBEAT_MS: u64 = 10_000;

/// Expired-lease scan interval (ms).
pub const EXPIRED_LEASE_SCAN_MS: u64 = 15_000;

/// Grace after deadline before force-settle (ms).
pub const SETTLE_GRACE_MS: u64 = 60_000;

/// Max in-loop transient model retries.
pub const TRANSIENT_MODEL_RETRIES: u32 = 3;

/// Max `use_agent_finish` continuation cycles per response.
pub const MAX_AGENT_FINISH_CYCLES: u32 = 32;

/// Max subagent / harness delegation depth.
pub const MAX_DELEGATION_DEPTH: u32 = 4;

/// Append batch size limit (bytes).
pub const MAX_BATCH_BYTES: usize = 12 * 1024 * 1024;

/// Fold checkpoint every N appends.
pub const FOLD_CHECKPOINT_INTERVAL: u64 = 64;

/// Spill threshold for SQLite/DO chunking (bytes).
pub const SPILL_THRESHOLD_BYTES: usize = 1024 * 1024;

/// Chunk size when spilling (bytes).
pub const SPILL_CHUNK_BYTES: usize = 512 * 1024;
