//! Durable Object lease + fencing protocol.
//!
//! The workers-rs host is a thin wrapper around this state machine. Native
//! tests cover claim, expiry, and stale-generation rejection so the wasm
//! build does not have to run in this workspace.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableLease {
    pub generation: u64,
    pub owner: String,
    pub expires_at_ms: i64,
}

/// Single-threaded DO claim. A live foreign owner cannot be stolen.
pub fn try_claim(
    current: Option<&DurableLease>,
    now_ms: i64,
    owner: &str,
    lease_ms: i64,
) -> Option<DurableLease> {
    match current {
        Some(lease) if lease.expires_at_ms > now_ms && lease.owner != owner => None,
        Some(lease) => Some(DurableLease {
            generation: lease.generation.saturating_add(1),
            owner: owner.to_string(),
            expires_at_ms: now_ms.saturating_add(lease_ms),
        }),
        None => Some(DurableLease {
            generation: 1,
            owner: owner.to_string(),
            expires_at_ms: now_ms.saturating_add(lease_ms),
        }),
    }
}

pub fn heartbeat(lease: &DurableLease, now_ms: i64, lease_ms: i64) -> DurableLease {
    DurableLease {
        generation: lease.generation,
        owner: lease.owner.clone(),
        expires_at_ms: now_ms.saturating_add(lease_ms),
    }
}

/// Append fence: the producer token must match the live generation.
pub fn fence_ok(held: &DurableLease, observed_generation: u64) -> bool {
    held.generation == observed_generation
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn cannot_steal_live_lease() {
        let a = try_claim(None, 0, "own_a", 30_000).unwrap();
        assert!(try_claim(Some(&a), 1, "own_b", 30_000).is_none());
        let b = try_claim(Some(&a), 40_000, "own_b", 30_000).unwrap();
        assert_eq!(b.generation, 2);
        assert!(fence_ok(&b, 2));
        assert!(!fence_ok(&b, 1));
    }

    #[test]
    fn owner_can_refresh() {
        let a = try_claim(None, 0, "own_a", 30_000).unwrap();
        let again = try_claim(Some(&a), 10, "own_a", 30_000).unwrap();
        assert_eq!(again.generation, 2);
        let beat = heartbeat(&again, 20, 30_000);
        assert_eq!(beat.generation, 2);
        assert_eq!(beat.expires_at_ms, 30_020);
    }
}
