use crate::session::{process_claim, ProcessOutcome};
use crate::Runtime;
use oah_core::{OwnerId, UnixMillis};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use oah_core::LEASE_MS;

const SCAN_MS: u64 = 250;

/// Native coordinator: claim runnable rows, recover expired leases, process.
pub struct Coordinator {
    runtime: Arc<Runtime>,
}

impl Coordinator {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }

    pub fn owner(&self) -> &OwnerId {
        &self.runtime.owner
    }

    pub async fn tick(&self, cancel: &CancellationToken) -> Result<u32, crate::RuntimeError> {
        crate::session::drain_pending_settlements(&self.runtime).await?;
        let now = UnixMillis::now_system();
        let expired = self.runtime.store.list_expired(now).await?;
        for row in expired {
            info!(submission = %row.submission_id, "lease expired; replacing attempt");
            match self
                .runtime
                .store
                .replace_attempt(&row.submission_id, &self.runtime.owner, now, LEASE_MS as i64)
                .await
            {
                Ok(claim) => {
                    let child = cancel.child_token();
                    if let Err(err) = process_claim(&self.runtime, claim, child).await {
                        warn!(error = %err, "recovery process failed");
                    }
                }
                Err(err) => warn!(error = %err, "replace_attempt failed"),
            }
        }

        let mut n = 0u32;
        while !cancel.is_cancelled() {
            let claim = self
                .runtime
                .store
                .claim_runnable(&self.runtime.owner, UnixMillis::now_system(), LEASE_MS as i64)
                .await?;
            let Some(claim) = claim else {
                break;
            };
            n = n.saturating_add(1);
            let child = cancel.child_token();
            match process_claim(&self.runtime, claim, child).await {
                Ok(ProcessOutcome::SettledCompleted) => {}
                Ok(other) => info!(?other, "submission finished"),
                Err(err) => warn!(error = %err, "process failed"),
            }
        }
        Ok(n)
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut notify = self.runtime.store.notify();
        loop {
            if cancel.is_cancelled() {
                if let Err(err) = self
                    .runtime
                    .store
                    .expire_owner_leases(&self.runtime.owner)
                    .await
                {
                    warn!(error = %err, "failed to expire leases on shutdown");
                }
                break;
            }
            let _ = self.tick(&cancel).await;
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = notify.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(SCAN_MS)) => {}
            }
        }
    }
}
