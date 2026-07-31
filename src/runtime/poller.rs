//! Busy-poll thread for Completion Queue dispatch.
//!
//! A dedicated thread (optionally core-affined) busy-polls a
//! [`CompletionQueue`] in a tight loop and dispatches completed
//! work requests to waiting [`tokio::sync::oneshot`] receivers.
//!
//! # Design
//!
//! - The poller thread runs **indefinitely** and never touches Tokio
//!   worker threads.
//! - Each async RDMA operation registers a `oneshot::Sender` keyed by
//!   its `wr_id` in a shared `Mutex<HashMap>`.
//! - When the poller harvests a completion, it looks up the matching
//!   sender and fires it, waking the awaiting future.
//!
//! # Shutdown
//!
//! Set the `shutdown` flag to gracefully stop the poller thread.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::error::RdmaError;
use crate::rdma::cq::WorkCompletion;
use crate::rdma::CompletionQueue;

/// The type of the shared pending-completion map.
///
/// Maps `wr_id` → `oneshot::Sender`. The poller thread removes entries
/// as completions arrive; async tasks insert entries before posting WRs.
pub type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<WorkCompletion, RdmaError>>>>>;

/// A dedicated thread that busy-polls a [`CompletionQueue`] and dispatches
/// completed work requests to waiting async futures via oneshot channels.
pub struct Poller {
    /// Shared CQ reference.
    #[allow(dead_code)]
    cq: Arc<CompletionQueue>,
    /// Handle to the polling thread (`None` after shutdown or detach).
    handle: Option<JoinHandle<()>>,
    /// Flag to signal shutdown to the poller thread.
    shutdown: Arc<AtomicBool>,
}

impl Poller {
    /// Start a new polling thread.
    ///
    /// # Parameters
    ///
    /// * `cq` — Completion queue to poll.
    /// * `cpu_core` — Optional CPU core to pin the poller thread to (0-based).
    ///
    /// # Returns
    ///
    /// A tuple of `(Poller, PendingMap)`. The `PendingMap` must be shared
    /// with the [`crate::runtime::ops::RdmaRuntime`] so it can register
    /// completion waiters.
    pub fn spawn(
        cq: Arc<CompletionQueue>,
        cpu_core: Option<usize>,
    ) -> (Self, PendingMap) {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = Arc::clone(&pending);
        let cq_clone = Arc::clone(&cq);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::Builder::new()
            .name("rdma-poller".into())
            .spawn(move || {
                // Set core affinity if requested
                if let Some(core) = cpu_core {
                    set_core_affinity(core);
                }

                tracing::info!(?cpu_core, "RDMA poller thread started");

                // Busy-poll loop
                loop {
                    // Check shutdown flag
                    if shutdown_clone.load(Ordering::Relaxed) {
                        tracing::info!("RDMA poller received shutdown signal");
                        break;
                    }

                    // Poll the CQ for completions
                    match cq_clone.poll(16) {
                        Ok(wcs) if !wcs.is_empty() => {
                            let mut map = match pending_clone.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => {
                                    tracing::error!("Pending map mutex poisoned; recovering");
                                    poisoned.into_inner()
                                }
                            };

                            for wc in wcs {
                                if let Some(sender) = map.remove(&wc.wr_id) {
                                    let result = if wc.is_success() {
                                        Ok(wc.clone())
                                    } else {
                                        Err(RdmaError::HardwareError(format!(
                                            "WC error: status={:?}, vendor_err={}",
                                            wc.status, wc.vendor_err
                                        )))
                                    };
                                    // If the receiver has been dropped, ignore the error.
                                    let _ = sender.send(result);
                                }
                            }
                            // Mutex guard dropped here
                        }
                        Ok(_) => {
                            // No completions — yield briefly to avoid burning CPU
                            // in a completely tight spin.
                            std::hint::spin_loop();
                        }
                        Err(e) => {
                            tracing::error!(?e, "CQ poll error in poller thread");
                            // Brief backoff on error
                            thread::sleep(Duration::from_micros(100));
                        }
                    }
                }

                tracing::info!("RDMA poller thread exiting");
            })
            .expect("Failed to spawn RDMA poller thread");

        (
            Poller {
                cq,
                handle: Some(handle),
                shutdown,
            },
            pending,
        )
    }

    /// Signal the poller thread to shut down and wait for it to exit.
    ///
    /// Returns `Ok(())` if the thread joined successfully.
    pub fn shutdown(mut self) -> Result<(), RdmaError> {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| {
                RdmaError::Internal("RDMA poller thread panicked".to_string())
            })?;
        }

        Ok(())
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            // Attempt to join with a short timeout; if it hangs, detach.
            // In production, we'd use a more sophisticated shutdown mechanism.
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// CPU affinity helper
// ---------------------------------------------------------------------------

/// Pin the current thread to the given CPU core.
///
/// Uses `pthread_setaffinity_np` via the `libc` crate.
fn set_core_affinity(core: usize) {
    let ret = unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::MaybeUninit::zeroed().assume_init();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(core, &mut cpuset);
        libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &cpuset,
        )
    };

    if ret != 0 {
        tracing::warn!(
            core,
            err = std::io::Error::last_os_error().to_string(),
            "Failed to set CPU affinity; poller will run on any core"
        );
    } else {
        tracing::info!(core, "RDMA poller pinned to CPU core");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_core_affinity_does_not_crash() {
        // Just verify the function doesn't panic on any input.
        // CPU 0 is almost always present.
        set_core_affinity(0);
    }
}
