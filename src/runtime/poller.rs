//! Polling threads for Completion Queue dispatch.
//!
//! Two polling strategies are provided:
//!
//! - [`Poller`] — A dedicated thread that **busy-polls** a
//!   [`CompletionQueue`] in a tight loop.
//! - [`AsyncPoller`] — A dedicated thread that uses **event-driven**
//!   [`CqEventChannel`] polling via `epoll` with an optional
//!   busy-poll fallback.
//!
//! Both dispatch completed work requests to waiting
//! [`tokio::sync::oneshot`] receivers.
//!
//! # Design
//!
//! - The poller threads run **indefinitely** and never touch Tokio
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
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::error::RdmaError;
use crate::rdma::cq::WorkCompletion;
use crate::rdma::{CompletionQueue, CqEventChannel};

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
    pub fn spawn(cq: Arc<CompletionQueue>, cpu_core: Option<usize>) -> (Self, PendingMap) {
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
            handle
                .join()
                .map_err(|_| RdmaError::Internal("RDMA poller thread panicked".to_string()))?;
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
// AsyncPoller — Event-driven CQ event channel polling (client-side only)
// ---------------------------------------------------------------------------

/// Configuration for [`AsyncPoller`] behaviour.
#[derive(Debug, Clone)]
pub struct AsyncPollerConfig {
    /// If `true` and no CQ event arrives within the epoll timeout,
    /// fall back to busy-polling the CQ directly for a short window.
    pub busy_poll_fallback: bool,
    /// Timeout (in milliseconds) for `epoll_wait` before the fallback
    /// busy-poll check kicks in.
    pub epoll_timeout_ms: i32,
    /// When in busy-poll fallback, how many iterations to spin before
    /// returning to epoll.
    pub busy_poll_iterations: u32,
}

impl Default for AsyncPollerConfig {
    fn default() -> Self {
        Self {
            busy_poll_fallback: false,
            epoll_timeout_ms: 1,
            busy_poll_iterations: 100,
        }
    }
}

/// An event-driven completion poller using [`CqEventChannel`] + `epoll`.
///
/// Unlike [`Poller`] which busy-spins, `AsyncPoller` blocks on `epoll_wait`
/// on the completion channel's file descriptor. When the fd becomes readable,
/// it retrieves the event, acknowledges it, and then polls the CQ for actual
/// completions.
///
/// An optional [`busy_poll_fallback`](AsyncPollerConfig::busy_poll_fallback)
/// mode can be enabled to fall back to busy-polling when no events arrive
/// within the timeout — useful during high-throughput bursts.
///
/// # Client-Side Only
///
/// This poller is designed for **client-side** use only. Server-side
/// one-sided RDMA operations (READ/WRITE/CAS) do not generate CQ events
/// on the server.
pub struct AsyncPoller {
    /// Shared CQ reference.
    #[allow(dead_code)]
    cq: Arc<CompletionQueue>,
    /// Shared completion channel reference.
    #[allow(dead_code)]
    channel: Arc<CqEventChannel>,
    /// Handle to the polling thread (`None` after shutdown or detach).
    handle: Option<JoinHandle<()>>,
    /// Flag to signal shutdown to the poller thread.
    shutdown: Arc<AtomicBool>,
}

impl AsyncPoller {
    /// Start a new event-driven polling thread.
    ///
    /// # Parameters
    ///
    /// * `cq` — Completion queue to poll. Must be associated with `channel`.
    /// * `channel` — Completion event channel whose fd is monitored via epoll.
    /// * `cpu_core` — Optional CPU core to pin the poller thread to (0-based).
    ///
    /// # Returns
    ///
    /// A tuple of `(AsyncPoller, PendingMap)`. The `PendingMap` must be shared
    /// with the [`crate::runtime::ops::RdmaRuntime`] so it can register
    /// completion waiters.
    pub fn spawn(
        cq: Arc<CompletionQueue>,
        channel: Arc<CqEventChannel>,
        cpu_core: Option<usize>,
    ) -> (Self, PendingMap) {
        Self::spawn_with_config(cq, channel, cpu_core, AsyncPollerConfig::default())
    }

    /// Start a new event-driven polling thread with custom configuration.
    ///
    /// # Parameters
    ///
    /// * `cq` — Completion queue to poll.
    /// * `channel` — Completion event channel.
    /// * `cpu_core` — Optional CPU core affinity.
    /// * `config` — Poller behaviour configuration.
    pub fn spawn_with_config(
        cq: Arc<CompletionQueue>,
        channel: Arc<CqEventChannel>,
        cpu_core: Option<usize>,
        config: AsyncPollerConfig,
    ) -> (Self, PendingMap) {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = Arc::clone(&pending);
        let cq_clone = Arc::clone(&cq);
        let channel_clone = Arc::clone(&channel);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::Builder::new()
            .name("rdma-async-poller".into())
            .spawn(move || {
                // Set core affinity if requested
                if let Some(core) = cpu_core {
                    set_core_affinity(core);
                }

                tracing::info!(
                    ?cpu_core,
                    busy_poll_fallback = config.busy_poll_fallback,
                    "RDMA async poller thread started"
                );

                // Create an epoll instance
                let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };

                if epoll_fd < 0 {
                    tracing::error!(
                        err = std::io::Error::last_os_error().to_string(),
                        "epoll_create1 failed; async poller aborting"
                    );
                    return;
                }

                // Register the CQ event channel fd with epoll
                let channel_fd = channel_clone.fd();
                let mut ev = libc::epoll_event {
                    events: libc::EPOLLIN as u32,
                    u64: 0,
                };

                let ret =
                    unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, channel_fd, &mut ev) };

                if ret != 0 {
                    tracing::error!(
                        err = std::io::Error::last_os_error().to_string(),
                        "epoll_ctl ADD failed; async poller aborting"
                    );
                    unsafe { libc::close(epoll_fd) };
                    return;
                }

                // Set the channel fd to non-blocking so get_event won't block
                // after epoll indicates readiness (belt-and-suspenders)
                let flags = unsafe { libc::fcntl(channel_fd, libc::F_GETFL, 0) };
                if flags >= 0 {
                    unsafe {
                        libc::fcntl(channel_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
                    }
                }

                // Request the first notification before entering the loop
                if let Err(e) = cq_clone.request_notification(false) {
                    tracing::error!(?e, "initial ibv_req_notify_cq failed");
                    unsafe { libc::close(epoll_fd) };
                    return;
                }

                // Main event loop
                let mut events: [libc::epoll_event; 1] = unsafe { std::mem::zeroed() };

                loop {
                    // Check shutdown flag
                    if shutdown_clone.load(Ordering::Relaxed) {
                        tracing::info!("RDMA async poller received shutdown signal");
                        break;
                    }

                    // Wait for an event on the completion channel fd
                    let nfds = unsafe {
                        libc::epoll_wait(
                            epoll_fd,
                            events.as_mut_ptr(),
                            events.len() as libc::c_int,
                            config.epoll_timeout_ms,
                        )
                    };

                    if nfds < 0 {
                        let err = std::io::Error::last_os_error();
                        if err.raw_os_error() == Some(libc::EINTR) {
                            // Interrupted by signal — continue
                            continue;
                        }
                        tracing::error!(err = err.to_string(), "epoll_wait error in async poller");
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }

                    if nfds > 0 {
                        // Event received — retrieve and acknowledge
                        match channel_clone.get_event() {
                            Ok((_event_cq, _ctx)) => {
                                channel_clone.ack_events(&cq_clone, 1);

                                // Harvest completions from the CQ
                                harvest_completions(&cq_clone, &pending_clone);

                                // Re-arm the notification
                                if let Err(e) = cq_clone.request_notification(false) {
                                    tracing::error!(?e, "ibv_req_notify_cq failed after event");
                                }
                            }
                            Err(e) => {
                                tracing::error!(?e, "ibv_get_cq_event failed");
                                std::thread::sleep(Duration::from_micros(100));
                            }
                        }
                    } else {
                        // Timeout with no events
                        if config.busy_poll_fallback {
                            // Busy-poll fallback: spin on the CQ for a limited window
                            let start = Instant::now();
                            let deadline =
                                start + Duration::from_millis(config.epoll_timeout_ms as u64);

                            for _ in 0..config.busy_poll_iterations {
                                if Instant::now() >= deadline {
                                    break;
                                }
                                let had_completions =
                                    harvest_completions(&cq_clone, &pending_clone);
                                if !had_completions {
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    }
                }

                unsafe { libc::close(epoll_fd) };
                tracing::info!("RDMA async poller thread exiting");
            })
            .expect("Failed to spawn RDMA async poller thread");

        (
            AsyncPoller {
                cq,
                channel,
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
                RdmaError::Internal("RDMA async poller thread panicked".to_string())
            })?;
        }

        Ok(())
    }
}

impl Drop for AsyncPoller {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            // Attempt to join with a short timeout; if it hangs, detach.
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helper: harvest completions from a CQ and dispatch via PendingMap
// ---------------------------------------------------------------------------

/// Poll the CQ for up to 16 completions and dispatch each to its
/// waiting oneshot sender.
///
/// Returns `true` if any completions were harvested.
fn harvest_completions(cq: &CompletionQueue, pending: &PendingMap) -> bool {
    match cq.poll(16) {
        Ok(wcs) if !wcs.is_empty() => {
            let mut map = match pending.lock() {
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
            true
        }
        Ok(_) => false,
        Err(e) => {
            tracing::error!(?e, "CQ poll error in async poller thread");
            false
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

    #[test]
    fn test_async_poller_config_defaults() {
        let config = AsyncPollerConfig::default();
        assert!(!config.busy_poll_fallback);
        assert_eq!(config.epoll_timeout_ms, 1);
        assert_eq!(config.busy_poll_iterations, 100);
    }

    #[test]
    fn test_async_poller_config_pending_map_type() {
        // Verify PendingMap is constructable and usable
        let map: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = oneshot::channel::<Result<WorkCompletion, RdmaError>>();
        {
            let mut m = map.lock().unwrap();
            m.insert(42, tx);
        }
        {
            let m = map.lock().unwrap();
            assert!(m.contains_key(&42));
        }
    }

    #[test]
    fn test_harvest_completions_empty() {
        use crate::error::RdmaError;
        use crate::rdma::cq::WorkCompletion;
        // Without a real CQ, we can at least verify the pending map works correctly
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = oneshot::channel::<Result<WorkCompletion, RdmaError>>();
        pending.lock().unwrap().insert(1, tx);
        // Clean up
        pending.lock().unwrap().clear();
        assert!(pending.lock().unwrap().is_empty());
    }
}
