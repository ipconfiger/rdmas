//! Retry logic and pending-operation tracking for one-sided RDMA.
//!
//! Design doc §四.5 / Wave 3 T3-D.
//!
//! # Retry policy
//!
//! A configurable retry policy with exponential backoff and jitter for
//! one-sided RDMA operations.  The [`retry_rdma_op`] wrapper only retries
//! errors where [`RdmaError::is_retriable`] returns `true` (Timeout,
//! CasFailed, VersionMismatch, NotConnected).
//!
//! # PendingTracker
//!
//! Tracks in-flight async operations.  When an operation times out or
//! fails, the tracker ensures the associated buffer is properly cleaned
//! up — preventing memory leaks from abandoned `Box::into_raw` pointers
//! in the distributed version.  In local simulation mode it simply tracks
//! pending IDs.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::error::RdmaError;

// ---------------------------------------------------------------------------
// RetryConfig
// ---------------------------------------------------------------------------

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: 3).
    pub max_retries: u32,
    /// Base delay between retries (default: 100 μs).
    pub base_delay: Duration,
    /// Maximum delay between retries (default: 10 ms).
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_micros(100),
            max_delay: Duration::from_millis(10),
        }
    }
}

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

/// Retry an operation with exponential backoff + jitter.
///
/// Every failure increments an attempt counter.  The delay between
/// attempts grows as `base_delay × 2^attempt`, capped at `max_delay`,
/// with ±25 % uniform jitter applied.
///
/// # Returns
///
/// * `Ok(result)` — the operation succeeded (possibly after retries).
/// * `Err(error)` — all retries exhausted; the last error is returned.
pub fn retry_with_backoff<T, F, E>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Debug,
{
    let mut rng = rand::thread_rng();
    let mut attempt = 0u32;

    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(err) => {
                if attempt >= config.max_retries {
                    return Err(err);
                }
                attempt += 1;

                // Exponential backoff: base × 2^attempt, capped at max_delay.
                let delay = config.base_delay * 2u32.pow(attempt);
                let delay = delay.min(config.max_delay);

                // Add jitter: ±25 %.
                let jitter = (delay.as_micros() as f64 * 0.25 * rng.gen_range(-1.0..1.0)) as i64;
                let final_delay = Duration::from_micros(
                    (delay.as_micros() as i64 + jitter).max(0) as u64,
                );

                std::thread::sleep(final_delay);
            }
        }
    }
}

/// Retry an RDMA operation **only if the error is retriable**.
///
/// Non-retriable errors (e.g. [`RdmaError::KvFull`], [`RdmaError::InvalidKey`],
/// [`RdmaError::HardwareError`]) are returned immediately without delay.
///
/// Retriable errors ([`RdmaError::Timeout`], [`RdmaError::CasFailed`],
/// [`RdmaError::VersionMismatch`], [`RdmaError::NotConnected`]) trigger
/// exponential backoff with jitter up to `max_retries` attempts.
pub fn retry_rdma_op<T, F>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T, RdmaError>
where
    F: FnMut() -> Result<T, RdmaError>,
{
    let mut rng = rand::thread_rng();
    let mut attempt = 0u32;

    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(err) => {
                if !err.is_retriable() || attempt >= config.max_retries {
                    return Err(err);
                }
                attempt += 1;

                let delay = config.base_delay * 2u32.pow(attempt);
                let delay = delay.min(config.max_delay);

                let jitter = (delay.as_micros() as f64 * 0.25 * rng.gen_range(-1.0..1.0)) as i64;
                let final_delay = Duration::from_micros(
                    (delay.as_micros() as i64 + jitter).max(0) as u64,
                );

                std::thread::sleep(final_delay);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PendingTracker
// ---------------------------------------------------------------------------

/// Tracks pending async RDMA operations.
///
/// Each operation is assigned a unique `future_id`.  When an operation
/// completes or times out, its entry is removed.  This prevents buffer
/// leaks in the distributed version where buffers are passed via
/// `Box::into_raw`.
///
/// In local simulation mode, it simply tracks pending IDs.
#[derive(Debug)]
pub struct PendingTracker {
    /// Map of `future_id` → `(start_time, description)`.
    pending: Mutex<HashMap<u64, (Instant, String)>>,
    /// Timeout for pending operations (default: 5 × P99 RTT ≈ 1 ms).
    timeout: Duration,
}

impl PendingTracker {
    /// Create a new `PendingTracker` with the given timeout.
    pub fn new(timeout: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            timeout,
        }
    }

    /// Register a new pending operation.
    pub fn register(&self, future_id: u64, description: &str) {
        let mut pending = self.pending.lock().unwrap();
        pending.insert(future_id, (Instant::now(), description.to_string()));
    }

    /// Mark an operation as completed.
    pub fn complete(&self, future_id: u64) {
        let mut pending = self.pending.lock().unwrap();
        pending.remove(&future_id);
    }

    /// Scan for timed-out operations and return their IDs together with
    /// the elapsed time since registration.
    ///
    /// The caller is responsible for cleaning up associated buffers.
    pub fn scan_timeouts(&self) -> Vec<(u64, Duration)> {
        let pending = self.pending.lock().unwrap();
        let now = Instant::now();

        pending
            .iter()
            .filter(|(_, (start, _))| now.duration_since(*start) > self.timeout)
            .map(|(id, (start, _))| (*id, now.duration_since(*start)))
            .collect()
    }

    /// Remove timed-out entries.  Returns the number removed.
    pub fn collect_timeouts(&self) -> usize {
        let mut pending = self.pending.lock().unwrap();
        let now = Instant::now();
        let before = pending.len();

        pending.retain(|_, (start, _)| now.duration_since(*start) <= self.timeout);

        before - pending.len()
    }

    /// Number of currently pending operations.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

impl Default for PendingTracker {
    fn default() -> Self {
        Self::new(Duration::from_millis(1))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    // use std::sync::Arc; — reserved for future PendingTracker sharing tests

    // ------------------------------------------------------------------
    // retry_with_backoff
    // ------------------------------------------------------------------

    #[test]
    fn test_retry_succeeds_on_first_attempt() {
        let config = RetryConfig::default();
        let result = retry_with_backoff(&config, || -> Result<i32, &str> { Ok(42) });
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_retry_succeeds_after_failures() {
        let config = RetryConfig {
            max_retries: 3,
            ..Default::default()
        };
        let mut attempts = 0;
        let result = retry_with_backoff(&config, || -> Result<i32, &str> {
            attempts += 1;
            if attempts < 3 {
                Err("fail")
            } else {
                Ok(99)
            }
        });
        assert_eq!(result, Ok(99));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn test_retry_exhausts_and_returns_error() {
        let config = RetryConfig {
            max_retries: 2,
            ..Default::default()
        };
        let result = retry_with_backoff(&config, || -> Result<i32, &str> {
            Err("always fail")
        });
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------
    // retry_rdma_op
    // ------------------------------------------------------------------

    #[test]
    fn test_rdma_retry_respects_retriability() {
        let config = RetryConfig::default();
        let result = retry_rdma_op(&config, || -> Result<i32, RdmaError> {
            Err(RdmaError::KvFull)
        });
        // KvFull is NOT retriable → fails immediately (0 sleep, no retry).
        assert!(result.is_err());
    }

    #[test]
    fn test_rdma_retry_retries_timeout() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_micros(10),
            ..Default::default()
        };
        let mut attempts = 0;
        let result = retry_rdma_op(&config, || -> Result<i32, RdmaError> {
            attempts += 1;
            if attempts < 3 {
                Err(RdmaError::Timeout)
            } else {
                Ok(1)
            }
        });
        assert_eq!(result, Ok(1));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn test_rdma_retry_non_retriable_does_not_retry() {
        // A non-retriable error should fail on the first attempt with no delay.
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            ..Default::default()
        };
        let start = Instant::now();
        let result = retry_rdma_op(&config, || -> Result<i32, RdmaError> {
            Err(RdmaError::InvalidKey)
        });
        let elapsed = start.elapsed();
        assert!(result.is_err());
        // Must not have waited for any backoff.
        assert!(
            elapsed < Duration::from_millis(1),
            "non-retriable error should not sleep, but took {elapsed:?}"
        );
    }

    // ------------------------------------------------------------------
    // PendingTracker
    // ------------------------------------------------------------------

    #[test]
    fn test_pending_tracker_register_and_complete() {
        let tracker = PendingTracker::default();
        tracker.register(1, "test_op");
        assert_eq!(tracker.pending_count(), 1);
        tracker.complete(1);
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_pending_tracker_timeout() {
        let tracker = PendingTracker::new(Duration::from_micros(100));
        tracker.register(1, "quick_op");
        std::thread::sleep(Duration::from_millis(1));
        let timeouts = tracker.scan_timeouts();
        assert!(!timeouts.is_empty());
        assert_eq!(timeouts[0].0, 1);
    }

    #[test]
    fn test_pending_tracker_collect_timeouts() {
        let tracker = PendingTracker::new(Duration::from_micros(100));
        tracker.register(1, "timeout_op");
        tracker.register(2, "also_timeout");
        std::thread::sleep(Duration::from_millis(1));
        let removed = tracker.collect_timeouts();
        assert_eq!(removed, 2);
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_pending_tracker_no_timeout_before_deadline() {
        let tracker = PendingTracker::new(Duration::from_secs(60));
        tracker.register(1, "long_timeout");
        let timeouts = tracker.scan_timeouts();
        assert!(timeouts.is_empty());
    }

    #[test]
    fn test_pending_tracker_multiple_registrations() {
        let tracker = PendingTracker::default();
        tracker.register(1, "a");
        tracker.register(2, "b");
        tracker.register(3, "c");
        assert_eq!(tracker.pending_count(), 3);
        tracker.complete(2);
        assert_eq!(tracker.pending_count(), 2);
    }

    #[test]
    fn test_pending_tracker_scan_does_not_remove() {
        let tracker = PendingTracker::new(Duration::from_micros(100));
        tracker.register(1, "op");
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(tracker.pending_count(), 1);
        let timeouts = tracker.scan_timeouts();
        assert_eq!(timeouts.len(), 1);
        // scan_timeouts is non-destructive.
        assert_eq!(tracker.pending_count(), 1);
    }

    // ------------------------------------------------------------------
    // RetryConfig defaults
    // ------------------------------------------------------------------

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay, Duration::from_micros(100));
        assert_eq!(config.max_delay, Duration::from_millis(10));
    }
}
