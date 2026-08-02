//! Memory watermark monitoring and alerting (T10-C).
//!
//! Background thread monitors hash table load factor, extent region usage,
//! and slab chunk utilization. When thresholds are exceeded, triggers
//! notifications to connected clients.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for watermark monitoring.
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Check interval (default: 5 seconds).
    pub check_interval_ms: u64,
    /// Hash table load factor threshold (0.0-1.0, default: 0.80).
    pub table_load_threshold: f64,
    /// Extent region usage threshold (0.0-1.0, default: 0.85).
    pub extent_usage_threshold: f64,
    /// Slab chunk usage threshold (0.0-1.0, default: 0.85).
    pub slab_usage_threshold: f64,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 5000,
            table_load_threshold: 0.80,
            extent_usage_threshold: 0.85,
            slab_usage_threshold: 0.85,
        }
    }
}

/// Current watermark status.
#[derive(Debug, Clone)]
pub struct WatermarkStatus {
    pub table_load: f64,
    pub extent_usage: f64,
    pub slab_usage: f64,
    pub any_exceeded: bool,
    pub exceeded_regions: Vec<String>,
}

/// Watermark monitor with callback notification.
pub struct WatermarkMonitor {
    config: WatermarkConfig,
    /// Callback invoked when any threshold is exceeded.
    on_exceeded: Option<Box<dyn Fn(WatermarkStatus) + Send + Sync>>,
    /// Shutdown flag.
    shutdown: Arc<AtomicBool>,
}

impl WatermarkMonitor {
    /// Create a new monitor with the given config.
    pub fn new(config: WatermarkConfig) -> Self {
        Self {
            config,
            on_exceeded: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the callback for threshold exceeded events.
    pub fn on_exceeded<F: Fn(WatermarkStatus) + Send + Sync + 'static>(&mut self, f: F) {
        self.on_exceeded = Some(Box::new(f));
    }

    /// Check current status against thresholds.
    ///
    /// # Arguments
    /// * `table_key_count` — current number of keys in hash table.
    /// * `table_capacity` — max buckets in hash table.
    /// * `extent_used` — bytes used in extent region.
    /// * `extent_capacity` — total bytes in extent region.
    /// * `slab_allocated` — number of allocated slab chunks.
    /// * `slab_total` — total number of slab chunks.
    pub fn check(
        &self,
        table_key_count: u64,
        table_capacity: u64,
        extent_used: u64,
        extent_capacity: u64,
        slab_allocated: u64,
        slab_total: u64,
    ) -> WatermarkStatus {
        let table_load = if table_capacity > 0 {
            table_key_count as f64 / table_capacity as f64
        } else {
            0.0
        };

        let extent_usage = if extent_capacity > 0 {
            extent_used as f64 / extent_capacity as f64
        } else {
            0.0
        };

        let slab_usage = if slab_total > 0 {
            slab_allocated as f64 / slab_total as f64
        } else {
            0.0
        };

        let mut exceeded_regions = Vec::new();

        if table_load > self.config.table_load_threshold {
            exceeded_regions.push("hash_table".to_string());
        }
        if extent_usage > self.config.extent_usage_threshold {
            exceeded_regions.push("extent_region".to_string());
        }
        if slab_usage > self.config.slab_usage_threshold {
            exceeded_regions.push("slab_region".to_string());
        }

        let any_exceeded = !exceeded_regions.is_empty();

        let status = WatermarkStatus {
            table_load,
            extent_usage,
            slab_usage,
            any_exceeded,
            exceeded_regions,
        };

        // Fire callback if threshold exceeded
        if any_exceeded {
            if let Some(ref callback) = self.on_exceeded {
                callback(status.clone());
            }
        }

        status
    }

    /// Returns true if any threshold is exceeded for the given status.
    pub fn is_exceeded(&self, status: &WatermarkStatus) -> bool {
        status.any_exceeded
    }

    /// Get a handle to the shutdown flag so external code can request stop.
    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Return the configured check interval as a Duration.
    pub fn check_interval(&self) -> Duration {
        Duration::from_millis(self.config.check_interval_ms)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = WatermarkConfig::default();
        assert_eq!(config.check_interval_ms, 5000);
        assert!((config.table_load_threshold - 0.80).abs() < 1e-9);
        assert!((config.extent_usage_threshold - 0.85).abs() < 1e-9);
        assert!((config.slab_usage_threshold - 0.85).abs() < 1e-9);
    }

    #[test]
    fn check_below_thresholds_is_ok() {
        let config = WatermarkConfig::default();
        let monitor = WatermarkMonitor::new(config);

        // 500 keys in 1000 buckets = 50% load
        let status = monitor.check(500, 1000, 500_000, 1_000_000, 50, 100);
        assert!(!status.any_exceeded);
        assert!(status.exceeded_regions.is_empty());
        assert!((status.table_load - 0.5).abs() < 1e-9);
        assert!((status.extent_usage - 0.5).abs() < 1e-9);
        assert!((status.slab_usage - 0.5).abs() < 1e-9);
    }

    #[test]
    fn check_table_load_exceeded() {
        let config = WatermarkConfig::default();
        let monitor = WatermarkMonitor::new(config);

        // 900 keys in 1000 buckets = 90% load -> exceeds 80% threshold
        let status = monitor.check(900, 1000, 500_000, 1_000_000, 50, 100);
        assert!(status.any_exceeded);
        assert!(status.exceeded_regions.contains(&"hash_table".to_string()));
        assert!(!status
            .exceeded_regions
            .contains(&"extent_region".to_string()));
        assert!(!status.exceeded_regions.contains(&"slab_region".to_string()));
    }

    #[test]
    fn check_extent_usage_exceeded() {
        let config = WatermarkConfig::default();
        let monitor = WatermarkMonitor::new(config);

        // 900K / 1M = 90% -> exceeds 85% threshold
        let status = monitor.check(500, 1000, 900_000, 1_000_000, 50, 100);
        assert!(status.any_exceeded);
        assert!(!status.exceeded_regions.contains(&"hash_table".to_string()));
        assert!(status
            .exceeded_regions
            .contains(&"extent_region".to_string()));
        assert!(!status.exceeded_regions.contains(&"slab_region".to_string()));
    }

    #[test]
    fn check_multiple_exceeded() {
        let config = WatermarkConfig::default();
        let monitor = WatermarkMonitor::new(config);

        // All three exceed thresholds
        let status = monitor.check(900, 1000, 900_000, 1_000_000, 90, 100);
        assert!(status.any_exceeded);
        assert!(status.exceeded_regions.contains(&"hash_table".to_string()));
        assert!(status
            .exceeded_regions
            .contains(&"extent_region".to_string()));
        assert!(status.exceeded_regions.contains(&"slab_region".to_string()));
        assert_eq!(status.exceeded_regions.len(), 3);
    }

    #[test]
    fn check_zero_capacity_handles_gracefully() {
        let config = WatermarkConfig::default();
        let monitor = WatermarkMonitor::new(config);

        let status = monitor.check(0, 0, 0, 0, 0, 0);
        assert!(!status.any_exceeded);
        assert_eq!(status.table_load, 0.0);
        assert_eq!(status.extent_usage, 0.0);
        assert_eq!(status.slab_usage, 0.0);
    }

    #[test]
    fn callback_fires_on_exceeded() {
        use std::sync::Mutex;

        let config = WatermarkConfig::default();
        let mut monitor = WatermarkMonitor::new(config);

        let fired = Arc::new(Mutex::new(false));
        let fired_clone = Arc::clone(&fired);

        monitor.on_exceeded(move |_status| {
            *fired_clone.lock().unwrap() = true;
        });

        // This should trigger the callback
        monitor.check(900, 1000, 10, 100, 5, 10);

        assert!(*fired.lock().unwrap());
    }

    #[test]
    fn callback_does_not_fire_when_ok() {
        use std::sync::Mutex;

        let config = WatermarkConfig::default();
        let mut monitor = WatermarkMonitor::new(config);

        let fired = Arc::new(Mutex::new(false));
        let fired_clone = Arc::clone(&fired);

        monitor.on_exceeded(move |_status| {
            *fired_clone.lock().unwrap() = true;
        });

        // Below thresholds - callback should not fire
        monitor.check(100, 1000, 10, 100, 5, 10);

        assert!(!*fired.lock().unwrap());
    }
}
