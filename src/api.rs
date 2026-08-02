//! Public API trait: stable contract between the engine core and external
//! consumers such as the LMCache connector crate.
//!
//! Design doc §四.1 (KvEngine trait). This trait is stabilized at Gate-3
//! and must not be broken by Wave 4 internal refactoring.

use crate::error::RdmaError;

/// Stable public interface for the RDMA KV engine.
///
/// The LMCache connector (Wave 5) codes against this trait.
/// Implementation details in `engine` and `client` modules may change,
/// but this contract must be preserved across waves.
///
/// # Multi-Tenant Usage
///
/// To isolate keys between tenants, hash keys through `TenantNamespace`:
/// ```
/// use rdmas::control::tenant::TenantNamespace;
/// let tenant = TenantNamespace::new(42);
/// let hashed_key = tenant.hashed_key(b"my_key");
/// // Use hashed_key with standard KvEngine operations
/// ```
pub trait KvEngine: Send + Sync {
    /// Read a single key. Returns `None` if the key does not exist.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RdmaError>;

    /// Write a key-value pair. Overwrites if key exists.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), RdmaError>;

    /// Delete a key. No-op if key does not exist.
    fn delete(&self, key: &[u8]) -> Result<(), RdmaError>;

    /// Check if a key exists without retrieving the value.
    fn exists(&self, key: &[u8]) -> Result<bool, RdmaError>;

    /// Batch read multiple keys.
    fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Vec<u8>>, RdmaError>>;

    /// Batch write multiple key-value pairs.
    fn batch_put(&self, kvs: &[(&[u8], &[u8])]) -> Vec<Result<(), RdmaError>>;

    /// Evict up to `n` least-recently-used entries.
    /// Returns the number of entries actually evicted.
    fn evict(&self, _n: usize) -> Result<usize, RdmaError> {
        Ok(0) // default: no-op for engines without LRU
    }

    /// Get current estimated key count for the engine.
    fn key_count(&self) -> u64 {
        0 // default
    }
}
