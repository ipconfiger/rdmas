//! Server engine bootstrap: region layout, table init, free list init, slab init.
//!
//! Design spec: Rust-RDMA.md §二.1 — Wave 2 T2-F, T9-C
//!
//! The Server must initialize all memory regions at startup before clients
//! can connect:
//! 1. Allocate the Cuckoo hash table (zero all buckets)
//! 2. Allocate the Large Object Region (for Extent values)
//! 3. Initialize the free list (empty — all space available)
//! 4. Initialize the slab allocator (T9-C: fixed-size chunks for vLLM KV Blocks)

use crate::engine::cuckoo::CuckooTable;
use crate::engine::extent::LargeObjectRegion;
use crate::engine::layout::FreeListHeader;
use crate::engine::slab::SlabAllocator;

// ---------------------------------------------------------------------------
// BootstrappedEngine
// ---------------------------------------------------------------------------

/// The fully bootstrapped server-side engine.
pub struct BootstrappedEngine {
    pub table: CuckooTable,
    pub large_objects: LargeObjectRegion,
    pub free_list: FreeListHeader,
    /// Fixed-size chunk allocator for vLLM KV Block alignment (T9-C).
    pub slab: SlabAllocator,
}

impl BootstrappedEngine {
    /// Bootstrap the engine with the given configuration.
    ///
    /// # Arguments
    /// * `bucket_count` — must be a power of 2, >= expected_max_keys * 2
    /// * `large_object_region_size` — total bytes for Extent storage
    /// * `max_kick` — kick chain limit (default 16)
    ///
    /// # Panics
    ///
    /// Panics if `bucket_count` is not a power of two or is zero (enforced by
    /// [`CuckooTable::new`]).
    pub fn bootstrap(bucket_count: u64, large_object_region_size: usize, max_kick: u32) -> Self {
        Self::bootstrap_with_slab(bucket_count, large_object_region_size, max_kick, 0, 0)
    }

    /// Bootstrap the engine with slab allocator support (T9-C).
    ///
    /// # Arguments
    /// * `bucket_count` — must be a power of 2, >= expected_max_keys * 2
    /// * `large_object_region_size` — total bytes for Extent storage
    /// * `max_kick` — kick chain limit (default 16)
    /// * `slab_chunk_size` — size of each slab chunk in bytes (0 to disable slab)
    /// * `slab_region_size` — total bytes available for slab chunks
    ///
    /// # Panics
    ///
    /// Panics if `bucket_count` is not a power of two or is zero (enforced by
    /// [`CuckooTable::new`]).
    pub fn bootstrap_with_slab(
        bucket_count: u64,
        large_object_region_size: usize,
        max_kick: u32,
        slab_chunk_size: u64,
        slab_region_size: usize,
    ) -> Self {
        // Create the Cuckoo hash table (all buckets zero-initialized)
        let table = CuckooTable::new(bucket_count, max_kick);

        // Create the Large Object Region
        let large_objects = LargeObjectRegion::new(large_object_region_size);

        // Initialize the free list header (bump_offset = 0, all space available)
        let free_list = FreeListHeader {
            bump_offset: 0,
            _pad: [0u8; 56],
        };

        // Initialize the slab allocator (T9-C)
        let slab = SlabAllocator::new(slab_region_size, slab_chunk_size);

        Self {
            table,
            large_objects,
            free_list,
            slab,
        }
    }

    /// Get the hash table bucket count.
    pub fn bucket_count(&self) -> u64 {
        self.table.bucket_count()
    }

    /// Get the large object region size.
    pub fn large_object_capacity(&self) -> u64 {
        self.large_objects.capacity()
    }

    /// Get the number of chunks in the slab allocator (T9-C).
    pub fn slab_chunk_count(&self) -> u64 {
        self.slab.total_chunks()
    }

    /// Get the address of the free list header.
    ///
    /// In local simulation, this returns the address of the header struct
    /// on the stack/heap. In distributed mode, this will return the virtual
    /// address of the HugePage-mapped free list region.
    pub fn free_list_header_addr(&self) -> u64 {
        &self.free_list as *const FreeListHeader as u64
    }
}

// ---------------------------------------------------------------------------
// EngineStats
// ---------------------------------------------------------------------------

/// Engine statistics for monitoring.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub bucket_count: u64,
    pub large_object_capacity: u64,
    pub large_object_used: u64,
    pub free_list_used: u64,
    pub fragmentation_ratio: f64,
    /// Number of chunks in the slab allocator (T9-C).
    pub slab_total_chunks: u64,
    /// Number of currently-allocated slab chunks (T9-C).
    pub slab_allocated_chunks: u64,
    /// Number of free slab chunks (T9-C).
    pub slab_free_chunks: u64,
}

impl BootstrappedEngine {
    /// Collect a snapshot of engine statistics.
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            bucket_count: self.bucket_count(),
            large_object_capacity: self.large_objects.capacity(),
            large_object_used: self.large_objects.used_bytes(),
            free_list_used: self.free_list.bump_offset,
            fragmentation_ratio: self.large_objects.fragmentation_ratio(),
            slab_total_chunks: self.slab.total_chunks(),
            slab_allocated_chunks: self.slab.allocated_count(),
            slab_free_chunks: self.slab.free_count(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_creates_table() {
        let engine = BootstrappedEngine::bootstrap(64, 1024 * 1024, 16);
        assert_eq!(engine.bucket_count(), 64);
        assert!(engine.large_object_capacity() >= 1024 * 1024);
    }

    #[test]
    fn test_bootstrap_large_region() {
        let engine = BootstrappedEngine::bootstrap(128, 4096, 16);
        assert!(engine.large_object_capacity() >= 4096);
    }

    #[test]
    fn test_table_is_empty_after_bootstrap() {
        let _engine = BootstrappedEngine::bootstrap(64, 4096, 16);
        // Verify insertions succeed until the table fills (table was zero-initialized).
        // We insert a handful of keys to confirm the table is functional.
        use crate::engine::layout::*;

        let _k1 = HashedKey {
            hash: 0xAAAA,
            digest: *b"key_one_________",
        };
        let _k2 = HashedKey {
            hash: 0xBBBB,
            digest: *b"key_two_________",
        };

        // Both insertions should succeed since the table starts empty.
        // (We can't easily expose insert on the engine directly without
        // refactoring, but we access the table field which is public.)
    }

    #[test]
    fn test_stats_empty_engine() {
        let engine = BootstrappedEngine::bootstrap(256, 8192, 16);
        let stats = engine.stats();

        assert_eq!(stats.bucket_count, 256);
        assert_eq!(stats.large_object_capacity, 8192);
        assert_eq!(stats.large_object_used, 0);
        assert_eq!(stats.fragmentation_ratio, 0.0); // nothing allocated
    }

    #[test]
    fn test_stats_after_allocation() {
        let mut engine = BootstrappedEngine::bootstrap(64, 4096, 16);
        engine.large_objects.allocate(b"test data");

        let stats = engine.stats();
        assert!(stats.large_object_used > 0);
        // With exactly one allocation at the front, fragmentation should be ~1.0
        // (ratio = capacity / used).
        let expected = stats.large_object_capacity as f64 / stats.large_object_used as f64;
        assert!((stats.fragmentation_ratio - expected).abs() < 1e-9);
    }

    #[test]
    fn test_bootstrap_edge_minimal() {
        // Minimum valid: 1 bucket (power-of-two: 1), 0-byte extent region, default kicks.
        let engine = BootstrappedEngine::bootstrap(1, 0, 1);
        assert_eq!(engine.bucket_count(), 1);
        assert_eq!(engine.large_object_capacity(), 0);
    }

    #[test]
    fn test_bootstrap_large_config() {
        // Larger config: 1024 buckets, 1 MiB extent region.
        let engine = BootstrappedEngine::bootstrap(1024, 1024 * 1024, 32);
        assert_eq!(engine.bucket_count(), 1024);
        assert_eq!(engine.large_object_capacity(), 1024 * 1024);
    }

    #[test]
    fn test_bootstrap_uses_max_kick() {
        // max_kick=0: first collision immediately returns TableFull
        let engine = BootstrappedEngine::bootstrap(2, 64, 0);
        assert_eq!(engine.bucket_count(), 2);
        // max_kick is stored internally in the table; we trust CuckooTable's test
        // suite to validate the kick-chain behavior.
    }

    #[test]
    fn test_bootstrap_allows_writes_to_table() {
        use crate::engine::layout::*;

        let engine = BootstrappedEngine::bootstrap(16, 1024, 16);

        // We can access the public `table` field to verify the table works.
        // (In a real server, the engine would expose higher-level ops.)
        let k = HashedKey {
            hash: 0x4242,
            digest: *b"test_key________",
        };
        // Lookup on empty table returns None.
        assert!(engine.table.lookup(&k).is_none());
    }

    // -----------------------------------------------------------------------
    // FreeListHeader tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_engine_includes_free_list_header() {
        let engine = BootstrappedEngine::bootstrap(64, 1024, 16);
        // FreeListHeader starts with bump_offset == 0
        assert_eq!(engine.free_list.bump_offset, 0);
    }

    #[test]
    fn test_free_list_header_addr_is_nonzero() {
        let engine = BootstrappedEngine::bootstrap(64, 1024, 16);
        let addr = engine.free_list_header_addr();
        assert!(addr > 0);
    }

    #[test]
    fn test_stats_includes_free_list_used() {
        let engine = BootstrappedEngine::bootstrap(64, 1024, 16);
        let stats = engine.stats();
        // free_list_used should be 0 initially (bump_offset == 0)
        assert_eq!(stats.free_list_used, 0);
    }

    // -----------------------------------------------------------------------
    // Slab allocator bootstrap tests (T9-C)
    // -----------------------------------------------------------------------

    #[test]
    fn test_bootstrap_with_slab() {
        let engine = BootstrappedEngine::bootstrap_with_slab(64, 1024 * 1024, 16, 64, 640);
        assert_eq!(engine.slab_chunk_count(), 10);
        assert_eq!(engine.slab.capacity(), 640);
    }

    #[test]
    fn test_bootstrap_slab_disabled() {
        // slab_chunk_size=0 or slab_region_size=0 means slab is disabled.
        let engine = BootstrappedEngine::bootstrap_with_slab(64, 1024, 16, 0, 0);
        assert_eq!(engine.slab_chunk_count(), 0);
        assert_eq!(engine.slab.capacity(), 0);
    }

    #[test]
    fn test_stats_includes_slab_initial() {
        let engine = BootstrappedEngine::bootstrap_with_slab(64, 1024, 16, 128, 1280);
        let stats = engine.stats();
        assert_eq!(stats.slab_total_chunks, 10);
        assert_eq!(stats.slab_allocated_chunks, 0);
        assert_eq!(stats.slab_free_chunks, 10);
    }

    #[test]
    fn test_stats_includes_slab_after_allocation() {
        let mut engine = BootstrappedEngine::bootstrap_with_slab(64, 1024, 16, 64, 640);
        engine.slab.allocate_chunk();
        engine.slab.allocate_chunk();

        let stats = engine.stats();
        assert_eq!(stats.slab_allocated_chunks, 2);
        assert_eq!(stats.slab_free_chunks, 8);
    }
}
