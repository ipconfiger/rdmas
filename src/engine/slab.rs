//! Fixed-size chunk allocator for vLLM KV Block alignment.
//!
//! T9-C: Provides O(1) allocation of fixed-size chunks from a
//! pre-allocated contiguous region. Designed to align with vLLM's
//! KV Block size (16 tokens × hidden_dim × dtype_size).
//!
//! Uses the same CAS bump-allocator protocol as DistributedExtentAllocator
//! for distributed mode, falling back to local bump allocation for simulation.

/// Fixed-size chunk allocator.
pub struct SlabAllocator {
    /// Chunk size in bytes (aligned to 8).
    chunk_size: u64,
    /// Total number of chunks in the region.
    total_chunks: u64,
    /// Current allocation index for local simulation.
    next_chunk: u64,
    /// Free chunk indices (for reclamation).
    free_chunks: Vec<u64>,
    /// Total capacity in bytes.
    capacity: u64,
}

/// Round `val` up to the nearest multiple of `align`.
#[inline]
const fn align_up(val: u64, align: u64) -> u64 {
    (val + align - 1) & !(align - 1)
}

impl SlabAllocator {
    /// Create a new slab allocator.
    ///
    /// `region_size` is the total bytes available for chunks.
    /// `chunk_size` is the size of each chunk (will be 8-byte aligned).
    pub fn new(region_size: usize, chunk_size: u64) -> Self {
        let aligned_chunk = align_up(chunk_size, 8);
        let total_chunks = if aligned_chunk > 0 {
            region_size as u64 / aligned_chunk
        } else {
            0
        };
        let capacity = total_chunks * aligned_chunk;

        Self {
            chunk_size: aligned_chunk,
            total_chunks,
            next_chunk: 0,
            free_chunks: Vec::new(),
            capacity,
        }
    }

    /// Number of chunks that fit in the region.
    pub fn total_chunks(&self) -> u64 {
        self.total_chunks
    }

    /// Allocate a chunk locally. Returns the chunk index (0-based).
    ///
    /// Tries free list first, then bump-allocates.
    pub fn allocate_chunk(&mut self) -> Option<u64> {
        // 1. Try the free list first.
        if let Some(idx) = self.free_chunks.pop() {
            return Some(idx);
        }

        // 2. Bump-allocate.
        if self.next_chunk < self.total_chunks {
            let idx = self.next_chunk;
            self.next_chunk += 1;
            Some(idx)
        } else {
            None
        }
    }

    /// Free a previously allocated chunk.
    pub fn free_chunk(&mut self, chunk_index: u64) {
        self.free_chunks.push(chunk_index);
    }

    /// Number of allocated chunks.
    pub fn allocated_count(&self) -> u64 {
        self.next_chunk - self.free_chunks.len() as u64
    }

    /// Number of free chunks remaining.
    pub fn free_count(&self) -> u64 {
        self.total_chunks - self.allocated_count()
    }

    /// Compute the byte offset of a chunk in the region.
    ///
    /// `offset = chunk_index * chunk_size`
    pub fn chunk_offset(&self, chunk_index: u64) -> u64 {
        chunk_index * self.chunk_size
    }

    /// Capacity in bytes.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

// ---------------------------------------------------------------------------
// DistributedSlabAllocator — CAS bump allocator over RDMA
// ---------------------------------------------------------------------------

/// Distributed-mode slab allocator using CAS bump protocol.
///
/// In distributed mode, SlabAllocator uses the same [`FreeListHeader`] bump_offset
/// CAS protocol as [`DistributedExtentAllocator`](crate::engine::extent::DistributedExtentAllocator).
/// Clients atomically advance a shared `bump_offset` to reserve chunk space
/// without server CPU involvement.
///
/// # Protocol
///
/// 1. RDMA READ the shared `bump_offset`.
/// 2. Compute `new = old + chunk_size`.
/// 3. RDMA CAS `bump_offset` from `old` to `new`.
/// 4. On success: the chunk index is `old / chunk_size`.
/// 5. Write data to `region_base + old`.
///
/// Freed chunks are returned to the server via `SyncFreeList` RPC and
/// cached in `local_free_chunks` for reuse.
#[derive(Debug)]
pub struct DistributedSlabAllocator {
    /// Chunk size in bytes (aligned to 8).
    pub chunk_size: u64,
    /// Total capacity in bytes.
    pub capacity: u64,
    /// Total number of chunks.
    pub total_chunks: u64,
    /// Locally cached freed chunk indices (from SyncFreeList RPC).
    pub local_free_chunks: Vec<u64>,
}

impl DistributedSlabAllocator {
    /// Create a new distributed slab allocator.
    pub fn new(region_size: usize, chunk_size: u64) -> Self {
        let aligned_chunk = align_up(chunk_size, 8);
        let total_chunks = if aligned_chunk > 0 {
            region_size as u64 / aligned_chunk
        } else {
            0
        };
        let capacity = total_chunks * aligned_chunk;

        Self {
            chunk_size: aligned_chunk,
            capacity,
            total_chunks,
            local_free_chunks: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::bootstrap::BootstrappedEngine;

    // -----------------------------------------------------------------------
    // Chunk size alignment
    // -----------------------------------------------------------------------

    #[test]
    fn test_chunk_size_alignment_63_to_64() {
        let slab = SlabAllocator::new(1024, 63);
        // 63 should be aligned up to 64.
        assert_eq!(slab.chunk_offset(1), 64);
    }

    #[test]
    fn test_chunk_size_alignment_65_to_72() {
        let slab = SlabAllocator::new(1024, 65);
        // 65 → 72 (next multiple of 8).
        assert_eq!(slab.chunk_offset(1), 72);
    }

    #[test]
    fn test_chunk_size_already_aligned() {
        let slab = SlabAllocator::new(1024, 64);
        assert_eq!(slab.chunk_offset(1), 64);
    }

    // -----------------------------------------------------------------------
    // Sequential allocation
    // -----------------------------------------------------------------------

    #[test]
    fn test_allocate_returns_sequential_indices() {
        let mut slab = SlabAllocator::new(1024, 64);
        assert_eq!(slab.allocate_chunk(), Some(0));
        assert_eq!(slab.allocate_chunk(), Some(1));
        assert_eq!(slab.allocate_chunk(), Some(2));
        assert_eq!(slab.allocate_chunk(), Some(3));
        assert_eq!(slab.allocated_count(), 4);
    }

    #[test]
    fn test_allocate_all_chunks() {
        let mut slab = SlabAllocator::new(640, 64); // 10 chunks
        let mut indices = Vec::new();
        for _ in 0..10 {
            indices.push(slab.allocate_chunk().unwrap());
        }
        assert_eq!(indices, (0..10).collect::<Vec<_>>());
        assert_eq!(slab.allocated_count(), 10);
        assert_eq!(slab.free_count(), 0);
    }

    // -----------------------------------------------------------------------
    // Free + reallocate reuses freed index
    // -----------------------------------------------------------------------

    #[test]
    fn test_free_and_reallocate_reuses_freed_index() {
        let mut slab = SlabAllocator::new(1024, 64);

        let _a = slab.allocate_chunk().unwrap(); // 0
        let b = slab.allocate_chunk().unwrap(); // 1
        let _c = slab.allocate_chunk().unwrap(); // 2

        slab.free_chunk(b); // free index 1

        // Next allocation should reuse freed index 1.
        let d = slab.allocate_chunk().unwrap();
        assert_eq!(d, 1);
        assert_eq!(slab.allocated_count(), 3);
    }

    #[test]
    fn test_free_multiple_and_reallocate_lifo_order() {
        let mut slab = SlabAllocator::new(1024, 64);

        let _a = slab.allocate_chunk().unwrap(); // 0
        let b = slab.allocate_chunk().unwrap(); // 1
        let c = slab.allocate_chunk().unwrap(); // 2

        slab.free_chunk(c);
        slab.free_chunk(b);

        // LIFO: b was freed last, so it comes back first.
        assert_eq!(slab.allocate_chunk(), Some(1)); // was b
        assert_eq!(slab.allocate_chunk(), Some(2)); // was c
    }

    // -----------------------------------------------------------------------
    // free_count
    // -----------------------------------------------------------------------

    #[test]
    fn test_free_count_decrements_on_allocate() {
        let mut slab = SlabAllocator::new(640, 64); // 10 chunks
        assert_eq!(slab.free_count(), 10);

        slab.allocate_chunk();
        assert_eq!(slab.free_count(), 9);

        slab.allocate_chunk();
        assert_eq!(slab.free_count(), 8);
    }

    #[test]
    fn test_free_count_increments_on_free() {
        let mut slab = SlabAllocator::new(640, 64); // 10 chunks
        let idx = slab.allocate_chunk().unwrap();
        assert_eq!(slab.free_count(), 9);

        slab.free_chunk(idx);
        assert_eq!(slab.free_count(), 10);
    }

    // -----------------------------------------------------------------------
    // Capacity
    // -----------------------------------------------------------------------

    #[test]
    fn test_capacity_matches_total_chunks_times_chunk_size() {
        let slab = SlabAllocator::new(1024, 64);
        // 1024 / 64 = 16 chunks → capacity = 16 * 64 = 1024
        assert_eq!(slab.capacity(), 1024);
        assert_eq!(slab.total_chunks(), 16);
        assert_eq!(slab.capacity(), slab.total_chunks() * 64);
    }

    #[test]
    fn test_capacity_with_unaligned_region_size() {
        let slab = SlabAllocator::new(1000, 64);
        // 1000 / 64 = 15 chunks → capacity = 15 * 64 = 960
        assert_eq!(slab.total_chunks(), 15);
        assert_eq!(slab.capacity(), 960);
    }

    // -----------------------------------------------------------------------
    // OOM returns None
    // -----------------------------------------------------------------------

    #[test]
    fn test_oom_returns_none_when_full() {
        let mut slab = SlabAllocator::new(64, 64); // exactly 1 chunk
        assert!(slab.allocate_chunk().is_some());
        assert!(slab.allocate_chunk().is_none());
        assert_eq!(slab.allocated_count(), 1);
    }

    #[test]
    fn test_oom_after_free_and_reallocate() {
        let mut slab = SlabAllocator::new(128, 64); // 2 chunks
        let a = slab.allocate_chunk().unwrap();
        let b = slab.allocate_chunk().unwrap();

        slab.free_chunk(a);
        slab.free_chunk(b);

        // Both should be reusable
        assert!(slab.allocate_chunk().is_some());
        assert!(slab.allocate_chunk().is_some());
        assert!(slab.allocate_chunk().is_none());
    }

    // -----------------------------------------------------------------------
    // chunk_offset
    // -----------------------------------------------------------------------

    #[test]
    fn test_chunk_offset_calculation() {
        let slab = SlabAllocator::new(1024, 64);
        assert_eq!(slab.chunk_offset(0), 0);
        assert_eq!(slab.chunk_offset(1), 64);
        assert_eq!(slab.chunk_offset(5), 320);
        assert_eq!(slab.chunk_offset(15), 960);
    }

    // -----------------------------------------------------------------------
    // Bootstrap engine includes slab
    // -----------------------------------------------------------------------

    #[test]
    fn test_bootstrap_engine_includes_slab() {
        let engine =
            BootstrappedEngine::bootstrap_with_slab(64, 1024 * 1024, 16, 65536, 65536 * 100);

        assert_eq!(engine.slab_chunk_count(), 100);
        assert_eq!(engine.bucket_count(), 64);
    }

    #[test]
    fn test_engine_stats_includes_slab() {
        let engine = BootstrappedEngine::bootstrap_with_slab(64, 1024, 16, 64, 640);

        let stats = engine.stats();
        assert_eq!(stats.slab_total_chunks, 10);
        // No allocations yet.
        assert_eq!(stats.slab_allocated_chunks, 0);
        assert_eq!(stats.slab_free_chunks, 10);
    }

    // -----------------------------------------------------------------------
    // Zero-size region
    // -----------------------------------------------------------------------

    #[test]
    fn test_zero_size_region() {
        let mut slab = SlabAllocator::new(0, 64);
        assert_eq!(slab.total_chunks(), 0);
        assert_eq!(slab.capacity(), 0);
        assert!(slab.allocate_chunk().is_none());
    }

    // -----------------------------------------------------------------------
    // Chunk size larger than region
    // -----------------------------------------------------------------------

    #[test]
    fn test_chunk_larger_than_region() {
        let mut slab = SlabAllocator::new(50, 64);
        assert_eq!(slab.total_chunks(), 0);
        assert_eq!(slab.capacity(), 0);
        assert!(slab.allocate_chunk().is_none());
    }

    // -----------------------------------------------------------------------
    // DistributedSlabAllocator tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_distributed_slab_creation() {
        let ds = DistributedSlabAllocator::new(1024 * 64, 64);
        assert_eq!(ds.total_chunks, 1024);
        assert_eq!(ds.capacity, 1024 * 64);
        assert_eq!(ds.chunk_size, 64);
        assert!(ds.local_free_chunks.is_empty());
    }

    #[test]
    fn test_distributed_slab_with_alignment() {
        let ds = DistributedSlabAllocator::new(1024 * 64, 63);
        assert_eq!(ds.chunk_size, 64); // aligned up from 63
        assert_eq!(ds.total_chunks, 1024);
    }
}
