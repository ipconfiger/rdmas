//! Large Object Region and Extent Allocator
//!
//! Design spec: Rust-RDMA.md §二.1, §二.4 — Wave 2 T2-D
//!
//! The Large Object Region stores MB-scale values (e.g., LMCache KV cache tensors)
//! as contiguous extents. Each extent has a 24-byte [`ExtentHeader`] followed by
//! the data payload.
//!
//! # Memory Layout
//!
//! ```text
//! [ExtentHeader | data...][ExtentHeader | data...][...free space...]
//! ```
//!
//! # Free List
//!
//! A lock-free stack of (offset, size) pairs. Freed extents are pushed here;
//! new allocations search the free list before bump-allocating from the tail.
//!
//! For local simulation (Wave 2), the backing store is a `Vec<u8>` and the
//! free list is a `VecDeque<(u64, u64)>`.

use std::collections::{HashSet, VecDeque};

use crate::engine::layout::*;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of an [`ExtentHeader`] in bytes.
pub const HEADER_SIZE: u64 = 24;

/// All extents are aligned to 8 bytes so that [`bytemuck::from_bytes`] works
/// on arbitrary offsets within the buffer.
const EXTENT_ALIGN: u64 = 8;

/// Round `val` up to the nearest multiple of `align`.
#[inline]
const fn align_up(val: u64, align: u64) -> u64 {
    (val + align - 1) & !(align - 1)
}

/// Compute the total footprint of an extent (header + data, 8-byte aligned).
#[inline]
fn extent_total(data_len: u64) -> u64 {
    align_up(HEADER_SIZE + data_len, EXTENT_ALIGN)
}

// ---------------------------------------------------------------------------
// ExtentError
// ---------------------------------------------------------------------------

/// Errors returned by extent allocator operations.
#[derive(Debug, PartialEq, Eq)]
pub enum ExtentError {
    /// The provided offset does not point to a valid extent within the buffer.
    InvalidOffset,
    /// The magic value in the header does not match [`EXTENT_MAGIC`].
    InvalidMagic,
    /// Not enough contiguous space to satisfy the allocation request.
    OutOfSpace,
    /// Attempted to free an offset that is not currently allocated.
    NotAllocated,
}

// ---------------------------------------------------------------------------
// LargeObjectRegion
// ---------------------------------------------------------------------------

/// Manages the Large Object Region: a contiguous byte buffer for storing
/// Extent-allocated large values.
///
/// # Allocation Strategy
///
/// 1. Search the free list for a freed extent ≥ needed size. If a larger
///    extent is found, split it and put the remainder back.
/// 2. If no suitable free extent exists, bump-allocate from `next_offset`.
/// 3. Returns `None` when the region is exhausted.
pub struct LargeObjectRegion {
    /// The backing buffer (simulated — in production, this is HugePage-backed).
    buffer: Vec<u8>,

    /// Free list: deque of `(offset, total_length)` for freed extents.
    free_list: VecDeque<(u64, u64)>,

    /// Set of currently-allocated extent offsets (for sweep iteration).
    allocated: HashSet<u64>,

    /// Next allocation offset when the free list is empty (bump pointer).
    next_offset: u64,

    /// Total capacity of the region in bytes.
    size: u64,
}

impl LargeObjectRegion {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new Large Object Region with the given total capacity.
    ///
    /// The buffer is zero-initialised.  In production this would be backed
    /// by RDMA-registered HugePages.
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0u8; size],
            free_list: VecDeque::new(),
            allocated: HashSet::new(),
            next_offset: 0,
            size: size as u64,
        }
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    /// Allocate an extent holding the given data.
    ///
    /// Returns the byte offset of the extent header within the region,
    /// or `None` if there is not enough contiguous space.
    pub fn allocate(&mut self, data: &[u8]) -> Option<u64> {
        let total_needed = extent_total(data.len() as u64);

        // 1. Search the free list for a suitable freed extent.
        if let Some(idx) = self
            .free_list
            .iter()
            .position(|&(_off, len)| len >= total_needed)
        {
            let (offset, len) = self.free_list.remove(idx).unwrap();

            // Split: put the unused tail back into the free list.
            if len > total_needed {
                self.free_list
                    .push_back((offset + total_needed, len - total_needed));
            }

            self.write_extent(offset, data);
            self.allocated.insert(offset);
            return Some(offset);
        }

        // 2. Bump-allocate from the tail.
        if self.next_offset + total_needed > self.size {
            return None;
        }

        let offset = self.next_offset;
        self.next_offset += total_needed;

        self.write_extent(offset, data);
        self.allocated.insert(offset);
        Some(offset)
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /// Read the data stored in an extent at the given offset.
    ///
    /// Returns `None` if the offset is invalid or the magic check fails.
    pub fn read(&self, offset: u64) -> Option<Vec<u8>> {
        let hdr = self.read_header(offset)?;
        if hdr.magic != EXTENT_MAGIC {
            return None;
        }
        let data_start = (offset as usize) + HEADER_SIZE as usize;
        let data_end = data_start + hdr.length as usize;
        if data_end > self.buffer.len() {
            return None;
        }
        Some(self.buffer[data_start..data_end].to_vec())
    }

    // -----------------------------------------------------------------------
    // Free
    // -----------------------------------------------------------------------

    /// Free a previously-allocated extent.
    ///
    /// The offset is pushed back to the free list so it may be reused by a
    /// future [`allocate`](Self::allocate) call.
    ///
    /// # Errors
    ///
    /// Returns [`ExtentError::NotAllocated`] if the offset is not in the
    /// allocated set, or [`ExtentError::InvalidMagic`] if the magic check
    /// fails.
    pub fn free(&mut self, offset: u64) -> Result<(), ExtentError> {
        if !self.allocated.contains(&offset) {
            return Err(ExtentError::NotAllocated);
        }

        let hdr = self.read_header(offset).ok_or(ExtentError::InvalidOffset)?;
        if hdr.magic != EXTENT_MAGIC {
            return Err(ExtentError::InvalidMagic);
        }

        let total_len = extent_total(hdr.length);
        // Zero the magic to prevent stale reads after freeing.
        self.zero_magic(offset);
        self.free_list.push_back((offset, total_len));
        self.allocated.remove(&offset);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // GC marking
    // -----------------------------------------------------------------------

    /// Mark an extent for garbage collection with the given epoch.
    ///
    /// When the GC thread later calls [`sweep`](Self::sweep) with a
    /// `min_active_epoch` greater than the stored `epoch_mark`, the extent
    /// will be collected.
    ///
    /// # Errors
    ///
    /// Returns [`ExtentError::InvalidOffset`] or [`ExtentError::InvalidMagic`].
    pub fn mark_for_gc(&mut self, offset: u64, epoch: u64) -> Result<(), ExtentError> {
        let hdr = self.read_header_mut(offset).ok_or(ExtentError::InvalidOffset)?;
        if hdr.magic != EXTENT_MAGIC {
            return Err(ExtentError::InvalidMagic);
        }
        hdr.epoch_mark = epoch;
        Ok(())
    }

    /// Sweep all extents whose `epoch_mark` is non-zero and strictly less
    /// than `min_active_epoch`.
    ///
    /// Returns the number of extents freed.
    pub fn sweep(&mut self, min_active_epoch: u64) -> usize {
        let offsets: Vec<u64> = self.allocated.iter().copied().collect();
        let mut count = 0;

        for offset in offsets {
            if let Some(hdr) = self.read_header(offset) {
                // Only sweep extents explicitly marked for GC (epoch > 0).
                if hdr.epoch_mark > 0 && hdr.epoch_mark < min_active_epoch {
                    let total_len = extent_total(hdr.length);
                    // Zero the magic to prevent stale reads after sweeping.
                    self.zero_magic(offset);
                    self.free_list.push_back((offset, total_len));
                    self.allocated.remove(&offset);
                    count += 1;
                }
            }
        }

        count
    }

    // -----------------------------------------------------------------------
    // Statistics
    // -----------------------------------------------------------------------

    /// Total capacity in bytes.
    pub fn capacity(&self) -> u64 {
        self.size
    }

    /// Total bytes currently occupied by live (allocated) extents, including
    /// headers.
    pub fn used_bytes(&self) -> u64 {
        self.allocated
            .iter()
            .filter_map(|&off| {
                self.read_header(off)
                    .map(|h| extent_total(h.length))
            })
            .sum()
    }

    /// Fragmentation ratio.
    ///
    /// - `1.0` means the buffer is fully packed (no free-list holes).
    /// - Values greater than `1.0` indicate wasted space from fragmentation.
    /// - Returns `0.0` when nothing is allocated.
    pub fn fragmentation_ratio(&self) -> f64 {
        let used = self.used_bytes();
        if used == 0 {
            return 0.0;
        }
        self.size as f64 / used as f64
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Write an [`ExtentHeader`] + data at the given offset.
    fn write_extent(&mut self, offset: u64, data: &[u8]) {
        let hdr = ExtentHeader {
            length: data.len() as u64,
            epoch_mark: 0,
            magic: EXTENT_MAGIC,
            _pad: 0,
        };
        let hdr_bytes: &[u8] = bytemuck::bytes_of(&hdr);
        let off = offset as usize;
        self.buffer[off..off + HEADER_SIZE as usize].copy_from_slice(hdr_bytes);
        self.buffer[off + HEADER_SIZE as usize..off + HEADER_SIZE as usize + data.len()]
            .copy_from_slice(data);
    }

    /// Read and interpret the [`ExtentHeader`] at the given offset.
    fn read_header(&self, offset: u64) -> Option<&ExtentHeader> {
        let off = offset as usize;
        if off + HEADER_SIZE as usize > self.buffer.len() {
            return None;
        }
        Some(bytemuck::from_bytes(
            &self.buffer[off..off + HEADER_SIZE as usize],
        ))
    }

    /// Mutably read the [`ExtentHeader`] at the given offset.
    fn read_header_mut(&mut self, offset: u64) -> Option<&mut ExtentHeader> {
        let off = offset as usize;
        if off + HEADER_SIZE as usize > self.buffer.len() {
            return None;
        }
        Some(bytemuck::from_bytes_mut(
            &mut self.buffer[off..off + HEADER_SIZE as usize],
        ))
    }

    /// Zero the magic field at the given offset to invalidate stale reads.
    fn zero_magic(&mut self, offset: u64) {
        let magic_off = offset as usize + 16; // magic is at byte 16 in ExtentHeader
        if magic_off + 4 <= self.buffer.len() {
            self.buffer[magic_off..magic_off + 4].copy_from_slice(&[0u8; 4]);
        }
    }

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Returns the current number of entries in the free list.
    #[cfg(test)]
    fn free_list_len(&self) -> usize {
        self.free_list.len()
    }

    /// Returns the number of currently-allocated extent offsets.
    #[cfg(test)]
    fn allocated_count(&self) -> usize {
        self.allocated.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Basic allocate / read round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_allocate_read_round_trip() {
        let mut region = LargeObjectRegion::new(1024);
        let data = b"Hello, extent!".to_vec();
        let offset = region.allocate(&data).expect("allocation should succeed");

        let read_back = region.read(offset).expect("read should succeed");
        assert_eq!(read_back, data);
        assert_eq!(region.capacity(), 1024);
        assert_eq!(region.used_bytes(), extent_total(data.len() as u64));
        assert_eq!(region.allocated_count(), 1);
    }

    #[test]
    fn test_allocate_multiple_readable() {
        let mut region = LargeObjectRegion::new(4096);
        let d0 = vec![0xAAu8; 100];
        let d1 = vec![0xBBu8; 200];
        let d2 = vec![0xCCu8; 50];

        let off0 = region.allocate(&d0).unwrap();
        let off1 = region.allocate(&d1).unwrap();
        let off2 = region.allocate(&d2).unwrap();

        assert_eq!(region.read(off0).unwrap(), d0);
        assert_eq!(region.read(off1).unwrap(), d1);
        assert_eq!(region.read(off2).unwrap(), d2);
        assert_eq!(region.allocated_count(), 3);
    }

    #[test]
    fn test_allocate_empty_data() {
        let mut region = LargeObjectRegion::new(256);
        let offset = region.allocate(&[]).expect("allocation should succeed");
        let read_back = region.read(offset).expect("read should succeed");
        assert!(read_back.is_empty());
        assert_eq!(region.used_bytes(), extent_total(0)); // header only, aligned
    }

    #[test]
    fn test_allocate_large_data() {
        let size = 65536; // 64 KiB
        let mut region = LargeObjectRegion::new(size);
        let data = vec![0x42u8; size - HEADER_SIZE as usize];
        let offset = region.allocate(&data).expect("allocation should succeed");
        let read_back = region.read(offset).unwrap();
        assert_eq!(read_back, data);
        // Next allocation should fail — buffer is full
        assert!(region.allocate(&[1u8; 1]).is_none());
    }

    // -----------------------------------------------------------------------
    // Out-of-space handling
    // -----------------------------------------------------------------------

    #[test]
    fn test_allocate_out_of_space() {
        let mut region = LargeObjectRegion::new(64);
        // Need 24 (header) + 50 = 74 bytes, but only have 64
        let data = vec![0u8; 50];
        assert!(region.allocate(&data).is_none());
    }

    #[test]
    fn test_allocate_exact_fit() {
        let mut region = LargeObjectRegion::new(HEADER_SIZE as usize + 16);
        let offset = region.allocate(&vec![0u8; 16]).expect("exact fit should succeed");
        assert_eq!(region.read(offset).unwrap().len(), 16);
        // No space left
        assert!(region.allocate(&[0u8]).is_none());
    }

    #[test]
    fn test_allocate_just_one_byte_over() {
        let cap = HEADER_SIZE as usize + 16;
        let mut region = LargeObjectRegion::new(cap);
        let offset = region.allocate(&vec![0u8; 15]).expect("should fit");
        assert_eq!(region.read(offset).unwrap().len(), 15);
        // Remaining: cap - (24+15) = cap - 39. For cap=40, 1 byte free.
        // 1 byte is not enough even for an empty extent (needs 24 bytes).
        assert!(region.allocate(&[]).is_none());
    }

    // -----------------------------------------------------------------------
    // Free + reallocate
    // -----------------------------------------------------------------------

    #[test]
    fn test_free_and_reallocate_same_size() {
        let mut region = LargeObjectRegion::new(4096);
        let data_a = vec![0x11u8; 500];
        let data_b = vec![0x22u8; 500];

        let off_a = region.allocate(&data_a).unwrap();
        let off_b = region.allocate(&data_b).unwrap();

        // Free the first extent
        region.free(off_a).expect("free should succeed");
        assert_eq!(region.free_list_len(), 1);

        // Allocate something of the same size — should reuse the freed slot
        let data_c = vec![0x33u8; 500];
        let off_c = region.allocate(&data_c).unwrap();
        assert_eq!(off_c, off_a, "should reuse exact freed offset");

        // Both extents should still be readable
        assert_eq!(region.read(off_c).unwrap(), data_c);
        assert_eq!(region.read(off_b).unwrap(), data_b);
    }

    #[test]
    fn test_free_and_reallocate_smaller() {
        let mut region = LargeObjectRegion::new(4096);
        let data_big = vec![0xAAu8; 400];
        let off_big = region.allocate(&data_big).unwrap();

        region.free(off_big).unwrap();

        // Allocate something smaller — should reuse and split
        let data_small = vec![0xBBu8; 100];
        let off_small = region.allocate(&data_small).unwrap();
        assert_eq!(off_small, off_big);
        assert_eq!(region.free_list_len(), 1, "remainder should be in free list");

        // The remainder can be used for another allocation
        let data_med = vec![0xCCu8; 200];
        let off_med = region.allocate(&data_med).unwrap();
        assert_eq!(region.read(off_med).unwrap(), data_med);
        assert_eq!(region.read(off_small).unwrap(), data_small);
    }

    #[test]
    fn test_free_and_reallocate_larger_falls_back_to_bump() {
        let mut region = LargeObjectRegion::new(4096);
        let data_s = vec![0x11u8; 100];
        let off = region.allocate(&data_s).unwrap();
        region.free(off).unwrap();

        // Freed slot is too small → bump-allocated
        let data_l = vec![0x22u8; 500];
        let off_l = region.allocate(&data_l).unwrap();
        assert_ne!(off_l, off, "should not reuse too-small slot");
        assert_eq!(region.read(off_l).unwrap(), data_l);
        // The small freed slot is still in the free list
        assert_eq!(region.free_list_len(), 1);
    }

    #[test]
    fn test_free_twice_is_error() {
        let mut region = LargeObjectRegion::new(1024);
        let off = region.allocate(&vec![0u8; 50]).unwrap();
        region.free(off).unwrap();
        let result = region.free(off);
        assert_eq!(result, Err(ExtentError::NotAllocated));
    }

    #[test]
    fn test_free_unallocated_offset() {
        let mut region = LargeObjectRegion::new(1024);
        let result = region.free(0);
        assert_eq!(result, Err(ExtentError::NotAllocated));
    }

    // -----------------------------------------------------------------------
    // Magic verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_invalid_offset() {
        let region = LargeObjectRegion::new(256);
        assert!(region.read(9999).is_none());
        assert!(region.read(256).is_none()); // one past end
    }

    #[test]
    fn test_read_corrupted_magic() {
        let mut region = LargeObjectRegion::new(256);
        let off = region.allocate(b"test").unwrap();

        // Corrupt the magic in the buffer directly
        let hdr_off = off as usize + 16; // magic is at byte 16 of the header
        region.buffer[hdr_off..hdr_off + 4].copy_from_slice(&[0u8; 4]);

        assert!(region.read(off).is_none(), "should reject corrupted magic");
    }

    #[test]
    fn test_mark_for_gc_invalid_offset() {
        let mut region = LargeObjectRegion::new(256);
        assert_eq!(
            region.mark_for_gc(9999, 42),
            Err(ExtentError::InvalidOffset)
        );
    }

    #[test]
    fn test_mark_for_gc_corrupted_magic() {
        let mut region = LargeObjectRegion::new(256);
        let off = region.allocate(b"test").unwrap();
        // Corrupt magic
        region.buffer[off as usize + 16..off as usize + 20].copy_from_slice(&[0; 4]);
        assert_eq!(
            region.mark_for_gc(off, 42),
            Err(ExtentError::InvalidMagic)
        );
    }

    // -----------------------------------------------------------------------
    // GC: mark + sweep
    // -----------------------------------------------------------------------

    #[test]
    fn test_sweep_collects_marked_extent() {
        let mut region = LargeObjectRegion::new(4096);
        let keep = region.allocate(b"keep me").unwrap();
        let toss = region.allocate(b"toss me").unwrap();

        region.mark_for_gc(toss, 100).unwrap();
        region.mark_for_gc(keep, 200).unwrap();

        // Sweep with min_active_epoch=150: toss (epoch 100) < 150, keep (200) >= 150
        let freed = region.sweep(150);
        assert_eq!(freed, 1);
        assert!(region.read(toss).is_none(), "tossed extent magic may still be readable but free-list reuse should reclaim it");
        assert_eq!(region.read(keep).unwrap(), b"keep me");
        assert_eq!(region.free_list_len(), 1);
    }

    #[test]
    fn test_sweep_ignores_unmarked_extents() {
        let mut region = LargeObjectRegion::new(4096);
        let off = region.allocate(b"unmarked").unwrap();

        // epoch_mark is 0 by default; sweep even with min_active_epoch=1
        let freed = region.sweep(1);
        assert_eq!(freed, 0, "unmarked extents (epoch=0) should not be swept");
        assert_eq!(region.read(off).unwrap(), b"unmarked");
    }

    #[test]
    fn test_sweep_zero_threshold_collects_nothing() {
        let mut region = LargeObjectRegion::new(4096);
        let off = region.allocate(b"data").unwrap();

        region.mark_for_gc(off, 1).unwrap();
        // min_active_epoch=1: epoch_mark (1) is not < 1
        let freed = region.sweep(1);
        assert_eq!(freed, 0);
    }

    #[test]
    fn test_sweep_returns_zero_when_nothing_to_collect() {
        let mut region = LargeObjectRegion::new(4096);
        region.allocate(b"a").unwrap();
        region.allocate(b"b").unwrap();
        let freed = region.sweep(100);
        assert_eq!(freed, 0);
    }

    #[test]
    fn test_sweep_multiple_epochs() {
        let mut region = LargeObjectRegion::new(4096);
        let e1 = region.allocate(b"epoch10").unwrap();
        let e2 = region.allocate(b"epoch20").unwrap();
        let e3 = region.allocate(b"epoch30").unwrap();

        region.mark_for_gc(e1, 10).unwrap();
        region.mark_for_gc(e2, 20).unwrap();
        region.mark_for_gc(e3, 30).unwrap();

        // Sweep at threshold=25: only e1 (10) and e2 (20) collected
        let freed = region.sweep(25);
        assert_eq!(freed, 2);
        assert_eq!(region.allocated_count(), 1);
        assert_eq!(region.read(e3).unwrap(), b"epoch30");
    }

    // -----------------------------------------------------------------------
    // Fragmentation ratio
    // -----------------------------------------------------------------------

    #[test]
    fn test_fragmentation_ratio_empty_region() {
        let region = LargeObjectRegion::new(1024);
        assert_eq!(region.fragmentation_ratio(), 0.0);
    }

    #[test]
    fn test_fragmentation_ratio_fully_packed() {
        // Two extents of 8 bytes each: extent_total(8) = align_up(32, 8) = 32.
        // Total = 64.  Capacity = 64 → fully packed.
        let cap = 64;
        let mut region = LargeObjectRegion::new(cap);
        region.allocate(&vec![0u8; 8]).unwrap();
        region.allocate(&vec![0u8; 8]).unwrap();

        let ratio = region.fragmentation_ratio();
        assert!((ratio - 1.0).abs() < 1e-9, "fully packed should be ~1.0, got {ratio}");
    }

    #[test]
    fn test_fragmentation_ratio_with_holes() {
        let mut region = LargeObjectRegion::new(4096);
        let a = region.allocate(&vec![0u8; 200]).unwrap();
        let _b = region.allocate(&vec![0u8; 300]).unwrap();
        region.free(a).unwrap();

        // Used bytes = only b (aligned footprint)
        // Ratio = 4096 / used > 1.0
        let ratio = region.fragmentation_ratio();
        assert!(ratio > 1.0, "should show > 1.0 with free-list holes");
        assert_eq!(region.used_bytes(), extent_total(300));
    }

    #[test]
    fn test_fragmentation_ratio_after_reuse() {
        let mut region = LargeObjectRegion::new(4096);
        let a = region.allocate(&vec![0xAAu8; 100]).unwrap();
        let _b = region.allocate(&vec![0xBBu8; 100]).unwrap();
        region.free(a).unwrap();
        // Reuse the exact slot
        region.allocate(&vec![0xCCu8; 100]).unwrap();
        // Should be fully packed again
        let ratio = region.fragmentation_ratio();
        let expected = region.size as f64 / region.used_bytes() as f64;
        assert!((ratio - expected).abs() < 1e-9);
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_allocated_count_tracks_correctly_through_free_realloc() {
        let mut region = LargeObjectRegion::new(8192);
        assert_eq!(region.allocated_count(), 0);

        let a = region.allocate(b"a").unwrap();
        assert_eq!(region.allocated_count(), 1);

        let b = region.allocate(b"b").unwrap();
        assert_eq!(region.allocated_count(), 2);

        region.free(a).unwrap();
        assert_eq!(region.allocated_count(), 1);

        region.allocate(b"c").unwrap();
        assert_eq!(region.allocated_count(), 2);

        region.free(b).unwrap();
        assert_eq!(region.allocated_count(), 1);
    }

    #[test]
    fn test_capacity_unchanged_after_operations() {
        let mut region = LargeObjectRegion::new(2048);
        assert_eq!(region.capacity(), 2048);

        let off = region.allocate(&vec![0u8; 500]).unwrap();
        assert_eq!(region.capacity(), 2048);

        region.free(off).unwrap();
        assert_eq!(region.capacity(), 2048);
    }

    #[test]
    fn test_sweeped_extent_can_be_reallocated() {
        let mut region = LargeObjectRegion::new(4096);
        let off = region.allocate(b"swept").unwrap();
        region.mark_for_gc(off, 5).unwrap();
        region.sweep(10);

        // The freed offset should now be in the free list and reusable
        let new_off = region.allocate(b"reused").unwrap();
        assert_eq!(new_off, off);
        assert_eq!(region.read(new_off).unwrap(), b"reused");
    }

    #[test]
    fn test_many_allocations_and_frees() {
        let mut region = LargeObjectRegion::new(16384);
        let mut offsets = Vec::new();

        // Allocate 20 extents of varying sizes
        for i in 0..20u64 {
            let data = vec![i as u8; 16 + i as usize * 5];
            let off = region.allocate(&data).unwrap();
            offsets.push((off, data));
        }

        // Read them all back
        for (off, expected) in &offsets {
            assert_eq!(region.read(*off).unwrap(), *expected);
        }

        // Free half of them
        for (off, _) in offsets.iter().take(10) {
            region.free(*off).unwrap();
        }

        // Allocate new ones — should reuse free list
        for i in 0..10u64 {
            let data = vec![(100 + i) as u8; 20];
            let _ = region.allocate(&data).unwrap();
        }

        // Remaining original extents still readable
        for (off, expected) in offsets.iter().skip(10) {
            assert_eq!(region.read(*off).unwrap(), *expected);
        }
    }
}
