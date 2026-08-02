//! region — HugePages-backed memory allocator (T1-B).
//!
//! Provides [`HugePageRegion`], a statically allocated contiguous memory
//! block backed by 2MB HugePages, registered for one-sided RDMA access.
//!
//! # Design
//!
//! - All allocation happens at initialization time via `mmap(…, MAP_HUGETLB)`.
//!   No dynamic `malloc` on the data path.
//! - Pages are pre-faulted (`MAP_POPULATE`) and locked (`mlock`) so that
//!   no page faults occur during RDMA operations.
//! - The region is registered with the InfiniBand HCA via `ibv_reg_mr`
//!   and auto-deregistered on `Drop` (RAII).
//!
//! # Security / System Requirements
//!
//! - The kernel must have HugePages pre-reserved:
//!   ```sh
//!   echo 512 > /proc/sys/vm/nr_hugepages
//!   ```
//! - The process must have `CAP_IPC_LOCK` or an appropriate `memlock` ulimit
//!   for `mlock` to succeed.

use std::ptr;

use crate::error::RdmaError;
use crate::rdma::ProtectionDomain;

/// Size of a single 2MB HugePage.
const HUGE_PAGE_SIZE: usize = 2 * 1024 * 1024;

/// Default RDMA access flags: local write + remote read/write/atomic.
///
/// Composed from `IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
/// IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC` (= 1 | 2 | 4 | 8 = 15).
const DEFAULT_ACCESS: i32 = (ibverbs_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as i32)
    | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as i32)
    | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ as i32)
    | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC as i32);

/// A HugePages-backed memory region registered for one-sided RDMA.
///
/// Allocates a contiguous block via `mmap` with `MAP_HUGETLB`,
/// locks it in physical memory with `mlock`, and registers it
/// with the RDMA HCA for direct remote access.
///
/// # Example (sketch)
///
/// ```ignore
/// let pd = context.alloc_pd()?;
/// let region = HugePageRegion::allocate(128 * 1024 * 1024, &pd)?;
/// // Use region.as_slice() for zero-copy access
/// // Use region.lkey() / region.rkey() for RDMA work requests
/// ```
pub struct HugePageRegion {
    /// Raw pointer to the mmap'd region.
    ptr: *mut u8,
    /// Size of the region (always a multiple of `HUGE_PAGE_SIZE`).
    size: usize,
    /// RDMA memory region handle (registered for remote access).
    /// Dropped **before** `munmap` in the `Drop` impl: the MR must be
    /// deregistered while the memory is still mapped.
    mr: Option<crate::rdma::MemoryRegion>,
}

// SAFETY: The backing memory is not shared without synchronisation;
// each HugePageRegion owns its pointer exclusively.
unsafe impl Send for HugePageRegion {}
unsafe impl Sync for HugePageRegion {}

impl HugePageRegion {
    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Allocate a HugePage-backed region with the default RDMA access flags
    /// (local write + remote read/write/atomic).
    ///
    /// `size` is rounded up to the nearest 2MB boundary.
    pub fn allocate(size: usize, pd: &ProtectionDomain) -> Result<Self, RdmaError> {
        Self::allocate_with_access(size, pd, DEFAULT_ACCESS)
    }

    /// Allocate a HugePage-backed region with custom RDMA access flags.
    ///
    /// `access` is a bitmask of `ibv_access_flags` values (cast to `i32`).
    /// Use this when the region only requires remote read and does not need
    /// remote write or atomic permissions.
    pub fn allocate_with_access(
        size: usize,
        pd: &ProtectionDomain,
        access: i32,
    ) -> Result<Self, RdmaError> {
        let rounded = round_up(size);

        // Step 1: mmap with MAP_HUGETLB
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                rounded,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB | libc::MAP_POPULATE,
                -1, // fd (ignored for MAP_ANONYMOUS)
                0,  // offset
            )
        };

        if ptr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            return Err(RdmaError::Internal(format!(
                "mmap failed for {} bytes (rounded to {}): {}. \
                 Check that HugePages are configured (nr_hugepages > 0) \
                 and the process has sufficient memlock limit.",
                size, rounded, err,
            )));
        }

        let ptr = ptr as *mut u8;

        // Step 2: mlock to prevent swapping (critical for RDMA)
        let mlock_ret = unsafe { libc::mlock(ptr as *const libc::c_void, rounded) };
        if mlock_ret != 0 {
            let err = std::io::Error::last_os_error();
            // Clean up the mmap before returning error.
            unsafe {
                libc::munmap(ptr as *mut libc::c_void, rounded);
            }
            return Err(RdmaError::Internal(format!(
                "mlock failed: {}. Check memlock ulimit (ulimit -l).",
                err,
            )));
        }

        tracing::info!(
            ptr = ?ptr,
            size = rounded,
            actual_requested = size,
            "Allocated HugePage region via mmap"
        );

        // Step 3: Register with RDMA
        let mr =
            crate::rdma::MemoryRegion::register(pd, ptr as *mut libc::c_void, rounded, access)?;

        Ok(Self {
            ptr,
            size: rounded,
            mr: Some(mr),
        })
    }

    /// Virtual address of the region (for remote RDMA addressing).
    pub fn addr(&self) -> u64 {
        self.ptr as u64
    }

    /// Size of the region in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Remote access key (`rkey`) for RDMA operations from a remote QP.
    pub fn rkey(&self) -> Option<u32> {
        self.mr.as_ref().map(|mr| mr.rkey())
    }

    /// Local access key (`lkey`) for local RDMA operations.
    pub fn lkey(&self) -> Option<u32> {
        self.mr.as_ref().map(|mr| mr.lkey())
    }

    /// View the entire region as an immutable byte slice.
    ///
    /// # Safety
    ///
    /// The caller must ensure no concurrent mutable access.
    #[allow(dead_code)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// View the entire region as a mutable byte slice.
    ///
    /// # Safety
    ///
    /// The caller must ensure exclusive access. Concurrent modification
    /// from another thread or via RDMA will cause data races.
    #[allow(dead_code)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

// ---------------------------------------------------------------------
// Drop — CRITICAL: deregister MR *before* munmap
// ---------------------------------------------------------------------

impl Drop for HugePageRegion {
    fn drop(&mut self) {
        // Step 1: Drop the MR first.
        // This calls `ibv_dereg_mr` internally via MemoryRegion::Drop,
        // notifying the HCA that the memory is no longer accessible.
        // Must happen while the pages are still mapped.
        drop(self.mr.take());

        // Step 2: Unmap the HugePages.
        unsafe {
            let ret = libc::munmap(self.ptr as *mut libc::c_void, self.size);
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                // Log but don't panic — the process is likely shutting down.
                tracing::error!(
                    ptr = ?self.ptr,
                    size = self.size,
                    error = %err,
                    "munmap failed in Drop"
                );
            }
        }

        tracing::debug!(
            ptr = ?self.ptr,
            size = self.size,
            "Released HugePage region"
        );
    }
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

/// Round `size` up to the nearest `HUGE_PAGE_SIZE` boundary.
///
/// If `size` is 0, returns `HUGE_PAGE_SIZE` (minimum allocation of
/// one HugePage).
#[inline]
const fn round_up(size: usize) -> usize {
    if size == 0 {
        return HUGE_PAGE_SIZE;
    }
    ((size + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE) * HUGE_PAGE_SIZE
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_up() {
        assert_eq!(round_up(0), HUGE_PAGE_SIZE);
        assert_eq!(round_up(1), HUGE_PAGE_SIZE);
        assert_eq!(round_up(HUGE_PAGE_SIZE - 1), HUGE_PAGE_SIZE);
        assert_eq!(round_up(HUGE_PAGE_SIZE), HUGE_PAGE_SIZE);
        assert_eq!(round_up(HUGE_PAGE_SIZE + 1), 2 * HUGE_PAGE_SIZE);
        assert_eq!(round_up(5 * HUGE_PAGE_SIZE + 123), 6 * HUGE_PAGE_SIZE);
    }
}
