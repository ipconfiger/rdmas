//! MemoryRegion — safe wrapper over `ibv_mr`.
//!
//! Registers a memory region with RDMA via `ibv_reg_mr` and
//! automatically deregisters on `Drop`. Stores `lkey`/`rkey` for
//! use by the data path.
//!
//! # Note on opaque `ibv_mr`
//!
//! The bindgen-generated `ibv_mr` type is opaque (`_address: u8`).
//! We use thin C wrapper functions (`ibv_mr_lkey`, `ibv_mr_rkey`,
//! `ibv_mr_addr`, `ibv_mr_length`) compiled from `src/wrapper_fns.c`
//! to access the internal fields safely.

use ibverbs_sys::{self, ibv_mr};
use std::ptr::NonNull;

use crate::error::RdmaError;
use crate::rdma::ProtectionDomain;

/// Safe Rust wrapper around RDMA access flags.
///
/// Combines `ibv_access_flags` bitmask values for memory region registration.
pub struct AccessFlags(pub u32);

#[allow(dead_code)]
impl AccessFlags {
    pub const LOCAL_WRITE: u32 = ibverbs_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as u32;
    pub const REMOTE_WRITE: u32 = ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as u32;
    pub const REMOTE_READ: u32 = ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ as u32;
    pub const REMOTE_ATOMIC: u32 = ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC as u32;
    pub const MW_BIND: u32 = ibverbs_sys::ibv_access_flags::IBV_ACCESS_MW_BIND as u32;
    pub const RELAXED_ORDERING: u32 =
        ibverbs_sys::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING as u32;
    pub const NONE: u32 = 0;
}

impl From<u32> for AccessFlags {
    fn from(flags: u32) -> Self {
        AccessFlags(flags)
    }
}

/// RAII wrapper for `ibv_mr`.
///
/// On drop, calls `ibv_dereg_mr`. The `lkey` and `rkey` are cached
/// at construction time so they can be accessed without touching the
/// opaque struct.
pub struct MemoryRegion {
    mr: NonNull<ibv_mr>,
    lkey: u32,
    rkey: u32,
    /// Size of the registered region, in bytes.
    size: usize,
}

// SAFETY: ibv_mr is safe to share across threads (verbs lib is thread-safe),
// and lkey/rkey are immutable read-only values.
unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

impl MemoryRegion {
    /// Register a memory region with the given protection domain.
    ///
    /// `addr` must point to a valid, locked, registered memory region
    /// (e.g., from HugePages mmap). `length` is the region size in bytes.
    /// `access` is a bitmask of `ibv_access_flags` converted to `i32`
    /// (e.g. `IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ`).
    ///
    /// # Errors
    ///
    /// Returns `RdmaError::Internal` if `ibv_reg_mr` returns null.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn register(
        pd: &ProtectionDomain,
        addr: *mut libc::c_void,
        length: usize,
        access: i32,
    ) -> Result<Self, RdmaError> {
        let mr_ptr = unsafe { ibverbs_sys::ibv_reg_mr(pd.as_ptr(), addr, length, access) };

        let mr = NonNull::new(mr_ptr).ok_or_else(|| {
            RdmaError::Internal(format!(
                "ibv_reg_mr failed: addr={:p}, length={}, access=0x{:x}",
                addr, length, access,
            ))
        })?;

        // Read lkey/rkey via thin C accessor wrappers (the struct is opaque
        // in generated bindings).
        let lkey = unsafe { ibverbs_sys::ibv_mr_lkey(mr_ptr) };
        let rkey = unsafe { ibverbs_sys::ibv_mr_rkey(mr_ptr) };

        tracing::debug!(
            ?addr,
            length,
            lkey,
            rkey,
            access,
            "Registered memory region"
        );

        Ok(Self {
            mr,
            lkey,
            rkey,
            size: length,
        })
    }

    /// Get the local access key (lkey).
    pub fn lkey(&self) -> u32 {
        self.lkey
    }

    /// Get the remote access key (rkey).
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// Get the virtual address of the registered buffer (for remote access).
    pub fn addr(&self) -> *mut libc::c_void {
        unsafe { ibverbs_sys::ibv_mr_addr(self.mr.as_ptr()) }
    }

    /// Get the size of the registered memory region in bytes.
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the length of the registered buffer in bytes.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Return the raw `*mut ibv_mr` for FFI calls.
    #[allow(dead_code)]
    pub fn as_ptr(&self) -> *mut ibv_mr {
        self.mr.as_ptr()
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        unsafe {
            let ret = ibverbs_sys::ibv_dereg_mr(self.mr.as_ptr());
            if ret != 0 {
                tracing::error!(
                    mr = ?self.mr,
                    "ibv_dereg_mr failed with error code {}",
                    ret,
                );
            }
        }
    }
}
