//! Protection Domain wrapper.
//!
//! A Protection Domain (PD) is a container for RDMA resources — memory
//! regions and queue pairs — that defines access rights and memory
//! protection boundaries.

use ibverbs_sys::*;

use crate::error::RdmaError;
use crate::rdma::context::Context;

/// RDMA Protection Domain.
///
/// Created via [`ProtectionDomain::allocate`] and automatically
/// deallocated on drop via `ibv_dealloc_pd`.
pub struct ProtectionDomain {
    inner: *mut ibv_pd,
}

impl ProtectionDomain {
    /// Allocate a new protection domain for the given device context.
    pub fn allocate(ctx: &Context) -> Result<Self, RdmaError> {
        let pd = unsafe { ibv_alloc_pd(ctx.as_ptr()) };

        if pd.is_null() {
            return Err(RdmaError::HardwareError(
                "ibv_alloc_pd returned NULL".to_string(),
            ));
        }

        Ok(ProtectionDomain { inner: pd })
    }

    /// Get the raw `ibv_pd` pointer for use in FFI calls.
    pub fn as_ptr(&self) -> *mut ibv_pd {
        self.inner
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        let ret = unsafe { ibv_dealloc_pd(self.inner) };
        if ret != 0 {
            // A failed deallocation in drop is a bug, but we cannot panic.
            // Log it at error level.
            tracing::error!(
                "ibv_dealloc_pd failed with return code {}; potential resource leak",
                ret
            );
        }
    }
}

// SAFETY: ibv_pd can be sent and shared across threads.
unsafe impl Send for ProtectionDomain {}
unsafe impl Sync for ProtectionDomain {}
