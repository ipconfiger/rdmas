//! Completion Queue wrapper.
//!
//! Completion Queues (CQs) collect work completion events from
//! posted send and receive work requests. The poller polls CQs
//! to harvest completed operations.

use std::os::raw::c_void;

use ibverbs_sys::*;

use crate::error::RdmaError;
use crate::rdma::context::Context;

/// RDMA Completion Queue.
///
/// Created via [`CompletionQueue::create`] and automatically destroyed
/// on drop via `ibv_destroy_cq`.
pub struct CompletionQueue {
    inner: *mut ibv_cq,
}

impl CompletionQueue {
    /// Create a new completion queue.
    ///
    /// # Parameters
    ///
    /// * `ctx` - The device context.
    /// * `cqe` - Minimum number of completion queue entries the CQ should hold.
    /// * `context` - User context pointer passed to completion events (can be null).
    /// * `channel` - Completion channel for event-driven notification (can be null).
    /// * `comp_vector` - Completion vector for interrupt steering (typically 0).
    pub fn create(
        ctx: &Context,
        cqe: u32,
        context: *mut c_void,
        channel: *mut ibv_comp_channel,
        comp_vector: u32,
    ) -> Result<Self, RdmaError> {
        let cq = unsafe {
            ibv_create_cq(
                ctx.as_ptr(),
                cqe as libc::c_int,
                context,
                channel,
                comp_vector as libc::c_int,
            )
        };

        if cq.is_null() {
            return Err(RdmaError::HardwareError(
                "ibv_create_cq returned NULL".to_string(),
            ));
        }

        Ok(CompletionQueue { inner: cq })
    }

    /// Poll for work completions.
    ///
    /// Returns a vector of completed work requests. An empty vector
    /// means no completions are available (not an error).
    pub fn poll(&self, num_entries: u32) -> Result<Vec<WorkCompletion>, RdmaError> {
        if num_entries == 0 {
            return Ok(Vec::new());
        }

        let mut wc_array: Vec<ibv_wc> = Vec::with_capacity(num_entries as usize);

        let ret = unsafe {
            ibv_poll_cq_wr(
                self.inner,
                num_entries as libc::c_int,
                wc_array.as_mut_ptr(),
            )
        };

        if ret < 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_poll_cq failed with return code {}",
                ret
            )));
        }

        let count = ret as usize;

        // SAFETY: ibv_poll_cq_wr initialized the first `count` elements.
        unsafe {
            wc_array.set_len(count);
        }

        Ok(wc_array.into_iter().map(|wc| WorkCompletion {
            wr_id: wc.wr_id,
            status: wc.status,
            opcode: wc.opcode,
            byte_len: wc.byte_len,
            vendor_err: wc.vendor_err,
            imm_data: unsafe { wc.__bindgen_anon_1.imm_data },
            qp_num: wc.qp_num,
            src_qp: wc.src_qp,
        }).collect())
    }

    /// Request completion notification on this CQ.
    ///
    /// When `solicited_only` is `true`, only solicited completions
    /// (those with `IBV_SEND_SOLICITED` flag) generate an event.
    pub fn request_notification(&self, solicited_only: bool) -> Result<(), RdmaError> {
        let ret = unsafe {
            ibv_req_notify_cq_wr(self.inner, solicited_only as libc::c_int)
        };

        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_req_notify_cq failed with return code {}",
                ret
            )));
        }

        Ok(())
    }

    /// Get the raw `ibv_cq` pointer for use in FFI calls.
    pub fn as_ptr(&self) -> *mut ibv_cq {
        self.inner
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let ret = unsafe { ibv_destroy_cq(self.inner) };
        if ret != 0 {
            tracing::error!(
                "ibv_destroy_cq failed with return code {}; potential resource leak",
                ret
            );
        }
    }
}

// SAFETY: ibv_cq can be sent and shared across threads.
unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

/// A single work completion entry.
///
/// Returned by [`CompletionQueue::poll`].
#[derive(Debug, Clone)]
pub struct WorkCompletion {
    /// User-assigned work request ID (matches `wr_id` on the posted WR).
    pub wr_id: u64,
    /// Completion status (`IBV_WC_SUCCESS` on success).
    pub status: ibv_wc_status,
    /// The operation that completed.
    pub opcode: ibv_wc_opcode,
    /// Number of bytes transferred.
    pub byte_len: u32,
    /// Vendor-specific error code (meaningful only on error).
    pub vendor_err: u32,
    /// Immediate data (in network byte order), valid for `IBV_WC_RECV_RDMA_WITH_IMM`.
    pub imm_data: u32,
    /// The QP number that generated this completion.
    pub qp_num: u32,
    /// Source QP number (for UD QPs).
    pub src_qp: u32,
}

impl WorkCompletion {
    /// Returns `true` if the operation completed successfully.
    pub fn is_success(&self) -> bool {
        self.status == ibv_wc_status::IBV_WC_SUCCESS
    }
}
