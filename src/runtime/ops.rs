//! Async RDMA operation primitives.
//!
//! Provides an async-safe [`RdmaRuntime`] that wraps a [`QueuePair`]
//! and [`CompletionQueue`], bridging the busy-poll completion thread
//! with Tokio futures via [`tokio::sync::oneshot`] channels.
//!
//! # Usage
//!
//! 1. Create a `RdmaRuntime` with a QP, CQ, and the pending map from
//!    [`Poller::spawn`](super::poller::Poller::spawn).
//! 2. Call `runtime.rdma_read(...).await` etc. from async contexts.
//! 3. The poller thread will harvest the completion, fire the oneshot,
//!    and the future will resolve.
//!
//! # Buffer Lifetime Safety
//!
//! All async methods take buffer references (`&[u8]` or `&mut [u8]`).
//! Because we `.await` the completion before returning, the borrow
//! outlives the RDMA operation. The buffer is guaranteed valid until
//! the HCA finishes DMA access.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::error::RdmaError;
use crate::rdma::cq::WorkCompletion;
use crate::rdma::qp::{ScatterGatherEntry, SendWorkRequest, SendWrOpcode};
use crate::rdma::{CompletionQueue, QueuePair};

use super::poller::PendingMap;

/// The async RDMA runtime.
///
/// Wraps a [`QueuePair`] and [`CompletionQueue`] and provides
/// async versions of one-sided RDMA operations by bridging the
/// busy-poll completion thread with Tokio futures.
pub struct RdmaRuntime {
    qp: Arc<QueuePair>,
    #[allow(dead_code)]
    cq: Arc<CompletionQueue>,
    /// Shared map of pending completions (wr_id → oneshot sender).
    pending: PendingMap,
    /// Monotonically increasing work request ID counter.
    next_wr_id: AtomicU64,
}

impl RdmaRuntime {
    /// Create a new async runtime wrapping the given QP and CQ.
    ///
    /// # Parameters
    ///
    /// * `qp` — The queue pair to post work requests to.
    /// * `cq` — The completion queue for harvesting completions.
    /// * `pending` — The pending map returned by [`Poller::spawn`](super::poller::Poller::spawn).
    pub fn new(
        qp: Arc<QueuePair>,
        cq: Arc<CompletionQueue>,
        pending: PendingMap,
    ) -> Self {
        Self {
            qp,
            cq,
            pending,
            next_wr_id: AtomicU64::new(1),
        }
    }

    /// Perform an async one-sided RDMA READ from remote memory.
    ///
    /// Reads `buf.len()` bytes from `remote_addr` on the remote peer
    /// into `buf`. The remote peer must have registered the memory at
    /// `remote_addr` with the given `remote_rkey` and `IBV_ACCESS_REMOTE_READ`.
    ///
    /// # Returns
    ///
    /// The number of bytes successfully read (always `buf.len()` on success).
    pub async fn rdma_read(
        &self,
        buf: &mut [u8],
        mr_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<u32, RdmaError> {
        let sge = ScatterGatherEntry {
            addr: buf.as_mut_ptr() as *mut std::os::raw::c_void,
            length: buf.len() as u32,
            lkey: mr_lkey,
        };

        let wr_id = self.next_wr_id();
        let wr = SendWorkRequest {
            wr_id,
            opcode: SendWrOpcode::RdmaRead,
            send_flags: 0,
            sge: vec![sge],
            remote_addr: Some(remote_addr),
            remote_rkey: Some(remote_rkey),
            compare_add: None,
            swap: None,
        };

        let wc = self.post_and_wait(wr).await?;
        Ok(wc.byte_len)
    }

    /// Perform an async one-sided RDMA WRITE to remote memory.
    ///
    /// Writes the contents of `buf` to `remote_addr` on the remote peer.
    /// The remote peer must have registered the memory at `remote_addr`
    /// with the given `remote_rkey` and `IBV_ACCESS_REMOTE_WRITE`.
    pub async fn rdma_write(
        &self,
        buf: &[u8],
        mr_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<(), RdmaError> {
        let sge = ScatterGatherEntry {
            addr: buf.as_ptr() as *mut std::os::raw::c_void,
            length: buf.len() as u32,
            lkey: mr_lkey,
        };

        let wr_id = self.next_wr_id();
        let wr = SendWorkRequest {
            wr_id,
            opcode: SendWrOpcode::RdmaWrite,
            send_flags: 0,
            sge: vec![sge],
            remote_addr: Some(remote_addr),
            remote_rkey: Some(remote_rkey),
            compare_add: None,
            swap: None,
        };

        self.post_and_wait(wr).await?;
        Ok(())
    }

    /// Perform an async one-sided RDMA CAS (Compare-And-Swap) on remote memory.
    ///
    /// Atomically reads the 8-byte value at `remote_addr`, compares it with
    /// `compare`, and if equal writes `swap`. The original value is read into
    /// `result_buf` (must be exactly 8 bytes).
    ///
    /// The remote peer must have registered the memory at `remote_addr` with
    /// the given `remote_rkey` and `IBV_ACCESS_REMOTE_ATOMIC`.
    ///
    /// # Returns
    ///
    /// `true` if the value was swapped (`compare` matched the current value),
    /// `false` if the value was NOT swapped (CAS failed, original value is
    /// in `result_buf`).
    ///
    /// # Panics
    ///
    /// Panics if `result_buf.len() != 8` (debug builds only).
    pub async fn rdma_cas(
        &self,
        result_buf: &mut [u8],
        compare: u64,
        swap: u64,
        mr_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<bool, RdmaError> {
        debug_assert_eq!(
            result_buf.len(),
            8,
            "CAS result buffer must be exactly 8 bytes"
        );

        let sge = ScatterGatherEntry {
            addr: result_buf.as_mut_ptr() as *mut std::os::raw::c_void,
            length: 8,
            lkey: mr_lkey,
        };

        let wr_id = self.next_wr_id();
        let wr = SendWorkRequest {
            wr_id,
            opcode: SendWrOpcode::RdmaCompareSwap,
            send_flags: 0,
            sge: vec![sge],
            remote_addr: Some(remote_addr),
            remote_rkey: Some(remote_rkey),
            compare_add: Some(compare),
            swap: Some(swap),
        };

        let wc = self.post_and_wait(wr).await?;

        // For CAS, IBV_WC_SUCCESS means the swap happened.
        // The original value is written to the local buffer by the HCA.
        Ok(wc.is_success())
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Allocate the next `wr_id` from the monotonically increasing counter.
    fn next_wr_id(&self) -> u64 {
        self.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Post a work request and register a oneshot waiter for its completion.
    ///
    /// This is the core bridge between the async world and the busy-poll
    /// completion thread:
    ///
    /// 1. Create a `oneshot` channel.
    /// 2. Insert the sender into `self.pending` keyed by `wr.wr_id`.
    /// 3. Post the WR to the QP.
    /// 4. If post fails, remove the pending entry and return the error.
    /// 5. Await the oneshot receiver — the poller thread will send the
    ///    completion when it arrives.
    async fn post_and_wait(
        &self,
        mut wr: SendWorkRequest,
    ) -> Result<WorkCompletion, RdmaError> {
        let wr_id = wr.wr_id;

        // Step 1: Create a oneshot channel
        let (tx, rx) = oneshot::channel();

        // Step 2: Register the sender in the pending map
        {
            let mut map = self
                .pending
                .lock()
                .map_err(|_| RdmaError::Internal("Pending map mutex poisoned".to_string()))?;
            map.insert(wr_id, tx);
        }

        // Step 3: Post the WR to the QP
        let posted_wr_id = self.qp.post_send(&mut wr).map_err(|e| {
            // Clean up: remove the pending entry on failure
            let mut map = match self.pending.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            map.remove(&wr_id);
            e
        })?;

        debug_assert_eq!(posted_wr_id, wr_id, "WR ID mismatch after post_send");

        // Step 4: Await the completion
        rx.await.map_err(|_| {
            // The sender was dropped without sending — this means the poller
            // removed our entry but didn't deliver a result. This is a bug.
            RdmaError::Internal(format!(
                "Oneshot channel dropped for wr_id={}: poller may have panicked",
                wr_id
            ))
        })?
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    #[test]
    fn test_atomic_wr_id_monotonic() {
        let counter = AtomicU64::new(1);

        let id1 = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id2 = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id3 = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        assert!(id1 < id2);
        assert!(id2 < id3);
    }
}
