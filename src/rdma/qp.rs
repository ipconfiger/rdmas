//! Queue Pair wrapper with state machine.
//!
//! Queue Pairs (QPs) are the core communication primitive in RDMA.
//! Each QP has a Send Queue (SQ) and a Receive Queue (RQ). This module
//! enforces the correct QP state machine transitions:
//! RESET → INIT → RTR → RTS.
//!
//! Only Reliable Connection (RC) QPs are currently supported.

use std::os::raw::c_void;
use std::ptr;

use ibverbs_sys::*;

use crate::error::RdmaError;
use crate::rdma::cq::CompletionQueue;
use crate::rdma::pd::ProtectionDomain;

/// RDMA Queue Pair.
///
/// Created via [`QueuePair::create`] and automatically destroyed
/// on drop via `ibv_destroy_qp`.
pub struct QueuePair {
    inner: *mut ibv_qp,
    /// Cached QP number for use in state transitions.
    qp_num: u32,
}

impl QueuePair {
    /// Create a new Queue Pair in RESET state.
    ///
    /// After creation, the QP must be transitioned through INIT → RTR → RTS
    /// before any data operations can be posted.
    ///
    /// # Parameters
    ///
    /// * `pd` - Protection domain for this QP.
    /// * `send_cq` - Completion queue for send completions.
    /// * `recv_cq` - Completion queue for receive completions.
    /// * `max_send_wr` - Maximum number of outstanding send work requests.
    /// * `max_recv_wr` - Maximum number of outstanding receive work requests.
    /// * `max_send_sge` - Maximum scatter/gather entries per send WR.
    /// * `max_recv_sge` - Maximum scatter/gather entries per receive WR.
    /// * `qp_type` - QP transport type. Use [`ibv_qp_type::IBV_QPT_RC`] for one-sided RDMA.
    pub fn create(
        pd: &ProtectionDomain,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
        max_send_wr: u32,
        max_recv_wr: u32,
        max_send_sge: u32,
        max_recv_sge: u32,
        qp_type: ibv_qp_type,
    ) -> Result<Self, RdmaError> {
        let mut init_attr = ibv_qp_init_attr {
            qp_context: ptr::null_mut(),
            send_cq: send_cq.as_ptr(),
            recv_cq: recv_cq.as_ptr(),
            srq: ptr::null_mut(),
            cap: ibv_qp_cap {
                max_send_wr,
                max_recv_wr,
                max_send_sge,
                max_recv_sge,
                max_inline_data: 0,
            },
            qp_type,
            sq_sig_all: 0, // We'll use IBV_SEND_SIGNALED per-WR
        };

        let qp = unsafe { ibv_create_qp(pd.as_ptr(), &mut init_attr) };

        if qp.is_null() {
            return Err(RdmaError::HardwareError(
                "ibv_create_qp returned NULL".to_string(),
            ));
        }

        // Query the QP number via our C accessor (ibv_qp is opaque in bindings).
        let qp_num = unsafe { ibv_qp_get_qp_num(qp) };

        Ok(QueuePair { inner: qp, qp_num })
    }

    /// Get the local QP number.
    ///
    /// This number must be shared with the remote peer before
    /// transitioning to RTR.
    pub fn qp_num(&self) -> u32 {
        self.qp_num
    }

    /// Transition the QP to INIT state.
    ///
    /// This is the first transition after creation (RESET → INIT).
    /// The QP is initialized with access to the specified port and
    /// the configured access flags.
    pub fn init(&mut self, port_num: u8, access_flags: u32) -> Result<(), RdmaError> {
        // Use MaybeUninit because ibv_qp_attr contains unions (ibv_gid)
        // that cannot be zero-initialized with mem::zeroed()
        let mut attr = unsafe { std::mem::MaybeUninit::<ibv_qp_attr>::zeroed().assume_init() };

        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num = port_num;
        attr.qp_access_flags = access_flags;

        let attr_mask = (ibv_qp_attr_mask::IBV_QP_STATE as u32
            | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX as u32
            | ibv_qp_attr_mask::IBV_QP_PORT as u32
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS as u32) as libc::c_int;

        let ret = unsafe { ibv_modify_qp(self.inner, &mut attr, attr_mask) };
        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_modify_qp (INIT) failed with return code {}",
                ret
            )));
        }

        // qp_num is already cached from creation; INIT does not change it.
        Ok(())
    }

    /// Transition the QP to RTR (Ready to Receive) state.
    ///
    /// # Parameters
    ///
    /// * `remote_qpn` - The remote peer's QP number.
    /// * `remote_lid` - The remote peer's local identifier (LID). Use port LID.
    /// * `remote_gid` - Optional GID for RoCE connections (required for Ethernet).
    /// * `port_num` - Local port number.
    /// * `rq_psn` - Starting receive packet sequence number.
    pub fn ready_to_receive(
        &self,
        remote_qpn: u32,
        remote_lid: u16,
        remote_gid: Option<ibv_gid>,
        port_num: u8,
        rq_psn: u32,
    ) -> Result<(), RdmaError> {
        let mut attr = unsafe { std::mem::MaybeUninit::<ibv_qp_attr>::zeroed().assume_init() };

        attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = ibv_mtu::IBV_MTU_1024;
        attr.dest_qp_num = remote_qpn;
        attr.rq_psn = rq_psn;

        // Set the address handle
        attr.ah_attr.dlid = remote_lid;
        attr.ah_attr.port_num = port_num;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.static_rate = 0;

        if let Some(gid) = remote_gid {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid = gid;
            attr.ah_attr.grh.sgid_index = 1; // Default: use GID index 1 for RoCEv2
            attr.ah_attr.grh.hop_limit = 64;
            attr.ah_attr.grh.traffic_class = 0;
            attr.ah_attr.grh.flow_label = 0;
        } else {
            attr.ah_attr.is_global = 0;
        }

        // RDMA atomic settings
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 12; // ~1 second

        let attr_mask = (ibv_qp_attr_mask::IBV_QP_STATE as u32
            | ibv_qp_attr_mask::IBV_QP_AV as u32
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU as u32
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN as u32
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN as u32
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC as u32
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER as u32) as libc::c_int;

        let ret = unsafe { ibv_modify_qp(self.inner, &mut attr, attr_mask) };
        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_modify_qp (RTR) failed with return code {}",
                ret
            )));
        }

        Ok(())
    }

    /// Transition the QP to RTS (Ready to Send) state.
    ///
    /// After this transition, the QP is fully operational and can
    /// post send and receive work requests.
    ///
    /// # Parameters
    ///
    /// * `sq_psn` - Starting send packet sequence number.
    pub fn ready_to_send(&self, sq_psn: u32) -> Result<(), RdmaError> {
        let mut attr = unsafe { std::mem::MaybeUninit::<ibv_qp_attr>::zeroed().assume_init() };

        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.sq_psn = sq_psn;

        // Timeout and retry settings
        attr.timeout = 14; // ~1 second (4.096us * 2^14)
        attr.retry_cnt = 7; // Retry 7 times
        attr.rnr_retry = 7; // RNR retry 7 times
        attr.max_rd_atomic = 16;

        let attr_mask = (ibv_qp_attr_mask::IBV_QP_STATE as u32
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN as u32
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT as u32
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT as u32
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY as u32
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC as u32)
            as libc::c_int;

        let ret = unsafe { ibv_modify_qp(self.inner, &mut attr, attr_mask) };
        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_modify_qp (RTS) failed with return code {}",
                ret
            )));
        }

        Ok(())
    }

    /// Post a send work request to the send queue.
    ///
    /// For one-sided RDMA operations (READ, WRITE, CAS), use the appropriate
    /// `SendWorkRequest` variant.
    ///
    /// Returns the `wr_id` of the posted WR on success, which can be matched
    /// against the `wr_id` in the [`WorkCompletion`] from [`CompletionQueue::poll`].
    pub fn post_send(&self, wr: &mut SendWorkRequest) -> Result<u64, RdmaError> {
        let mut sge_entries: Vec<ibv_sge> = wr
            .sge
            .iter()
            .map(|sge| ibv_sge {
                addr: sge.addr as u64,
                length: sge.length,
                lkey: sge.lkey,
            })
            .collect();

        let wr_id = wr.wr_id;

        // Default send flags: IBV_SEND_SIGNALED to get completions
        let send_flags = wr.send_flags | ibv_send_flags::IBV_SEND_SIGNALED as u32;

        let mut send_wr = ibv_send_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: if sge_entries.is_empty() {
                ptr::null_mut()
            } else {
                sge_entries.as_mut_ptr()
            },
            num_sge: sge_entries.len() as libc::c_int,
            opcode: map_send_opcode(&wr.opcode),
            send_flags,
            __bindgen_anon_1: unsafe { std::mem::zeroed() },
            wr: unsafe { std::mem::zeroed() },
            qp_type: unsafe { std::mem::zeroed() },
            __bindgen_anon_2: unsafe { std::mem::zeroed() },
        };

        // Set operation-specific fields in the union
        match &wr.opcode {
            SendWrOpcode::RdmaRead | SendWrOpcode::RdmaWrite => {
                send_wr.wr.rdma.remote_addr = wr.remote_addr.unwrap_or(0);
                send_wr.wr.rdma.rkey = wr.remote_rkey.unwrap_or(0);
            }
            SendWrOpcode::RdmaCompareSwap => {
                send_wr.wr.atomic.remote_addr = wr.remote_addr.unwrap_or(0);
                send_wr.wr.atomic.compare_add = wr.compare_add.unwrap_or(0);
                send_wr.wr.atomic.swap = wr.swap.unwrap_or(0);
                send_wr.wr.atomic.rkey = wr.remote_rkey.unwrap_or(0);
            }
            SendWrOpcode::Send => {
                // No additional fields needed for plain send
            }
        }

        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();

        let ret = unsafe { ibv_post_send_wr(self.inner, &mut send_wr, &mut bad_wr) };

        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_post_send failed with return code {}",
                ret
            )));
        }

        Ok(wr_id)
    }

    /// Post a batch of send work requests as a linked chain.
    ///
    /// RDMA allows chaining multiple WRs via the `next` pointer in `ibv_send_wr`.
    /// All WRs in the chain are submitted with a single `ibv_post_send` call (one
    /// doorbell ring). Only the LAST WR in the chain gets `IBV_SEND_SIGNALED` —
    /// a single CQ completion is generated for the entire batch.
    ///
    /// # Returns
    /// The `wr_id` of the LAST WR in the chain (single completion notification).
    pub fn post_send_batch(&self, wrs: &mut [SendWorkRequest]) -> Result<u64, RdmaError> {
        if wrs.is_empty() {
            return Err(RdmaError::Internal("empty batch".into()));
        }

        // Collect all SGEs — they must outlive the ibv_sge and ibv_send_wr arrays
        let mut all_sges: Vec<Vec<ibv_sge>> = Vec::with_capacity(wrs.len());
        let mut send_wrs: Vec<ibv_send_wr> = Vec::with_capacity(wrs.len());

        for wr in wrs.iter() {
            let sge_entries: Vec<ibv_sge> = wr
                .sge
                .iter()
                .map(|sge| ibv_sge {
                    addr: sge.addr as u64,
                    length: sge.length,
                    lkey: sge.lkey,
                })
                .collect();
            all_sges.push(sge_entries);
        }

        // Build the WR chain
        for (i, wr) in wrs.iter().enumerate() {
            let is_last = i == wrs.len() - 1;
            let send_flags = if is_last {
                wr.send_flags | ibv_send_flags::IBV_SEND_SIGNALED as u32
            } else {
                wr.send_flags & !(ibv_send_flags::IBV_SEND_SIGNALED as u32)
            };

            let sges = &all_sges[i];
            let mut swr = ibv_send_wr {
                wr_id: wr.wr_id,
                next: ptr::null_mut(), // Linked below
                sg_list: if sges.is_empty() {
                    ptr::null_mut()
                } else {
                    sges.as_ptr() as *mut _
                },
                num_sge: sges.len() as libc::c_int,
                opcode: map_send_opcode(&wr.opcode),
                send_flags,
                __bindgen_anon_1: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
                wr: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
                qp_type: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
                __bindgen_anon_2: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
            };

            // Set op-specific fields
            match &wr.opcode {
                SendWrOpcode::RdmaRead | SendWrOpcode::RdmaWrite => {
                    swr.wr.rdma.remote_addr = wr.remote_addr.unwrap_or(0);
                    swr.wr.rdma.rkey = wr.remote_rkey.unwrap_or(0);
                }
                SendWrOpcode::RdmaCompareSwap => {
                    swr.wr.atomic.remote_addr = wr.remote_addr.unwrap_or(0);
                    swr.wr.atomic.compare_add = wr.compare_add.unwrap_or(0);
                    swr.wr.atomic.swap = wr.swap.unwrap_or(0);
                    swr.wr.atomic.rkey = wr.remote_rkey.unwrap_or(0);
                }
                SendWrOpcode::Send => {}
            }

            send_wrs.push(swr);
        }

        // Link the chain: each WR's next points to the next one
        for i in 0..send_wrs.len() - 1 {
            send_wrs[i].next = &mut send_wrs[i + 1] as *mut ibv_send_wr;
        }

        let last_id = wrs.last().unwrap().wr_id;
        let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();

        let ret = unsafe { ibv_post_send_wr(self.inner, &mut send_wrs[0], &mut bad_wr) };

        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_post_send (batch) failed: ret={}",
                ret
            )));
        }

        Ok(last_id)
    }

    /// Post a receive work request to the receive queue.
    ///
    /// Receive WRs are needed for two-sided operations. For one-sided RDMA
    /// (READ, WRITE, CAS), no receive WRs are needed on the target side.
    ///
    /// Returns the `wr_id` of the posted WR on success.
    pub fn post_recv(&self, wr: &mut RecvWorkRequest) -> Result<u64, RdmaError> {
        let mut sge_entries: Vec<ibv_sge> = wr
            .sge
            .iter()
            .map(|sge| ibv_sge {
                addr: sge.addr as u64,
                length: sge.length,
                lkey: sge.lkey,
            })
            .collect();

        let wr_id = wr.wr_id;

        let mut recv_wr = ibv_recv_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: if sge_entries.is_empty() {
                ptr::null_mut()
            } else {
                sge_entries.as_mut_ptr()
            },
            num_sge: sge_entries.len() as libc::c_int,
        };

        let mut bad_wr: *mut ibv_recv_wr = ptr::null_mut();

        let ret = unsafe { ibv_post_recv_wr(self.inner, &mut recv_wr, &mut bad_wr) };

        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_post_recv failed with return code {}",
                ret
            )));
        }

        Ok(wr_id)
    }

    /// Get the raw `ibv_qp` pointer for use in FFI calls.
    pub fn as_ptr(&self) -> *mut ibv_qp {
        self.inner
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        let ret = unsafe { ibv_destroy_qp(self.inner) };
        if ret != 0 {
            tracing::error!(
                "ibv_destroy_qp failed with return code {}; potential resource leak",
                ret
            );
        }
    }
}

// SAFETY: ibv_qp can be sent across threads. However, concurrent
// post_send/post_recv calls to the same QP must be externally synchronized.
unsafe impl Send for QueuePair {}
unsafe impl Sync for QueuePair {}

// ---------------------------------------------------------------------------
// Public types for work requests
// ---------------------------------------------------------------------------

/// An RDMA send work request posted to a QP's send queue.
pub struct SendWorkRequest {
    /// User-assigned ID, echoed back in the completion.
    pub wr_id: u64,
    /// Operation type: RDMA READ, WRITE, CAS, or SEND.
    pub opcode: SendWrOpcode,
    /// Send flags (will be OR'd with `IBV_SEND_SIGNALED`).
    pub send_flags: u32,
    /// Scatter/gather entries describing the local buffer(s).
    pub sge: Vec<ScatterGatherEntry>,
    /// Remote memory address (for RDMA READ, WRITE, CAS).
    pub remote_addr: Option<u64>,
    /// Remote memory rkey (for RDMA READ, WRITE, CAS).
    pub remote_rkey: Option<u32>,
    /// Compare value for CAS (for RDMA CAS).
    pub compare_add: Option<u64>,
    /// Swap value for CAS (for RDMA CAS).
    pub swap: Option<u64>,
}

/// An RDMA receive work request posted to a QP's receive queue.
pub struct RecvWorkRequest {
    /// User-assigned ID, echoed back in the completion.
    pub wr_id: u64,
    /// Scatter/gather entries describing where to place received data.
    pub sge: Vec<ScatterGatherEntry>,
}

/// A scatter/gather entry describing a local memory segment.
#[derive(Debug, Clone)]
pub struct ScatterGatherEntry {
    /// Virtual address of the local buffer.
    pub addr: *mut c_void,
    /// Length of the buffer segment in bytes.
    pub length: u32,
    /// Local memory key for this segment.
    pub lkey: u32,
}

/// Send work request opcode for one-sided and two-sided operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendWrOpcode {
    /// One-sided RDMA read from remote memory.
    RdmaRead,
    /// One-sided RDMA write to remote memory.
    RdmaWrite,
    /// One-sided atomic compare-and-swap on remote memory.
    RdmaCompareSwap,
    /// Two-sided send.
    Send,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn map_send_opcode(opcode: &SendWrOpcode) -> ibv_wr_opcode {
    match opcode {
        SendWrOpcode::RdmaWrite => ibv_wr_opcode::IBV_WR_RDMA_WRITE,
        SendWrOpcode::RdmaRead => ibv_wr_opcode::IBV_WR_RDMA_READ,
        SendWrOpcode::RdmaCompareSwap => ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP,
        SendWrOpcode::Send => ibv_wr_opcode::IBV_WR_SEND,
    }
}
