//! RDMA Transport: thin wrapper over the existing src/rdma/ + src/runtime/ stack.
//! Does NOT modify any existing RDMA code.

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ibverbs_sys::{ibv_access_flags, ibv_qp_type};

use crate::control::client::ControlClient;
use crate::error::RdmaError;
use crate::rdma::qp::{ScatterGatherEntry, SendWorkRequest, SendWrOpcode};
use crate::rdma::QpGuard;
use crate::rdma::{CompletionQueue, Context, ProtectionDomain, QueuePair};
use crate::runtime::poller::Poller;

use super::{ReconnectableTransport, Transport};

pub struct RdmaTransport {
    /// Guarded QP with health checking before each post.
    qp_guard: QpGuard,
    cq: Arc<CompletionQueue>,
    next_wr_id: AtomicU64,
    #[allow(dead_code)]
    pending: Arc<
        std::sync::Mutex<
            std::collections::HashMap<
                u64,
                tokio::sync::oneshot::Sender<Result<crate::rdma::WorkCompletion, RdmaError>>,
            >,
        >,
    >,
    _poller: Poller,
    _context: Context,
    _pd: ProtectionDomain,
}

impl RdmaTransport {
    /// Internal helper: build a fully-initialized QP (INIT → RTR → RTS).
    fn init_qp(
        context: &Context,
        pd: &ProtectionDomain,
        cq: &Arc<CompletionQueue>,
    ) -> Result<QueuePair, RdmaError> {
        let mut qp = QueuePair::create(pd, cq, cq, 128, 128, 1, 1, ibv_qp_type::IBV_QPT_RC)?;

        let access_flags = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as u32)
            | (ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as u32)
            | (ibv_access_flags::IBV_ACCESS_REMOTE_READ as u32)
            | (ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC as u32);

        qp.init(1, access_flags)?;

        let port_attr = context.query_port(1)?;
        let gid = context.query_gid(1, 0);
        let qp_num = qp.qp_num();

        qp.ready_to_receive(qp_num, port_attr.lid, gid, 1, 0)?;
        qp.ready_to_send(0)?;

        Ok(qp)
    }
}

#[async_trait]
impl Transport for RdmaTransport {
    async fn connect(server_addr: &str) -> Result<Self, RdmaError> {
        // 1. Connect via gRPC to discover server metadata
        let mut control = ControlClient::connect(server_addr)
            .await
            .map_err(|e| RdmaError::Internal(format!("gRPC connect: {}", e)))?;
        let _metadata = control
            .discover()
            .await
            .map_err(|e| RdmaError::Internal(format!("discover: {}", e)))?;

        // 2. Open RDMA device
        let context =
            Context::open().ok_or_else(|| RdmaError::Internal("No RDMA device found".into()))?;

        // 3. Create PD, CQ
        let pd = ProtectionDomain::allocate(&context)?;
        let cq =
            CompletionQueue::create(&context, 128, std::ptr::null_mut(), std::ptr::null_mut(), 0)?;
        let cq = Arc::new(cq);

        // 4. Create and initialize QP, wrap in QpGuard
        let qp = Self::init_qp(&context, &pd, &cq)?;
        let qp_guard = QpGuard::new(Arc::new(qp));

        // 5. Start poller
        let pending = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let (poller, _poller_pending) = Poller::spawn(cq.clone(), None);

        Ok(RdmaTransport {
            qp_guard,
            cq,
            next_wr_id: AtomicU64::new(1),
            pending,
            _poller: poller,
            _context: context,
            _pd: pd,
        })
    }

    async fn read(
        &self,
        buf: &mut [u8],
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<(), RdmaError> {
        let mut wr = SendWorkRequest {
            wr_id: self.next_wr_id.fetch_add(1, Ordering::Relaxed),
            opcode: SendWrOpcode::RdmaRead,
            send_flags: 0,
            sge: vec![ScatterGatherEntry {
                addr: buf.as_mut_ptr() as *mut std::os::raw::c_void,
                length: buf.len() as u32,
                lkey,
            }],
            remote_addr: Some(remote_addr),
            remote_rkey: Some(rkey),
            compare_add: None,
            swap: None,
        };

        self.qp_guard.post_send(&mut wr)?;

        let wcs = self.cq.poll(1)?;
        if wcs.is_empty() || !wcs[0].is_success() {
            return Err(RdmaError::Internal("READ completion failed".into()));
        }
        Ok(())
    }

    async fn write(
        &self,
        buf: &[u8],
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<(), RdmaError> {
        let mut wr = SendWorkRequest {
            wr_id: self.next_wr_id.fetch_add(1, Ordering::Relaxed),
            opcode: SendWrOpcode::RdmaWrite,
            send_flags: 0,
            sge: vec![ScatterGatherEntry {
                addr: buf.as_ptr() as *mut std::os::raw::c_void,
                length: buf.len() as u32,
                lkey,
            }],
            remote_addr: Some(remote_addr),
            remote_rkey: Some(rkey),
            compare_add: None,
            swap: None,
        };

        self.qp_guard.post_send(&mut wr)?;

        let wcs = self.cq.poll(1)?;
        if wcs.is_empty() || !wcs[0].is_success() {
            return Err(RdmaError::Internal("WRITE completion failed".into()));
        }
        Ok(())
    }

    async fn cas(
        &self,
        compare: u64,
        swap: u64,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<bool, RdmaError> {
        let mut wr = SendWorkRequest {
            wr_id: self.next_wr_id.fetch_add(1, Ordering::Relaxed),
            opcode: SendWrOpcode::RdmaCompareSwap,
            send_flags: 0,
            sge: vec![ScatterGatherEntry {
                addr: std::ptr::null_mut(),
                length: 8,
                lkey,
            }],
            remote_addr: Some(remote_addr),
            remote_rkey: Some(rkey),
            compare_add: Some(compare),
            swap: Some(swap),
        };

        self.qp_guard.post_send(&mut wr)?;

        let wcs = self.cq.poll(1)?;
        Ok(!wcs.is_empty() && wcs[0].is_success())
    }

    fn is_rdma(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "RDMA"
    }
}

#[async_trait]
impl ReconnectableTransport for RdmaTransport {
    async fn reconnect(&self, server_addr: &str) -> Result<Box<dyn Transport>, RdmaError> {
        // Reconnect: create a brand-new transport instance from scratch.
        // The old QP will be dropped when the old RdmaTransport is dropped.
        let transport = RdmaTransport::connect(server_addr).await?;
        Ok(Box::new(transport))
    }
}
