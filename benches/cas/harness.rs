//! RDMA CAS benchmark harness.
//!
//! Sets up a self-connected QP (loopback via SoftRoCE rxe0)
//! for measuring CAS/READ/WRITE latency and throughput.
//!
//! # Self-Connection Protocol
//!
//! Since we use a single process on a single machine:
//! 1. Open rxe0 device
//! 2. Create PD, MR, CQ, QP
//! 3. Transition QP INIT → RTR (self-loop) → RTS
//! 4. Post RDMA operations targeting its own MR

use rdmas::error::RdmaError;
use rdmas::rdma::qp::{ScatterGatherEntry, SendWorkRequest, SendWrOpcode};
use rdmas::rdma::{CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair};

/// Context bundle for benchmark operations.
pub struct BenchContext {
    pub cq: CompletionQueue,
    pub qp: QueuePair,
    pub mr: MemoryRegion,
    /// Buffer backing the MR (must outlive mr)
    _buf: Vec<u8>,
    /// Buffer for receiving CAS data
    _cas_buf: [u64; 1],
}

/// Allocate a 1-page buffer for benchmark use.
const BENCH_BUF_SIZE: usize = 4096;

/// Initialize RDMA resources for self-connected CAS benchmark.
pub fn setup_rdma() -> Result<BenchContext, RdmaError> {
    let context =
        Context::open().ok_or_else(|| RdmaError::HardwareError("No RDMA device found".into()))?;

    eprintln!("Device: {}", context.name());

    let pd = ProtectionDomain::allocate(&context)?;

    let mut buf = vec![0u8; BENCH_BUF_SIZE];
    let mr = MemoryRegion::register(
        &pd,
        buf.as_mut_ptr() as *mut libc::c_void,
        BENCH_BUF_SIZE,
        (ibverbs_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as i32)
            | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as i32)
            | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ as i32)
            | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC as i32),
    )?;

    eprintln!(
        "MR: lkey={:#x}, rkey={:#x}, size={}",
        mr.lkey(),
        mr.rkey(),
        mr.size()
    );

    let cq = CompletionQueue::create(&context, 128, std::ptr::null_mut(), std::ptr::null_mut(), 0)?;

    let mut qp = QueuePair::create(
        &pd,
        &cq,
        &cq,
        128, // max_send_wr
        128, // max_recv_wr
        1,   // max_send_sge
        1,   // max_recv_sge
        ibverbs_sys::ibv_qp_type::IBV_QPT_RC,
    )?;

    let qp_num = qp.qp_num();
    eprintln!("QP: qp_num={}", qp_num);

    // Access flags for one-sided RDMA
    let access = (ibverbs_sys::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as u32)
        | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as u32)
        | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_READ as u32)
        | (ibverbs_sys::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC as u32);

    // INIT
    qp.init(1, access)?;
    eprintln!("QP state: INIT");

    // RTR (self-loop: remote_qpn = our own qp_num)
    // For RoCE (SoftRoCE), we need GID. Try GID index 0 first,
    // fall back to index 1 if 0 returns null GID.
    let gid = context
        .query_gid(1, 1)
        .or_else(|| context.query_gid(1, 0))
        .ok_or_else(|| RdmaError::HardwareError("no RoCE GID found".into()))?;
    qp.ready_to_receive(qp_num, 0, Some(gid), 1, 0)?;
    eprintln!("QP state: RTR");

    // RTS
    qp.ready_to_send(0)?;
    eprintln!("QP state: RTS — ready for self-RDMA");

    let cas_buf: [u64; 1] = [0];

    Ok(BenchContext {
        cq,
        qp,
        mr,
        _buf: buf,
        _cas_buf: cas_buf,
    })
}

/// Perform a single RDMA CAS operation and wait for completion.
pub fn bench_cas_single(ctx: &BenchContext) -> bool {
    let addr = ctx.mr.addr() as u64;
    let rkey = ctx.mr.rkey();

    // CAS: compare 0, swap 1
    let mut wr = SendWorkRequest {
        wr_id: 1,
        opcode: SendWrOpcode::RdmaCompareSwap,
        send_flags: 0,
        sge: vec![ScatterGatherEntry {
            addr: ctx.mr.addr(),
            length: 8,
            lkey: ctx.mr.lkey(),
        }],
        remote_addr: Some(addr),
        remote_rkey: Some(rkey),
        compare_add: Some(0),
        swap: Some(1),
    };

    match ctx.qp.post_send(&mut wr) {
        Ok(id) => match ctx.cq.poll(1) {
            Ok(wcs) => wcs.first().map_or(false, |wc| wc.is_success()),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// Perform a single RDMA READ and wait for completion.
pub fn bench_read_single(ctx: &BenchContext) -> bool {
    let addr = ctx.mr.addr() as u64;
    let rkey = ctx.mr.rkey();

    let mut wr = SendWorkRequest {
        wr_id: 2,
        opcode: SendWrOpcode::RdmaRead,
        send_flags: 0,
        sge: vec![ScatterGatherEntry {
            addr: ctx.mr.addr(),
            length: 8,
            lkey: ctx.mr.lkey(),
        }],
        remote_addr: Some(addr),
        remote_rkey: Some(rkey),
        compare_add: None,
        swap: None,
    };

    match ctx.qp.post_send(&mut wr) {
        Ok(_id) => match ctx.cq.poll(1) {
            Ok(wcs) => wcs.first().map_or(false, |wc| wc.is_success()),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// Perform a single RDMA WRITE and wait for completion.
pub fn bench_write_single(ctx: &BenchContext) -> bool {
    let addr = ctx.mr.addr() as u64;
    let rkey = ctx.mr.rkey();

    let mut wr = SendWorkRequest {
        wr_id: 3,
        opcode: SendWrOpcode::RdmaWrite,
        send_flags: 0,
        sge: vec![ScatterGatherEntry {
            addr: ctx.mr.addr(),
            length: 8,
            lkey: ctx.mr.lkey(),
        }],
        remote_addr: Some(addr),
        remote_rkey: Some(rkey),
        compare_add: None,
        swap: None,
    };

    match ctx.qp.post_send(&mut wr) {
        Ok(_id) => match ctx.cq.poll(1) {
            Ok(wcs) => wcs.first().map_or(false, |wc| wc.is_success()),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// Post `batch_size` CAS operations and poll for all completions.
/// Returns true only if ALL operations completed successfully.
pub fn bench_cas_batch(ctx: &BenchContext, batch_size: u32) -> bool {
    let addr = ctx.mr.addr() as u64;
    let rkey = ctx.mr.rkey();

    for i in 0..batch_size {
        let mut wr = SendWorkRequest {
            wr_id: 10 + i as u64,
            opcode: SendWrOpcode::RdmaCompareSwap,
            send_flags: 0,
            sge: vec![ScatterGatherEntry {
                addr: ctx.mr.addr(),
                length: 8,
                lkey: ctx.mr.lkey(),
            }],
            remote_addr: Some(addr),
            remote_rkey: Some(rkey),
            compare_add: Some(0),
            swap: Some((i + 1) as u64),
        };
        if ctx.qp.post_send(&mut wr).is_err() {
            return false;
        }
    }

    let mut completed = 0u32;
    while completed < batch_size {
        match ctx.cq.poll(batch_size - completed) {
            Ok(wcs) => {
                if wcs.iter().all(|wc| wc.is_success()) {
                    completed += wcs.len() as u32;
                } else {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
    true
}
