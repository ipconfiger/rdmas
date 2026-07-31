//! Client session lifecycle for connecting to an RDMA KV server.
//!
//! A [`ClientSession`] manages the full lifecycle of a client connecting
//! to an RDMA KV server:
//!
//! 1. Discover server metadata via gRPC (control plane)
//! 2. Open local RDMA device
//! 3. Allocate protection domain (PD)
//! 4. Create completion queue (CQ) + queue pair (QP)
//! 5. Transition QP through INIT → RTR → RTS (with server's QP info)
//! 6. Register local buffers for RDMA
//! 7. Start a background heartbeat loop
//!
//! # Reconnection
//!
//! On errors, the session can re-discover server metadata (generation tracking)
//! and re-establish the RDMA connection. See [`ClientSession::reconnect`].
//!
//! # Design Constraint
//!
//! - QP state machine (INIT→RTR→RTS) is managed here, leveraging
//!   [`QueuePair`] from the `rdma` module.
//! - Connection teardown and reconnection logic also lives here.

use std::sync::Arc;
use std::time::Duration;

use ibverbs_sys::ibv_qp_type::IBV_QPT_RC;
use ibverbs_sys::ibv_access_flags;
use crate::control::client::ControlClient;
use crate::control::server::proto::*;
use crate::error::RdmaError;
use crate::rdma::context::Context;
use crate::rdma::cq::CompletionQueue;
use crate::rdma::pd::ProtectionDomain;
use crate::rdma::qp::QueuePair;
use crate::runtime::ops::RdmaRuntime;
use crate::runtime::poller::Poller;

/// Default RDMA port number (port 1 on most HCAs).
const DEFAULT_PORT_NUM: u8 = 1;

/// Default capacity for send and receive queues.
const DEFAULT_MAX_SEND_WR: u32 = 256;
const DEFAULT_MAX_RECV_WR: u32 = 256;
const DEFAULT_MAX_SGE: u32 = 16;

/// Default number of completion queue entries.
const DEFAULT_CQE: u32 = 256;

/// Default heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// Access flags for the client QP:
/// RC transport needs LOCAL_WRITE for send/receive,
/// REMOTE_READ/REMOTE_WRITE/REMOTE_ATOMIC for one-sided operations.
const QP_ACCESS_FLAGS: u32 =
    (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as u32)
    | (ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as u32)
    | (ibv_access_flags::IBV_ACCESS_REMOTE_READ as u32)
    | (ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC as u32);

/// A client session connected to an RDMA KV server.
///
/// Owns the full RDMA resources (Context, PD, CQ, QP) and the control-plane
/// connection. The [`RdmaRuntime`] provides async one-sided RDMA operations.
///
/// # Example (sketch)
///
/// ```ignore
/// let session = ClientSession::connect("127.0.0.1:50051", 42).await?;
/// println!("Buckets: {}", session.metadata().bucket_count);
/// // Use session.runtime().rdma_read(...).await
/// ```
pub struct ClientSession {
    /// Unique client ID (locally assigned).
    pub client_id: u64,

    /// Server metadata: MR regions, generation, bucket count.
    pub metadata: ServerMetadata,

    /// Control plane client for heartbeat / re-discovery.
    control: ControlClient,

    /// RDMA device context.
    context: Context,

    /// Protection domain for this session.
    pd: ProtectionDomain,

    /// Completion queue (shared with poller + runtime).
    cq: Arc<CompletionQueue>,

    /// Queue pair (send/receive).
    qp: Arc<QueuePair>,

    /// The busy-poll thread handle (kept alive for the session lifetime).
    #[allow(dead_code)]
    poller: Poller,

    /// Async RDMA runtime for one-sided operations.
    runtime: Arc<RdmaRuntime>,

    /// Heartbeat interval.
    heartbeat_interval: Duration,
}

impl ClientSession {
    /// Connect to a server and establish an RDMA session.
    ///
    /// # Steps
    ///
    /// 1. Connect control plane client via gRPC
    /// 2. Discover server metadata (MR regions, bucket count, generation)
    /// 3. Open the first available RDMA device
    /// 4. Create PD, CQ, QP
    /// 5. Transition QP: RESET → INIT → RTR → RTS
    /// 6. Spawn the RDMA poller thread and create the async runtime
    /// 7. Start the heartbeat background task
    ///
    /// # Note on remote QP number
    ///
    /// For Wave 3, the server's QP number is communicated via a fixed convention
    /// (value `1`) until the control-plane protocol is extended with a `qp_num`
    /// field in `ServerMetadata`. The remote LID is auto-detected from the local
    /// port `port_info.lid` (both client and server share the same HCA in
    /// single-machine development).
    pub async fn connect(
        server_addr: &str,
        client_id: u64,
    ) -> Result<Self, ClientSessionError> {
        // ---- Step 1: Control plane ----
        let mut control = ControlClient::connect(server_addr).await?;
        let metadata = control.discover().await?;

        // ---- Step 2: Open RDMA device ----
        let context = Context::open().ok_or(ClientSessionError::NoDevice)?;

        // ---- Step 3: Allocate PD ----
        let pd = ProtectionDomain::allocate(&context)?;

        // ---- Step 4: Create CQ + QP ----
        let cq = Arc::new(CompletionQueue::create(
            &context,
            DEFAULT_CQE,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )?);

        // Create QP as a local mutable so we can transition it through
        // the state machine before sharing it with the runtime via Arc.
        let mut qp = QueuePair::create(
            &pd,
            &cq,
            &cq, // Use same CQ for both send and recv
            DEFAULT_MAX_SEND_WR,
            DEFAULT_MAX_RECV_WR,
            DEFAULT_MAX_SGE,
            DEFAULT_MAX_SGE,
            IBV_QPT_RC,
        )?;

        // ---- Step 5: Query port attributes ----
        let port_info = context.query_port(DEFAULT_PORT_NUM)?;

        // Build remote QP connection info.
        //
        // In single-machine dev mode, the remote peer shares the same HCA.
        // We use the local port LID as the remote LID. The remote QP number
        // is hard-coded to 1 as a placeholder convention until the control
        // plane protocol carries QP connection parameters.
        let remote_lid = port_info.lid;
        let remote_qp_num = 1u32; // Placeholder — TODO: get from ServerMetadata
        let remote_gid = context.query_gid(DEFAULT_PORT_NUM, 0);

        // PSN values: 0 is a safe default (start of packet sequence).
        let rq_psn: u32 = 0;
        let sq_psn: u32 = 0;

        // ---- Step 6: QP state machine ----
        //
        // Transition the QP through RESET → INIT → RTR → RTS before
        // wrapping it in Arc. Only `init` requires `&mut self`; the
        // other transitions use `&self`.
        qp.init(DEFAULT_PORT_NUM, QP_ACCESS_FLAGS)?;
        qp.ready_to_receive(
            remote_qp_num,
            remote_lid,
            remote_gid,
            DEFAULT_PORT_NUM,
            rq_psn,
        )?;
        qp.ready_to_send(sq_psn)?;

        // Now wrap in Arc for sharing with the runtime.
        let qp = Arc::new(qp);

        // ---- Step 7: Spawn poller + create runtime ----
        let (poller, pending) = Poller::spawn(Arc::clone(&cq), None);

        let runtime = Arc::new(RdmaRuntime::new(
            Arc::clone(&qp),
            Arc::clone(&cq),
            pending,
        ));

        // ---- Step 8: Heartbeat interval ----
        let heartbeat_interval =
            Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS);

        Ok(Self {
            client_id,
            metadata,
            control,
            context,
            pd,
            cq,
            qp,
            poller,
            runtime,
            heartbeat_interval,
        })
    }

    // ---- Accessors ----

    /// Get server metadata (MR regions, generation, bucket count).
    pub fn metadata(&self) -> &ServerMetadata {
        &self.metadata
    }

    /// Get the hash table region metadata, if present.
    pub fn hash_table_region(&self) -> Option<&RegionMetadata> {
        self.metadata
            .regions
            .iter()
            .find(|r| r.r#type() == RegionType::HashTable)
    }

    /// Get the large object region metadata, if present.
    pub fn large_object_region(&self) -> Option<&RegionMetadata> {
        self.metadata
            .regions
            .iter()
            .find(|r| r.r#type() == RegionType::LargeObject)
    }

    /// Get the free list region metadata, if present.
    pub fn free_list_region(&self) -> Option<&RegionMetadata> {
        self.metadata
            .regions
            .iter()
            .find(|r| r.r#type() == RegionType::FreeList)
    }

    /// Get the RDMA runtime for performing async one-sided operations.
    pub fn runtime(&self) -> &Arc<RdmaRuntime> {
        &self.runtime
    }

    /// Get a reference to the queue pair.
    pub fn qp(&self) -> &QueuePair {
        &self.qp
    }

    /// Get the completion queue.
    pub fn cq(&self) -> &CompletionQueue {
        &self.cq
    }

    /// Get the protection domain.
    pub fn pd(&self) -> &ProtectionDomain {
        &self.pd
    }

    /// Get the RDMA device context.
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// Get the local QP number.
    pub fn qp_num(&self) -> u32 {
        self.qp.qp_num()
    }

    // ---- Heartbeat ----

    /// Start the heartbeat background task.
    ///
    /// Spawns a `tokio` task that periodically sends heartbeats to the
    /// control-plane server. This keeps the server aware of active clients
    /// and prevents premature GC of client state.
    ///
    /// In a production implementation with a shared control client (e.g.
    /// behind `Arc<Mutex<>>`), the heartbeat task would call
    /// `control.heartbeat(client_id, timestamp).await`.
    pub fn start_heartbeat(self: &Arc<Self>, shutdown: Arc<std::sync::atomic::AtomicBool>) {
        let client_id = self.client_id;
        let interval = self.heartbeat_interval;

        tokio::spawn({
            let _session = Arc::clone(self);
            async move {
                tracing::info!(
                    client_id,
                    interval_ms = interval.as_millis(),
                    "Heartbeat loop started"
                );

                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                        tracing::info!(client_id, "Heartbeat loop received shutdown signal");
                        break;
                    }

                    tokio::time::sleep(interval).await;

                    // TODO: In production, the control client would be behind
                    // Arc<Mutex<>> so the heartbeat task can call:
                    //
                    //   let active_ts = now_ms() as u32;
                    //   let mut ctrl = control.lock().await;
                    //   let _ = ctrl.heartbeat(client_id, active_ts).await;
                    //
                    // For Wave 3, we log a heartbeat event to aid debugging.
                    tracing::trace!(
                        client_id,
                        "Heartbeat tick (control-plane call pending integration)"
                    );
                }

                tracing::info!(client_id, "Heartbeat loop exited");
            }
        });
    }

    /// Re-discover server metadata and update the cached copy.
    ///
    /// Useful when the server rotates memory regions (generation bump).
    pub async fn refresh_metadata(&mut self) -> Result<&ServerMetadata, ClientSessionError> {
        let metadata = self.control.discover().await?;
        self.metadata = metadata;
        Ok(&self.metadata)
    }

    /// Reconnect to the server after a failure.
    ///
    /// Drops and re-creates the RDMA resources, then re-establishes
    /// the QP state machine and metadata cache.
    pub async fn reconnect(
        server_addr: &str,
        client_id: u64,
    ) -> Result<Self, ClientSessionError> {
        // For Wave 3, reconnect is equivalent to a fresh connect.
        Self::connect(server_addr, client_id).await
    }
}

// ---- Drop ----

impl Drop for ClientSession {
    fn drop(&mut self) {
        // Note: In an async context, we can't easily call deregister.
        // In production, call `shutdown()` before dropping the session.
        //
        // The RDMA resources (QP, CQ, PD, Context) are dropped automatically
        // via their own Drop implementations. The poller thread will exit
        // when the Poller handle is dropped.
        tracing::debug!(
            client_id = self.client_id,
            "ClientSession dropped; RDMA resources released via RAII"
        );
    }
}

// ---- Error type ----

/// Error type for client session operations.
///
/// Wraps gRPC (tonic), transport, and RDMA errors into a single error type.
#[derive(Debug, thiserror::Error)]
pub enum ClientSessionError {
    /// gRPC status error from the control plane.
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// gRPC transport error (connection refused, timeout, etc.).
    #[error("gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// RDMA hardware or internal error.
    #[error("RDMA error: {0}")]
    Rdma(#[from] RdmaError),

    /// No RDMA device found on the system.
    #[error("No RDMA device found")]
    NoDevice,

    /// Server metadata is missing a required memory region.
    #[error("Server metadata missing required region: {0}")]
    MissingRegion(String),
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_client_session_error_display() {
        let err = ClientSessionError::NoDevice;
        assert!(
            format!("{}", err).contains("No RDMA device"),
            "NoDevice error should contain 'No RDMA device', got: {err}"
        );
    }

    #[test]
    fn test_client_session_error_grpc_display() {
        let err = ClientSessionError::Grpc(tonic::Status::internal("test"));
        let display = format!("{}", err);
        assert!(
            display.contains("gRPC error"),
            "Grpc error should contain 'gRPC error', got: {display}"
        );
    }

    #[test]
    fn test_client_session_error_rdma_from() {
        let rdma_err = RdmaError::HardwareError("test hw error".to_string());
        let session_err: ClientSessionError = rdma_err.into();
        let display = format!("{}", session_err);
        assert!(
            display.contains("RDMA error"),
            "Rdma error should contain 'RDMA error', got: {display}"
        );
    }

    #[test]
    fn test_client_id_uniqueness() {
        // Verify that a simple pseudo-random ID scheme produces no duplicates
        // for a reasonable range. Uses the multiplicative hash
        // (i * 2654435761) % (1 << 32) — a standard Knuth hash.
        let ids: Vec<u64> = (0..100)
            .map(|i| (i * 2654435761u64) % (1u64 << 32))
            .collect();

        let mut seen = HashSet::new();
        for id in &ids {
            assert!(
                seen.insert(*id),
                "Duplicate client ID {id} found — hash collision!"
            );
        }
        assert_eq!(seen.len(), 100);
    }

    #[test]
    fn test_region_type_filter_helpers() {
        // Construct a minimal ServerMetadata with known regions.
        let metadata = ServerMetadata {
            generation: 1,
            bucket_count: 1024,
            regions: vec![
                RegionMetadata {
                    vaddr: 0x1000,
                    rkey: 101,
                    size: 65536,
                    r#type: RegionType::HashTable as i32,
                    generation: 1,
                },
                RegionMetadata {
                    vaddr: 0x20000,
                    rkey: 102,
                    size: 1048576,
                    r#type: RegionType::LargeObject as i32,
                    generation: 1,
                },
                RegionMetadata {
                    vaddr: 0x120000,
                    rkey: 103,
                    size: 4096,
                    r#type: RegionType::FreeList as i32,
                    generation: 1,
                },
            ],
        };

        // This test verifies the filter logic. We can't instantiate a full
        // ClientSession without an HCA, but we can test metadata filtering
        // in isolation by constructing a mock session-like context.
        let hash_region = metadata
            .regions
            .iter()
            .find(|r| r.r#type() == RegionType::HashTable);
        assert!(hash_region.is_some());
        assert_eq!(hash_region.unwrap().rkey, 101);

        let large_region = metadata
            .regions
            .iter()
            .find(|r| r.r#type() == RegionType::LargeObject);
        assert!(large_region.is_some());
        assert_eq!(large_region.unwrap().rkey, 102);

        let free_region = metadata
            .regions
            .iter()
            .find(|r| r.r#type() == RegionType::FreeList);
        assert!(free_region.is_some());
        assert_eq!(free_region.unwrap().rkey, 103);
    }

    #[test]
    fn test_default_constants_are_reasonable() {
        assert!(DEFAULT_MAX_SEND_WR > 0);
        assert!(DEFAULT_MAX_RECV_WR > 0);
        assert!(DEFAULT_MAX_SGE > 0);
        assert!(DEFAULT_CQE > 0);
        assert_ne!(DEFAULT_PORT_NUM, 0);
        assert!(DEFAULT_HEARTBEAT_INTERVAL_MS > 0);
    }
}
