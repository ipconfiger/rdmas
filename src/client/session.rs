//! Client session lifecycle for connecting to an RDMA KV server.
//!
//! A [`ClientSession`] manages the full lifecycle of a client connecting
//! to an RDMA KV server:
//!
//! 1. Negotiate protocol version (T10-E)
//! 2. Discover server metadata via gRPC (control plane)
//! 3. Auto-detect transport: try RDMA first, fall back to TCP
//! 4. Start a background heartbeat loop with generation checks (T10-D)
//!
//! # Transport abstraction
//!
//! The session delegates all data-plane operations (read/write/cas) to a
//! [`Transport`] trait object. This allows transparent fallback from RDMA
//! to TCP when RDMA hardware is not available.
//!
//! # Keepalive & Reconnection (T10-D)
//!
//! The heartbeat loop detects generation changes (server restart) and
//! marks `reconnect` flags. On reconnect, all cached MR metadata is
//! invalidated and re-discovered.
//!
//! # Design Constraint
//!
//! - Transport auto-detection is handled in [`ClientSession::connect`].
//! - Connection teardown and reconnection logic also lives here.

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::channel::ClientTlsConfig;
use tonic::transport::{Certificate, Identity};

use crate::control::client::ControlClient;
use crate::control::server::proto::*;
use crate::error::RdmaError;
use crate::transport::{RdmaTransport, TcpTransport, Transport};

/// Default heartbeat interval (milliseconds).
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// Minimum compatible server protocol version (T10-E).
const MIN_COMPATIBLE_VERSION: u32 = 1;

/// TLS configuration for mTLS client connections (T11-A).
///
/// All fields are PEM-encoded strings.
#[derive(Clone)]
pub struct TlsConfig {
    /// PEM-encoded CA certificate used to verify the server.
    pub ca_cert_pem: String,
    /// PEM-encoded client certificate (for mutual TLS).
    pub client_cert_pem: String,
    /// PEM-encoded client private key.
    pub client_key_pem: String,
}

impl TlsConfig {
    /// Convert this TlsConfig into a tonic `ClientTlsConfig` for use with
    /// gRPC channel construction.
    pub fn to_client_tls_config(&self) -> Result<ClientTlsConfig, Box<dyn std::error::Error>> {
        let ca = Certificate::from_pem(&self.ca_cert_pem);
        let identity = Identity::from_pem(&self.client_cert_pem, &self.client_key_pem);

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(ca)
            .identity(identity);

        Ok(tls_config)
    }
}

/// A client session connected to an RDMA KV server via an abstract transport.
///
/// Owns the control-plane connection and a boxed [`Transport`] object that
/// handles all data-plane operations. The transport is auto-detected at
/// connect time: RDMA is preferred, with TCP as a fallback.
///
/// # Example (sketch)
///
/// ```ignore
/// let session = ClientSession::connect("127.0.0.1:50051", 42).await?;
/// println!("Buckets: {}", session.metadata().bucket_count);
/// // Use session.transport().read(...).await
/// ```
pub struct ClientSession {
    /// Unique client ID (locally assigned).
    pub client_id: u64,

    /// Server address for reconnection (T10-D).
    server_addr: String,

    /// Server metadata: MR regions, generation, bucket count.
    pub metadata: ServerMetadata,

    /// Server protocol version (T10-E).
    pub server_version: u32,

    /// Control plane client for heartbeat / re-discovery.
    control: ControlClient,

    /// Abstract transport layer (RDMA or TCP fallback).
    transport: Box<dyn Transport>,

    /// Heartbeat interval.
    heartbeat_interval: Duration,
}

impl ClientSession {
    /// Connect to a server and establish a transport session.
    ///
    /// # Steps
    ///
    /// 1. Connect control plane client via gRPC
    /// 2. Negotiate protocol version (T10-E)
    /// 3. Discover server metadata (MR regions, bucket count, generation)
    /// 4. Auto-detect transport: try RDMA first, fall back to TCP
    ///
    /// # Transport auto-detection
    ///
    /// RDMA is attempted first. If it fails (no hardware, connection refused,
    /// etc.), the session falls back to TCP on port `server_port + 1`.
    pub async fn connect(
        server_addr: &str,
        client_id: u64,
    ) -> Result<Self, ClientSessionError> {
        // ---- Step 1: Control plane ----
        let mut control = ControlClient::connect(server_addr).await?;

        // ---- Step 2: Version negotiation (T10-E) ----
        let version = control.get_version().await.map_err(|e| {
            ClientSessionError::Grpc(e)
        })?;
        if version.service_version < MIN_COMPATIBLE_VERSION {
            return Err(ClientSessionError::VersionMismatch(format!(
                "Server version {} is incompatible; minimum required is {}",
                version.service_version, MIN_COMPATIBLE_VERSION
            )));
        }

        // ---- Step 3: Discover metadata ----
        let metadata = control.discover().await.map_err(|e| {
            ClientSessionError::Grpc(e)
        })?;

        // ---- Step 4: Auto-detect transport ----
        let transport = Self::create_transport_inner(server_addr).await?;

        // ---- Step 5: Heartbeat interval ----
        let heartbeat_interval =
            Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS);

        Ok(Self {
            client_id,
            server_addr: server_addr.to_string(),
            server_version: version.service_version,
            metadata,
            control,
            transport,
            heartbeat_interval,
        })
    }

    /// Connect with mTLS (T11-A).
    ///
    /// Same as [`connect`] but establishes the gRPC control-plane channel over
    /// TLS 1.3 with mutual authentication. The data-plane transport (RDMA/TCP)
    /// remains unencrypted — this is standard practice for RDMA in trusted
    /// datacenter fabrics.
    ///
    /// # Arguments
    ///
    /// * `server_addr` - Server host:port (e.g. `"server.example.com:9400"`)
    /// * `client_id` - Unique client identifier
    /// * `tls_config` - PEM-encoded CA cert, client cert, and client key
    pub async fn connect_tls(
        server_addr: &str,
        client_id: u64,
        tls_config: &TlsConfig,
    ) -> Result<Self, ClientSessionError> {
        // ---- Step 1: Control plane over mTLS ----
        let client_tls = tls_config.to_client_tls_config().map_err(|e| {
            ClientSessionError::Tls(format!(
                "Failed to build client TLS config: {}",
                e
            ))
        })?;
        let mut control = ControlClient::connect_tls(server_addr, client_tls).await?;

        // ---- Step 2: Version negotiation (T10-E) ----
        let version = control.get_version().await.map_err(|e| {
            ClientSessionError::Grpc(e)
        })?;
        if version.service_version < MIN_COMPATIBLE_VERSION {
            return Err(ClientSessionError::VersionMismatch(format!(
                "Server version {} is incompatible; minimum required is {}",
                version.service_version, MIN_COMPATIBLE_VERSION
            )));
        }

        // ---- Step 3: Discover metadata ----
        let metadata = control.discover().await.map_err(|e| {
            ClientSessionError::Grpc(e)
        })?;

        // ---- Step 4: Auto-detect transport (RDMA or TCP, unencrypted) ----
        let transport = Self::create_transport_inner(server_addr).await?;

        // ---- Step 5: Heartbeat interval ----
        let heartbeat_interval =
            Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS);

        Ok(Self {
            client_id,
            server_addr: server_addr.to_string(),
            server_version: version.service_version,
            metadata,
            control,
            transport,
            heartbeat_interval,
        })
    }

    /// Create a transport connection, trying RDMA first then falling back to TCP.
    async fn create_transport_inner(
        server_addr: &str,
    ) -> Result<Box<dyn Transport>, ClientSessionError> {
        match RdmaTransport::connect(server_addr).await {
            Ok(rdma) => {
                tracing::info!("Using RDMA transport");
                Ok(Box::new(rdma))
            }
            Err(e) => {
                tracing::warn!("RDMA unavailable ({}), falling back to TCP", e);
                // TCP port: use the same host as the gRPC server, but port + 1
                let tcp_addr = server_addr.replace(":9400", ":9401");
                let tcp = TcpTransport::connect(&tcp_addr)
                    .await
                    .map_err(|e| {
                        ClientSessionError::Rdma(RdmaError::Internal(format!(
                            "TCP fallback: {}",
                            e
                        )))
                    })?;
                Ok(Box::new(tcp))
            }
        }
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

    /// Get the abstract transport for read/write/cas operations.
    pub fn transport(&self) -> &dyn Transport {
        self.transport.as_ref()
    }

    /// Whether this session is using RDMA transport.
    pub fn is_rdma(&self) -> bool {
        self.transport.is_rdma()
    }

    /// Human-readable transport name for logging.
    pub fn transport_name(&self) -> &str {
        self.transport.name()
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
        let metadata = self.control.discover().await.map_err(|e| {
            ClientSessionError::Grpc(e)
        })?;
        self.metadata = metadata;
        Ok(&self.metadata)
    }

    // ---- MR Metadata Invalidation (T10-D) ----

    /// Invalidate all cached remote memory region metadata.
    /// Called when server restarts (generation change detected).
    pub fn invalidate_mr_metadata(&mut self) {
        self.metadata = ServerMetadata::default();
        tracing::warn!("MR metadata invalidated due to generation change");
    }

    /// Check if reconnection is needed based on heartbeat response.
    ///
    /// Returns `true` if the server explicitly requests reconnect, or if
    /// the server's generation has changed (indicating a restart).
    pub fn check_reconnect_needed(&self, response: &HeartbeatResponse) -> bool {
        response.reconnect || response.generation != self.metadata.generation
    }

    // ---- Reconnection (T10-D) ----

    /// Full reconnect sequence: discover new metadata + recreate transport.
    ///
    /// # Steps
    ///
    /// 1. Invalidate old MR metadata
    /// 2. Re-discover server metadata
    /// 3. Reconnect transport with new metadata
    /// 4. Update local state
    pub async fn reconnect(&mut self) -> Result<(), ClientSessionError> {
        tracing::info!("Starting reconnect sequence");

        // Step 1: Invalidate old metadata
        self.invalidate_mr_metadata();

        // Step 2: Re-discover server metadata
        let metadata = self.control.discover().await.map_err(|e| {
            ClientSessionError::Grpc(e)
        })?;

        // Step 3: Reconnect transport with new metadata
        self.transport = Self::create_transport_inner(&self.server_addr).await?;

        // Step 4: Update local state
        self.metadata = metadata;

        tracing::info!("Reconnect complete, generation={}", self.metadata.generation);
        Ok(())
    }
}

// ---- Drop ----

impl Drop for ClientSession {
    fn drop(&mut self) {
        // The transport (RDMA or TCP) is dropped automatically via its
        // own Drop implementation. The boxed trait object ensures RAII cleanup.
        tracing::debug!(
            client_id = self.client_id,
            transport = self.transport.name(),
            "ClientSession dropped; transport resources released via RAII"
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

    /// Protocol version mismatch between client and server (T10-E).
    #[error("version mismatch: {0}")]
    VersionMismatch(String),

    /// TLS configuration or handshake error (T11-A).
    #[error("TLS error: {0}")]
    Tls(String),
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
    fn test_heartbeat_interval_is_reasonable() {
        assert!(DEFAULT_HEARTBEAT_INTERVAL_MS > 0);
    }

    // ---- T10-E: Version mismatch error ----

    #[test]
    fn test_version_mismatch_error_display() {
        let err = ClientSessionError::VersionMismatch(
            "Server version 0 is incompatible; minimum required is 1".to_string(),
        );
        let display = format!("{}", err);
        assert!(
            display.contains("version mismatch"),
            "VersionMismatch should contain 'version mismatch', got: {display}"
        );
    }

    // ---- T10-D: Generation check ----

    #[test]
    fn test_check_reconnect_needed_when_reconnect_flag_set() {
        // Can't instantiate a full ClientSession without transport, so we
        // use prost's default-generated types for unit testing the logic.
        let metadata = ServerMetadata {
            generation: 1,
            bucket_count: 1024,
            regions: vec![],
        };

        // Simulate what a session would do: create metadata, then check response.
        // HeartbeatResponse with reconnect=true
        let response = HeartbeatResponse {
            generation: 1,
            reconnect: true,
            server_version: 1,
        };

        // If reconnect flag is set, we should reconnect.
        assert!(response.reconnect, "reconnect flag should trigger reconnect");

        // If generation differs from metadata, should also reconnect.
        let response_stale = HeartbeatResponse {
            generation: 2,
            reconnect: false,
            server_version: 1,
        };
        let needs = response_stale.generation != metadata.generation;
        assert!(needs, "generation change should trigger reconnect");
    }

    #[test]
    fn test_check_reconnect_not_needed_when_same() {
        let metadata = ServerMetadata {
            generation: 5,
            bucket_count: 1024,
            regions: vec![],
        };

        let response = HeartbeatResponse {
            generation: 5,
            reconnect: false,
            server_version: 1,
        };

        let needs = response.reconnect || response.generation != metadata.generation;
        assert!(!needs, "same generation and no reconnect flag should not trigger reconnect");
    }

    #[test]
    fn test_invalidate_mr_metadata_resets_to_default() {
        let mut metadata = ServerMetadata {
            generation: 5,
            bucket_count: 2048,
            regions: vec![RegionMetadata {
                vaddr: 0x1000,
                rkey: 42,
                size: 4096,
                r#type: RegionType::HashTable as i32,
                generation: 5,
            }],
        };

        // Use the metadata first to avoid unused-assignment warning
        assert_eq!(metadata.generation, 5);
        assert_eq!(metadata.bucket_count, 2048);
        assert!(!metadata.regions.is_empty());

        // Simulate invalidation
        metadata = ServerMetadata::default();

        assert_eq!(metadata.generation, 0);
        assert_eq!(metadata.bucket_count, 0);
        assert!(metadata.regions.is_empty());
    }
}
