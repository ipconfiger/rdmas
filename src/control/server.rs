use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
// TLS support for mTLS control-plane encryption (T11-A)
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

use crate::engine::bootstrap::BootstrappedEngine;

// Generated proto module (adjust path based on actual tonic-build output)
pub mod proto {
    tonic::include_proto!("rdmas.control");
}

use proto::control_plane_server::{ControlPlane, ControlPlaneServer};
use proto::*;

/// Protocol version constants (T10-E).
const SERVICE_VERSION: u32 = 1;
const SERVICE_VERSION_STR: &str = "1.0.0";
const MIN_COMPATIBLE_VERSION: u32 = 1;

/// The gRPC control plane server.
pub struct ControlServer {
    engine: Arc<RwLock<BootstrappedEngine>>,
    generation: u64,
    // Client registry: client_id → (active_ts, last_heartbeat)
    clients: Arc<RwLock<std::collections::HashMap<u64, (u32, std::time::Instant)>>>,
    /// Whether TLS/mTLS is enabled for this server instance (T11-A).
    pub use_tls: bool,
}

impl ControlServer {
    pub fn new(engine: BootstrappedEngine) -> Self {
        Self {
            engine: Arc::new(RwLock::new(engine)),
            generation: 1,
            clients: Arc::new(RwLock::new(std::collections::HashMap::new())),
            use_tls: false,
        }
    }

    /// Builder: enable TLS for this server instance (T11-A).
    pub fn with_tls(mut self) -> Self {
        self.use_tls = true;
        self
    }

    /// Create a TLS-configured server using the provided cert chain and private key.
    ///
    /// `cert_pem`: PEM-encoded certificate chain
    /// `key_pem`: PEM-encoded private key
    /// `ca_cert_pem`: Optional CA certificate for client authentication (mTLS)
    ///
    /// When `ca_cert_pem` is `Some`, mutual TLS is enforced: clients must present a
    /// certificate signed by the given CA.
    pub fn tls_config(
        cert_pem: &str,
        key_pem: &str,
        ca_cert_pem: Option<&str>,
    ) -> Result<ServerTlsConfig, Box<dyn std::error::Error>> {
        let identity = Identity::from_pem(cert_pem, key_pem);

        let mut config = ServerTlsConfig::new().identity(identity);

        // If a CA cert is provided, require client certificates (mTLS).
        if let Some(ca_pem) = ca_cert_pem {
            let ca = Certificate::from_pem(ca_pem);
            config = config.client_ca_root(ca);
        }

        Ok(config)
    }

    pub fn into_service(self) -> ControlPlaneServer<Self> {
        ControlPlaneServer::new(self)
    }
}

#[tonic::async_trait]
impl ControlPlane for ControlServer {
    async fn discover(
        &self,
        _request: Request<DiscoverRequest>,
    ) -> Result<Response<DiscoverResponse>, Status> {
        let engine = self.engine.read().await;

        let metadata = ServerMetadata {
            generation: self.generation,
            bucket_count: engine.bucket_count(),
            regions: vec![
                // Note: In real distributed mode, these would come from
                // actual HugePage regions with real rkey/vaddr.
                // For now, provide placeholder metadata.
                RegionMetadata {
                    vaddr: 0,                         // Set by actual MR registration in distributed mode
                    rkey: 0,                          // Set by actual MR registration
                    size: engine.bucket_count() * 64, // Each bucket = 64B
                    r#type: RegionType::HashTable as i32,
                    generation: self.generation,
                },
                RegionMetadata {
                    vaddr: 0,
                    rkey: 0,
                    size: engine.large_object_capacity(),
                    r#type: RegionType::LargeObject as i32,
                    generation: self.generation,
                },
                RegionMetadata {
                    // In local simulation, the free list header is a struct field.
                    // Its address serves as the region start; rkey = 0 for now.
                    // In distributed mode, these will be real HugePage vaddr/rkey.
                    vaddr: engine.free_list_header_addr(),
                    rkey: 0,
                    size: 64, // FreeListHeader is exactly 64 bytes (one cache line)
                    r#type: RegionType::FreeList as i32,
                    generation: self.generation,
                },
            ],
        };

        Ok(Response::new(DiscoverResponse {
            metadata: Some(metadata),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let mut clients = self.clients.write().await;

        clients.insert(req.client_id, (req.active_ts, std::time::Instant::now()));

        Ok(Response::new(HeartbeatResponse {
            generation: self.generation,
            reconnect: false,
            server_version: SERVICE_VERSION,
        }))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let req = request.into_inner();
        self.clients.write().await.remove(&req.client_id);
        Ok(Response::new(DeregisterResponse {}))
    }

    async fn sync_free_list(
        &self,
        request: Request<SyncFreeListRequest>,
    ) -> Result<Response<SyncFreeListResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(count = req.freed_offsets.len(), "sync_free_list");
        Ok(Response::new(SyncFreeListResponse {
            accepted_count: req.freed_offsets.len() as u32,
        }))
    }

    /// Protocol version negotiation (T10-E).
    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            service_version: SERVICE_VERSION,
            service_version_str: SERVICE_VERSION_STR.to_string(),
            min_compatible_version: MIN_COMPATIBLE_VERSION,
        }))
    }

    /// Watermark notification handler (T10-C).
    /// Server-side stub: receives notification from a client-side push.
    /// Actual server-to-client push requires streaming or client polling.
    async fn notify_watermark(
        &self,
        request: Request<WatermarkNotification>,
    ) -> Result<Response<()>, Status> {
        let notification = request.into_inner();
        tracing::warn!(
            table_load = notification.table_load,
            extent_usage = notification.extent_usage,
            slab_usage = notification.slab_usage,
            exceeded_regions = ?notification.exceeded_regions,
            "Watermark threshold exceeded"
        );
        Ok(Response::new(()))
    }
}

/// Get min active_ts across all clients (for GC).
pub fn min_active_ts(clients: &std::collections::HashMap<u64, (u32, std::time::Instant)>) -> u32 {
    clients.values().map(|(ts, _)| *ts).min().unwrap_or(0)
}
