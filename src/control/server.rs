use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::engine::bootstrap::BootstrappedEngine;

// Generated proto module (adjust path based on actual tonic-build output)
pub mod proto {
    tonic::include_proto!("rdmas.control");
}

use proto::control_plane_server::{ControlPlane, ControlPlaneServer};
use proto::*;

/// The gRPC control plane server.
pub struct ControlServer {
    engine: Arc<RwLock<BootstrappedEngine>>,
    generation: u64,
    // Client registry: client_id → (active_ts, last_heartbeat)
    clients: Arc<RwLock<std::collections::HashMap<u64, (u32, std::time::Instant)>>>,
}

impl ControlServer {
    pub fn new(engine: BootstrappedEngine) -> Self {
        Self {
            engine: Arc::new(RwLock::new(engine)),
            generation: 1,
            clients: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
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
                    vaddr: 0,  // Set by actual MR registration in distributed mode
                    rkey: 0,   // Set by actual MR registration
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
                    vaddr: 0,
                    rkey: 0,
                    size: 0,  // Free list size TBD
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
}

/// Get min active_ts across all clients (for GC).
pub fn min_active_ts(clients: &std::collections::HashMap<u64, (u32, std::time::Instant)>) -> u32 {
    clients.values().map(|(ts, _)| *ts).min().unwrap_or(0)
}
