use tonic::transport::Channel;
use tonic::Request;

// Import generated proto
use super::server::proto::control_plane_client::ControlPlaneClient;
use super::server::proto::*;

/// Client for the control plane gRPC service.
pub struct ControlClient {
    inner: ControlPlaneClient<Channel>,
}

impl ControlClient {
    /// Connect to a control plane server.
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))?;
        let channel = endpoint.connect().await?;

        let inner = ControlPlaneClient::new(channel);
        Ok(Self { inner })
    }

    /// Discover server metadata: get all MR region info.
    pub async fn discover(&mut self) -> Result<ServerMetadata, tonic::Status> {
        let response = self.inner.discover(Request::new(DiscoverRequest {})).await?;
        response.into_inner().metadata.ok_or_else(|| {
            tonic::Status::internal("no metadata in discover response")
        })
    }

    /// Send heartbeat with activity timestamp.
    pub async fn heartbeat(
        &mut self,
        client_id: u64,
        active_ts: u32,
    ) -> Result<HeartbeatResponse, tonic::Status> {
        let response = self.inner.heartbeat(Request::new(HeartbeatRequest {
            client_id,
            active_ts,
        })).await?;
        Ok(response.into_inner())
    }

    /// Notify server of client departure.
    pub async fn deregister(&mut self, client_id: u64) -> Result<(), tonic::Status> {
        self.inner.deregister(Request::new(DeregisterRequest {
            client_id,
        })).await?;
        Ok(())
    }
}
