//! Rectifiers Director integration for reporting cache operations.
//!
//! This module is gated behind the `director` feature flag. When enabled,
//! the `DirectorIntegration` connects to a Rectifiers Director gRPC server
//! and reports store/remove operations as they complete.

#[cfg(feature = "director")]
use rdmas_director::proto::{
    director_client::DirectorClient,
    DeregisterRequest, DeregisterResponse,
    HeartbeatRequest, HeartbeatResponse,
    RegisterRequest, RegisterResponse,
    ReportRemoveRequest, ReportStoreRequest,
};

/// Parses the chunk hash from an LMCache key and returns it as a u64.
///
/// LMCache key format: `model@rank@group@chunk_hash[@salt]`
///
/// The chunk_hash is the 4th segment (0-indexed: index 3), stored as a hex
/// string. Returns `None` if the key doesn't have enough segments or the
/// hex string cannot be parsed as u64.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "director")]
/// assert_eq!(extract_chunk_hash("llama-7b@0@0@a1b2c3d4"), Some(0xa1b2c3d4u64));
/// assert_eq!(extract_chunk_hash("llama-7b@0@0@a1b2c3d4@salt"), Some(0xa1b2c3d4u64));
/// assert_eq!(extract_chunk_hash("bad_key"), None);
/// ```
#[cfg(feature = "director")]
pub fn extract_chunk_hash(key: &str) -> Option<u64> {
    let parts: Vec<&str> = key.split('@').collect();
    if parts.len() < 4 {
        return None;
    }
    // The chunk_hash is the 4th segment (index 3).
    u64::from_str_radix(parts[3], 16).ok()
}

/// Integration handle for the Rectifiers Director gRPC service.
///
/// Created when `adapter_params` in the Python constructor includes a
/// `director_addr`. Wraps a tonic client, connection parameters, and a
/// dedicated tokio runtime for fire-and-forget gRPC calls.
#[cfg(feature = "director")]
pub struct DirectorIntegration {
    /// Tonic gRPC client (cheaply cloneable).
    client: DirectorClient<tonic::transport::Channel>,
    /// Node identifier (from adapter_params).
    node_id: String,
    /// Tenant identifier (from adapter_params).
    tenant_id: String,
    /// Model name (from adapter_params).
    model_name: String,
    /// Block size in tokens (from adapter_params).
    block_size: u32,
    /// Cache salt for prefix-aware hashing.
    cache_salt: String,
    /// Dedicated tokio runtime for background gRPC calls.
    rt: tokio::runtime::Runtime,
}

#[cfg(feature = "director")]
impl DirectorIntegration {
    /// Create a new DirectorIntegration and connect to the Director server.
    ///
    /// This is a synchronous constructor — it creates a tokio runtime
    /// internally and uses `block_on` to establish the gRPC connection.
    ///
    /// # Arguments
    ///
    /// - `addr`: Director gRPC address (e.g., `localhost:50051`).
    /// - `node_id`: Unique node identifier.
    /// - `tenant_id`: Tenant identifier.
    /// - `model_name`: Model being served.
    /// - `block_size`: Cache block size in tokens.
    pub fn connect(
        addr: &str,
        node_id: &str,
        tenant_id: &str,
        model_name: &str,
        block_size: u32,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Runtime::new()?;
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?;
        let client = rt.block_on(async { DirectorClient::connect(endpoint).await })?;

        Ok(Self {
            client,
            node_id: node_id.to_string(),
            tenant_id: tenant_id.to_string(),
            model_name: model_name.to_string(),
            block_size,
            cache_salt: String::new(),
            rt,
        })
    }

    /// Return a handle to the dedicated tokio runtime.
    ///
    /// Used by the caller to `spawn` fire-and-forget gRPC tasks.
    /// Returns a cloned [`tokio::runtime::Handle`] so the caller can
    /// move the `DirectorIntegration` into a spawned task without
    /// borrow conflicts.
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }

    /// Report a batch of store operations to the Director.
    ///
    /// Extracts chunk hashes from each LMCache key using [`extract_chunk_hash`]
    /// and sends a [`ReportStoreRequest`] via gRPC.
    ///
    /// This method is `async` and should be called via `tokio::spawn` inside
    /// the Director's runtime to avoid blocking the completion drain path.
    pub async fn report_store(&self, block_hashes: &[String]) {
        let hashes: Vec<u64> = block_hashes
            .iter()
            .filter_map(|key| extract_chunk_hash(key))
            .collect();

        if hashes.is_empty() {
            return;
        }

        let request = ReportStoreRequest {
            node_id: self.node_id.clone(),
            block_hashes: hashes,
            tenant_id: self.tenant_id.clone(),
            model_name: self.model_name.clone(),
            block_size: self.block_size,
            cache_salt: self.cache_salt.clone(),
        };

        let mut client = self.client.clone();
        if let Err(e) = client.report_store(request).await {
            tracing::warn!("Director ReportStore RPC failed: {e}");
        }
    }

    /// Report a batch of remove operations to the Director.
    ///
    /// Extracts chunk hashes from each LMCache key using [`extract_chunk_hash`]
    /// and sends a [`ReportRemoveRequest`] via gRPC.
    ///
    /// This method is `async` and should be called via `tokio::spawn` inside
    /// the Director's runtime to avoid blocking the completion drain path.
    pub async fn report_remove(&self, block_hashes: &[String]) {
        let hashes: Vec<u64> = block_hashes
            .iter()
            .filter_map(|key| extract_chunk_hash(key))
            .collect();

        if hashes.is_empty() {
            return;
        }

        let request = ReportRemoveRequest {
            node_id: self.node_id.clone(),
            block_hashes: hashes,
            tenant_id: self.tenant_id.clone(),
            model_name: self.model_name.clone(),
        };

        let mut client = self.client.clone();
        if let Err(e) = client.report_remove(request).await {
            tracing::warn!("Director ReportRemove RPC failed: {e}");
        }
    }

    /// Register this worker instance with the Director.
    ///
    /// Called once during connector initialization. On failure the caller
    /// should log a warning and continue (graceful degradation).
    pub async fn register(
        &self,
        instance_id: &str,
        role: i32,
        rpc_endpoint: &str,
        dp_rank: u32,
    ) -> Result<RegisterResponse, tonic::Status> {
        let request = RegisterRequest {
            instance_id: instance_id.to_string(),
            role,
            tenant_id: self.tenant_id.clone(),
            model_name: self.model_name.clone(),
            rpc_endpoint: rpc_endpoint.to_string(),
            dp_rank,
            node_id: self.node_id.clone(),
            block_size: self.block_size,
        };
        let mut client = self.client.clone();
        client.register(request).await.map(|r| r.into_inner())
    }

    /// Send a heartbeat to the Director for this instance.
    ///
    /// Called periodically (every ~10 seconds) from a background task.
    /// Failures are best-effort — the caller logs a warning and continues.
    pub async fn heartbeat(
        &self,
        instance_id: &str,
    ) -> Result<HeartbeatResponse, tonic::Status> {
        let active_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let request = HeartbeatRequest {
            instance_id: instance_id.to_string(),
            active_ts,
        };
        let mut client = self.client.clone();
        client.heartbeat(request).await.map(|r| r.into_inner())
    }

    /// Deregister this worker instance from the Director.
    ///
    /// Called on connector close. Best-effort — failures are logged but do
    /// not block shutdown.
    pub async fn deregister(
        &self,
        instance_id: &str,
    ) -> Result<DeregisterResponse, tonic::Status> {
        let request = DeregisterRequest {
            instance_id: instance_id.to_string(),
        };
        let mut client = self.client.clone();
        client.deregister(request).await.map(|r| r.into_inner())
    }
}
