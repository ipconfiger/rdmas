//! Control plane: gRPC service for node discovery, MR metadata broadcast,
//! heartbeat, and fault detection (T3-A).
//!
//! Uses `tonic` for gRPC. All control-plane operations are low-frequency
//! and never touch data-plane hot-path memory.

/// gRPC server: register MRs, broadcast to clients, heartbeat
pub mod server;

/// gRPC client: receive MR metadata, maintain connection
pub mod client;

/// Asynchronous replication: backup node via RDMA WRITE (T4-D)
pub mod replication;

/// Multi-tenant namespace isolation via hash-seed mixing (T11-C)
pub mod tenant;

pub use tenant::TenantNamespace;
