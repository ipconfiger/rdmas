//! Abstract transport layer: unified read/write/cas interface
//! over RDMA (One-Sided) and TCP (Two-Sided) backends.

use crate::error::RdmaError;
use async_trait::async_trait;

/// Abstract transport layer for remote memory operations.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a remote node.
    async fn connect(addr: &str) -> Result<Self, RdmaError>
    where
        Self: Sized;

    /// Read from remote memory into a local buffer.
    async fn read(
        &self,
        buf: &mut [u8],
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<(), RdmaError>;

    /// Write a local buffer to remote memory.
    async fn write(
        &self,
        buf: &[u8],
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<(), RdmaError>;

    /// Atomic compare-and-swap on remote memory.
    async fn cas(
        &self,
        compare: u64,
        swap: u64,
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<bool, RdmaError>;

    /// Whether this transport uses RDMA (for perf stats / optimization decisions).
    fn is_rdma(&self) -> bool;

    /// Human-readable transport name.
    fn name(&self) -> &'static str;
}

/// Transport that supports reconnection after failure.
///
/// When the underlying connection enters a failure state (e.g., QP ERROR
/// for RDMA, or TCP disconnect), the caller can invoke `reconnect()` to
/// obtain a fresh transport instance without restarting the application.
///
/// # Usage with QpGuard
///
/// [`QpGuard`](crate::rdma::QpGuard) detects QP ERROR state on each
/// operation. When it returns an error, the retry layer (e.g.,
/// [`retry_rdma_op`](crate::client::retry::retry_rdma_op)) catches it
/// and the caller invokes `reconnect()` to create a new transport.
#[async_trait]
pub trait ReconnectableTransport: Transport {
    /// Attempt to reconnect after a connection failure.
    ///
    /// Returns a fresh transport instance or an error. The old transport
    /// instance should be discarded after a successful reconnect.
    async fn reconnect(&self, server_addr: &str) -> Result<Box<dyn Transport>, RdmaError>;
}

pub mod gdr;
pub mod rdma;
pub mod tcp;

pub use rdma::RdmaTransport;
pub use tcp::TcpTransport;

#[cfg(feature = "gdr")]
pub use gdr::GdrTransport;
