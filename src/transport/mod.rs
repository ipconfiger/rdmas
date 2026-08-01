//! Abstract transport layer: unified read/write/cas interface
//! over RDMA (One-Sided) and TCP (Two-Sided) backends.

use async_trait::async_trait;
use crate::error::RdmaError;

/// Abstract transport layer for remote memory operations.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to a remote node.
    async fn connect(addr: &str) -> Result<Self, RdmaError> where Self: Sized;

    /// Read from remote memory into a local buffer.
    async fn read(&self, buf: &mut [u8], local_lkey: u32, remote_addr: u64, remote_rkey: u32) -> Result<(), RdmaError>;

    /// Write a local buffer to remote memory.
    async fn write(&self, buf: &[u8], local_lkey: u32, remote_addr: u64, remote_rkey: u32) -> Result<(), RdmaError>;

    /// Atomic compare-and-swap on remote memory.
    async fn cas(&self, compare: u64, swap: u64, local_lkey: u32, remote_addr: u64, remote_rkey: u32) -> Result<bool, RdmaError>;

    /// Whether this transport uses RDMA (for perf stats / optimization decisions).
    fn is_rdma(&self) -> bool;

    /// Human-readable transport name.
    fn name(&self) -> &'static str;
}

pub mod rdma;
pub mod tcp;

pub use rdma::RdmaTransport;
pub use tcp::TcpTransport;
