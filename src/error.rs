//! Error types for the RDMAS crate.
//!
//! Design doc §四.5: errors are categorized as retriable or terminal
//! to inform client-side retry logic.

use thiserror::Error;

/// RDMA KV operation errors
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RdmaError {
    /// Operation timed out (retriable)
    #[error("operation timed out")]
    Timeout,

    /// RDMA CAS failed — another client won the race (retriable)
    #[error("CAS compare-and-swap failed")]
    CasFailed,

    /// Optimistic read version changed — read must be retried (retriable)
    #[error("version mismatch during optimistic read")]
    VersionMismatch,

    /// Cuckoo hash table is full (not retriable)
    #[error("hash table full")]
    KvFull,

    /// Invalid key (not retriable)
    #[error("invalid key")]
    InvalidKey,

    /// Fatal hardware error (not retriable)
    #[error("hardware error: {0}")]
    HardwareError(String),

    /// Connection lost — may succeed after reconnect (retriable)
    #[error("not connected")]
    NotConnected,

    /// Protocol version mismatch between client and server (not retriable, T10-E).
    #[error("protocol version mismatch: {0}")]
    ProtocolVersionMismatch(String),

    /// Internal error (not retriable)
    #[error("internal error: {0}")]
    Internal(String),
}

impl RdmaError {
    /// Returns `true` if the operation should be retried.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::CasFailed | Self::VersionMismatch | Self::NotConnected
        )
    }
}
