//! Client-side one-sided RDMA operations (T3-B, T3-C, T3-D).
//!
//! In Wave 3, these replace the local pointer/atomic operations from
//! the engine module with real `rdma_read`, `rdma_write`, and
//! `rdma_cas` operations against remote memory regions.

/// Read path: Inline 1-RTT, Extent 2-RTT, optimistic version check
pub mod read;

/// Write path: Cuckoo insert with RDMA CAS kick chain state machine
pub mod write;

/// Retry logic: timeout, CAS failure, version mismatch, PendingTracker
pub mod retry;

/// Client session lifecycle: connect, heartbeat, reconnect, crash recovery
pub mod session;

/// Performance optimizations: fast-path, SGE batching, poller stats (Wave 4 T4-C)
pub mod opt;
