//! Safe Rust wrappers over `rdma-sys` FFI bindings.
//!
//! This module provides RAII-managed, safe abstractions for:
//!
//! - [`Context`] — opened RDMA device context
//! - [`ProtectionDomain`] — memory protection domain
//! - [`MemoryRegion`] — registered memory region (auto-deregistered on drop)
//! - [`QueuePair`] — send/receive queue pair (auto-destroyed on drop)
//! - [`CompletionQueue`] — work completion queue
//!
//! All `unsafe` FFI calls are confined to this module. The public API
//! is entirely safe Rust.
//!
//! # Design Constraint
//!
//! - QP state machine (INIT→RTR→RTS) is managed here (T1-A extended scope)
//! - Connection teardown and reconnection logic also lives here

/// RDMA device context (ibv_context wrapper)
pub mod context;
/// Protection domain (ibv_pd wrapper)
pub mod pd;
/// Memory region (ibv_mr wrapper) — crucial for zero-copy RDMA
pub mod mr;
/// Queue pair — the workhorse for posting RDMA operations
pub mod qp;
/// QP error state recovery — QpGuard health-checking wrapper
pub mod qp_recovery;
/// Completion queue — polling for completed work requests
pub mod cq;

// Re-export the key types for convenience
pub use context::Context;
pub use context::DeviceAttr;
pub use context::PortAttr;
pub use pd::ProtectionDomain;
pub use mr::MemoryRegion;
pub use cq::{CompletionQueue, CqEventChannel, WorkCompletion};
pub use qp::{QueuePair, RecvWorkRequest, ScatterGatherEntry, SendWorkRequest, SendWrOpcode};
pub use qp_recovery::QpGuard;
