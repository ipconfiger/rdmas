//! Async RDMA runtime (T1-C).
//!
//! Implements the busy-poll polling thread + lock-free channel pattern
//! (design doc §四.2, 方案 A):
//!
//! 1. A dedicated thread (core-affined) busy-polls Completion Queues
//! 2. Completed Work Completions are dispatched via `crossbeam` channels
//! 3. `tokio::sync::oneshot` channels wake awaiting `Future`s
//!
//! # Safety
//!
//! The polling thread never touches Tokio worker threads. CQ polling
//! is inherently blocking — it must NOT run on async executors.

/// Polling thread: core-affine, busy-poll CQ, dispatch completions
pub mod poller;

/// Async RDMA operation primitives: `async fn rdma_read/write/cas`
pub mod ops;
