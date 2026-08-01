//! # RDMAS — One-Sided RDMA Distributed KV Store
//!
//! A high-performance, lock-free, distributed key-value store built on
//! one-sided RDMA operations (READ/WRITE/CAS). Server CPU participates
//! only in control-plane operations; the data plane runs entirely on
//! the client via RDMA verbs.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │  Control Plane (gRPC/Tonic)                         │
//! │  • Node discovery  • MR metadata distribution      │
//! │  • Heartbeat       • Fault detection               │
//! └─────────────────────────────────────────────────────┘
//! ┌─────────────────────────────────────────────────────┐
//! │  Data Plane (One-Sided RDMA)                        │
//! │  • RDMA READ/WRITE/CAS  • Cuckoo Hashing           │
//! │  • Dual-mode: Inline (<32B) + Extent (large obj)    │
//! │  • Epoch GC  • Lease-based deadlock prevention      │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! ## Module Map
//!
//! | Module    | Wave | Purpose                              |
//! |-----------|------|--------------------------------------|
//! | `rdma`    | W1   | Safe wrappers over `rdma-sys` FFI    |
//! | `mem`     | W1   | HugePages allocator + MR registration |
//! | `runtime` | W1   | Async runtime: poll thread + channel  |
//! | `engine`  | W2   | Cuckoo hash + dual-mode + concurrency |
//! | `client`  | W3   | One-sided read/write/cas client ops   |
//! | `control` | W3   | gRPC control plane server             |
//!
//! # Safety
//!
//! `unsafe` blocks are contained within `rdma` and `runtime` modules
//! (FFI boundary). All other modules expose only safe Rust APIs.
//! Resources are managed via RAII (`Drop` implementations).

pub mod client;
pub mod control;
pub mod engine;
pub mod mem;
pub mod rdma;
pub mod runtime;
pub mod transport;

/// Shared error types for the entire crate.
pub mod error;

/// Public API trait: stable contract between engine core and
/// external consumers (e.g., LMCache connector).
pub mod api;
