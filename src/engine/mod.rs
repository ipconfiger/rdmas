//! Core KV engine: Cuckoo hashing + dual-mode storage + concurrency control.
//!
//! This module contains the heart of the system. It is designed to be
//! testable in pure local mode (using raw pointers to simulate remote
//! memory) before being wired to real RDMA operations in Wave 3.

/// Data layout: `HashBucket`, `ExtentHeader`, `#[repr(C, align(64))]` types
pub mod layout;

/// Cuckoo hashing: insert, lookup, delete, kick chain
pub mod cuckoo;

/// Lock-free concurrency: CAS lock + lease + version-based optimistic reads
pub mod concurrency;

/// Large object region and extent allocator (free list)
pub mod extent;

/// Server-side engine bootstrap: region layout, table init, free list init
pub mod bootstrap;

/// Epoch-based garbage collector for extent region (Wave 4 T4-B)
pub mod gc;

/// LRU eviction tracker for cache management (T10-A)
pub mod lru;

/// Fixed-size chunk allocator for vLLM KV Block alignment (T9-C)
pub mod slab;

/// Memory watermark monitoring and alerting (T10-C)
pub mod watermark;
