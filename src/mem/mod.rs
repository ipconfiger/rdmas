//! HugePages-backed memory allocator (T1-B).
//!
//! Allocates large contiguous memory regions via `mmap` with
//! `MAP_HUGETLB`. Intended for the hash table, large object region,
//! and free list region.
//!
//! # Design Constraints
//!
//! - All allocation happens at initialization time (no dynamic `malloc`
//!   on the data plane hot path)
//! - All pages are pre-faulted and locked via `mlock`
//! - Regions are registered with RDMA via `ibv_reg_mr` for direct access
//! - RAII cleanup: `Drop` releases `munmap` + `ibv_dereg_mr`

/// HugePage-backed memory region with RDMA registration
pub mod region;
