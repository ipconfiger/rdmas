# RDMAS — One-Sided RDMA Distributed KV Store

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-334%20passed-green.svg)]()

> 📖 [中文版 (Chinese)](README.zh.md)

A high-performance distributed key-value store built on **one-sided RDMA** (READ/WRITE/CAS). The server CPU is completely absent from the data plane — all reads and writes are performed directly by clients via RDMA verbs. Designed as a configurable L2 storage backend for [LMCache](https://github.com/LMCache/LMCache) (the vLLM KV cache library).

## Design Goals

| Dimension | Target | Approach |
|-----------|--------|----------|
| **Throughput** | ≥ 10M OPS | One-Sided RDMA, server CPU zero participation |
| **Read Latency** | P50 < 5μs, P99 < 10μs | Cuckoo Hashing (≤ 2 probes), Inline small values 1 RTT |
| **Correctness** | Linearizability | CAS + version number + lease takeover |
| **Reliability** | Recoverable from node failure | Async replication + Epoch GC |
| **Safety** | Safe Rust API | `unsafe` contained at FFI boundary |
| **LMCache Integration** | Zero-copy L2 backend | PyO3 `native_plugin`, memoryview → `void*` direct transfer |

## Design Philosophy

### Five Inviolable Principles

1. **Zero CPU on the data plane** — the server CPU only handles MR registration, GC advancement, and control-plane heartbeats. The read/write hot path never touches server memory, avoiding CPU cache coherence pitfalls.

2. **Static memory layout** — all memory is allocated upfront during initialization using HugePages. The data plane strictly forbids `malloc`. All structures use `#[repr(C)]` + `Pod` + 64B cache-line alignment.

3. **Deterministic reads + dual-mode storage** — Cuckoo Hashing guarantees at most 2 probes. Small KV pairs (≤ 32 B) are stored inline for 1 RTT. Large objects (LMCache KV cache tensors) use the Extent region for a single READ.

4. **Minimal `unsafe`** — `unsafe` is confined within the FFI boundary. The public API exposes only safe Rust. The `Drop` trait manages RDMA resource lifetimes.

5. **Engine decoupled from integration** — the RDMA KV engine is a general-purpose core. The LMCache adapter is a standalone PyO3 crate. The engine has no Python dependency.

### Architecture

```
                                 RoCEv2 (25/100 Gbps)
┌──────────────────────┐                              ┌──────────────────────┐
│   Client             │   RDMA READ/WRITE/CAS         │   Server             │
│  ┌────────────────┐  │◄─────────────────────────────│  ┌────────────────┐  │
│  │ ClientSession  │  │                              │  │ ControlPlane   │  │
│  │ gRPC + QP      │  │   Control Plane (gRPC)        │  │ gRPC Server    │  │
│  └────────────────┘  │◄─────────────────────────────│  └────────────────┘  │
│                      │                              │                      │
│  Data Plane:         │                              │  HugePage Regions:   │
│  • Cuckoo Read       │                              │  • Hash Table (64MB) │
│  • CAS Write + Kick  │                              │  • Large Obj (1GB)  │
│  • Extent READ       │                              │  • Free List        │
└──────────────────────┘                              └──────────────────────┘
```

---

## Quick Start

### System Requirements

- Linux (x86_64), kernel 5.15+
- RDMA NIC (Mellanox ConnectX-4+ or SoftRoCE)
- Rust 1.75+
- HugePages configured (server side)

### Install Dependencies

```bash
# Fedora
sudo dnf install -y rdma-core-devel libibverbs-utils clang glibc-headers

# Ubuntu
sudo apt install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-utils clang libclang-dev
```

### Build

```bash
git clone https://github.com/ipconfiger/rdmas.git
cd rdmas
cargo build --release
cargo test --release
# Expected: 334 tests passed, 0 failures
```

### Configure HugePages (Server Side)

```bash
# Reserve space for a 1M-bucket hash table (64 MB) + large object region
echo 4096 | sudo tee /proc/sys/vm/nr_hugepages
# Verify
grep Huge /proc/meminfo
```

### Configure SoftRoCE (Development Without Hardware)

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev <eth0>
ibv_devices  # should list rxe0
```

### Run Benchmarks

```bash
# CAS hardware verification (highest priority)
cargo bench --bench cas

# Engine performance
cargo bench --bench engine
```

---

## LMCache Integration

RDMAS supports serving as a configurable L2 storage backend for [LMCache](https://github.com/LMCache/LMCache). LMCache is the LLM KV cache library that ships with vLLM, storing raw model KV cache tensors.

### Build the Connector

```bash
# Requires Python 3.10+ development headers
cd crates/lmcache-connector
cargo build --release
# Output: target/release/liblmcache_rdma_connector.so
```

### LMCache Configuration

```json
--l2-adapter '{
  "type": "native_plugin",
  "module_path": "lmcache_rdma_connector",
  "class_name": "RDMANativeConnector",
  "adapter_params": {
    "device": "mlx5_0",
    "server": "10.0.0.1:9400",
    "num_workers": 4,
    "batch_chunk_num_bytes": 16777216
  },
  "eviction": {
    "eviction_policy": "LRU",
    "trigger_watermark": 0.8
  },
  "serde": {
    "type": "fp8",
    "fp8_dtype": "float8_e4m3fn"
  }
}'
```

### Data Flow

```
vLLM decode/prefill
  → LMCache L1 (CPU/GPU) miss
  → L2 lookup
  → RDMANativeConnector.submit_batch_get(keys, memoryviews)
  → Rust worker: RDMA READ (Server Large Object Region)
  → eventfd notification
  → drain_completions()
  → memoryview filled with KV cache bytes
  → LMCache returns to vLLM, zero CPU copy
```

### Key Mapping

LMCache `ObjectKey` is serialized as a string:

```
<model_name>@<kv_rank:08x>@<object_group_id hex>@<chunk_hash hex>
# Example: llama-7b@0000000c@0@a1b2c3d4
```

Mapped to the RDMAS engine:
- Compute a 64-bit XXH64 hash of the string → Cuckoo bucket `key_hash`
- Store a 16 B digest in `key_or_digest` for collision verification
- Values use Extent mode, retrieved with a single RDMA READ for the full tensor

---

## Project Structure

```
rdmas/
├── crates/
│   ├── ibverbs-sys/          # libibverbs FFI bindings (bindgen 0.70)
│   └── lmcache-connector/    # LMCache L2 PyO3 cdylib
├── src/
│   ├── rdma/                 # Safe RDMA wrappers (Context/PD/MR/CQ/QP)
│   ├── mem/                  # HugePages allocator
│   ├── runtime/              # Async RDMA runtime (busy-poll + oneshot)
│   ├── engine/               # Cuckoo + Concurrency + Extent + GC
│   ├── client/               # Distributed read/write/retry/session
│   └── control/              # gRPC control plane (Server/Client/Replication)
├── benches/
│   ├── cas/                  # RDMA CAS hardware verification
│   └── engine/               # Engine performance benchmarks
├── proto/                    # gRPC Protocol Buffers
├── docs/                     # Design documents + execution plans
└── tests/                    # Integration tests
```

## Documentation

| Document | Description |
|----------|-------------|
| [Design Spec v3](docs/Rust-RDMA.md) | 534-line complete technical design, cross-audited by Oracle |
| [Production Deployment Guide](docs/deployment.md) | Production deployment: hardware, HugePages, PFC/ECN, Docker, troubleshooting |
| [Development Execution Plan](docs/开发执行计划.md) | 6-wave track + gate checklist, 242 lines |
| [Improvement Plan v2](docs/改进开发计划-Wave8-11-v2.md) | Waves 8–11 improvement roadmap (Oracle-reviewed), 15 tracks |
| [Wave 7 Hardware Validation](docs/Wave7-硬件实测计划.md) | RDMA real-machine validation, 3-phase 7-day plan |
| [Operations Manual](docs/operations.md) | Monitoring, alerting, troubleshooting, tuning, day-to-day ops |
| [Extent Protocol Design](docs/extent-protocol.md) | CAS bump allocator distributed extent protocol |
| [Security Design](docs/security.md) | Threat model, mTLS implementation, latency budget |

## License

MIT OR Apache-2.0
