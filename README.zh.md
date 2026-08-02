# RDMAS — One-Sided RDMA Distributed KV Store

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-334%20passed-green.svg)]()

> 📖 [English Version](README.md)

基于**单边 RDMA**（One-Sided RDMA）的高性能分布式键值存储引擎。Server CPU 在数据面零参与，所有读写由 Client 通过 RDMA READ/WRITE/CAS 直接完成。可作为 [LMCache](https://github.com/LMCache/LMCache)（vLLM KV cache 库）的可配置 L2 存储引擎。

## 设计目标

| 维度 | 目标 | 实现方式 |
|------|------|---------|
| **吞吐** | ≥ 10M OPS | One-Sided RDMA, Server CPU 零参与 |
| **读延迟** | P50 < 5μs, P99 < 10μs | Cuckoo Hashing (最多 2 次探测), Inline 小值 1 RTT |
| **正确性** | 线性一致 | CAS + 版本号 + 租约过期接管 |
| **可靠性** | 节点宕机可恢复 | 异步复制 + Epoch GC |
| **安全性** | Safe Rust API | `unsafe` 收敛在 FFI 边界 |
| **LMCache 集成** | 零拷贝 L2 后端 | PyO3 `native_plugin`, memoryview 直传 `void*` |

## 设计哲学

### 五条不可违背的原则

1. **数据面零 CPU** — Server CPU 只做 MR 注册、GC 推进、控制面心跳。读写热路径不触碰 Server 内存，规避 CPU 缓存一致性深坑。

2. **静态内存布局** — 初始化期用 HugePages 一次性规划所有内存。数据面严禁 `malloc`。所有结构 `#[repr(C)]` + `Pod` + 64B 缓存行对齐。

3. **确定性读 + 双模负载** — Cuckoo Hashing 保证最多 2 次探测。小 KV（≤32B）Inline 化单 RTT；大对象（LMCache KV cache 张量）走 Extent 单次 READ。

4. **`unsafe` 最小化** — FFI 边界内使用 `unsafe`，边界外只暴露 Safe API。`Drop` trait 管理 RDMA 资源生命周期。

5. **引擎与集成解耦** — RDMA KV 引擎是通用核心，LMCache 适配是独立的 PyO3 crate。引擎不依赖 Python。

### 架构

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

## 快速开始

### 系统要求

- Linux (x86_64), kernel 5.15+
- RDMA 网卡 (Mellanox ConnectX-4+ 或 SoftRoCE)
- Rust 1.75+
- HugePages 配置 (Server 端)

### 安装依赖

```bash
# Fedora
sudo dnf install -y rdma-core-devel libibverbs-utils clang glibc-headers

# Ubuntu
sudo apt install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-utils clang libclang-dev
```

### 编译

```bash
git clone https://github.com/ipconfiger/rdmas.git
cd rdmas
cargo build --release
cargo test --release
# 预期: 211 tests passed, 0 failures
```

### 配置 HugePages（Server 端）

```bash
# 为 1M 桶哈希表 (64MB) + 大对象区预留
echo 4096 | sudo tee /proc/sys/vm/nr_hugepages
# 验证
grep Huge /proc/meminfo
```

### 配置 SoftRoCE（无硬件时开发测试用）

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev <eth0>
ibv_devices  # 应列出 rxe0
```

### 运行基准测试

```bash
# CAS 硬件验证 (最高优先级)
cargo bench --bench cas

# 引擎性能
cargo bench --bench engine
```

---

## 与 LMCache 集成

RDMAS 支持作为 [LMCache](https://github.com/LMCache/LMCache) 的可配置 L2 存储后端。LMCache 是配合 vLLM 的 LLM KV cache 库，存储的是模型 KV cache 张量的原始字节。

### 编译 Connector

```bash
# 需要 Python 3.10+ 开发头文件
cd crates/lmcache-connector
cargo build --release
# 产物: target/release/liblmcache_rdma_connector.so
```

### LMCache 配置

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

### 数据流

```
vLLM decode/prefill
  → LMCache L1 (CPU/GPU) miss
  → L2 查询
  → RDMANativeConnector.submit_batch_get(keys, memoryviews)
  → Rust worker: RDMA READ(Server Large Object Region)
  → eventfd 通知
  → drain_completions()
  → memoryview 被填入 KV cache 字节
  → LMCache 返回 vLLM，零 CPU 拷贝
```

### Key 映射

LMCache 的 `ObjectKey` 序列化为字符串：
```
<model_name>@<kv_rank:08x>@<object_group_id hex>@<chunk_hash hex>
# 例: llama-7b@0000000c@0@a1b2c3d4
```

映射到 RDMAS 引擎：
- 对该字符串求 64-bit XXH64 哈希 → Cuckoo 桶的 `key_hash`
- 16B digest 存入 `key_or_digest` 用于冲突校验
- Value 走 Extent 模式，单次 RDMA READ 取整块张量

---

## 项目结构

```
rdmas/
├── crates/
│   ├── ibverbs-sys/          # libibverbs FFI 绑定 (bindgen 0.70)
│   └── lmcache-connector/    # LMCache L2 PyO3 cdylib
├── src/
│   ├── rdma/                 # Safe RDMA 封装 (Context/PD/MR/CQ/QP)
│   ├── mem/                  # HugePages 分配器
│   ├── runtime/              # Async RDMA 运行时 (busy-poll + oneshot)
│   ├── engine/               # Cuckoo + Concurrency + Extent + GC
│   ├── client/               # 分布式读/写/重试/会话
│   └── control/              # gRPC 控制面 (Server/Client/Replication)
├── benches/
│   ├── cas/                  # RDMA CAS 硬件验证
│   └── engine/               # 引擎性能基准
├── proto/                    # gRPC Protocol Buffers
├── docs/                     # 设计文档 + 执行计划
└── tests/                    # 集成测试
```

## 文档

| 文档 | 说明 |
|------|------|
| [设计方案 v3](docs/Rust-RDMA.md) | 534 行完整技术设计，经 Oracle 交叉审计 |
| [生产部署指南](docs/deployment.md) | 生产环境部署：硬件、HugePages、PFC/ECN、Docker、故障排查 |
| [开发执行计划](docs/开发执行计划.md) | 6 波次轨道 + Gate 门禁，242 行 |
| [Wave 7 硬件实测](docs/Wave7-硬件实测计划.md) | RDMA 实机验证 3 Phase 7 天计划 |
| [进度报告](docs/进度报告.md) | 当前完成状态 + 性能基准数据 |

## 许可证

MIT OR Apache-2.0
