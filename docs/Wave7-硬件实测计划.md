# Wave 7 — RDMA 硬件实机验证与性能对标

> 前置条件：获得两台配备 RDMA 网卡（Mellanox ConnectX-4/5/6 或类似）的物理机，
> 或一台双端口 RDMA 网卡机器。本波次将本地仿真引擎**升级为真实分布式数据面**，
> 完成 One-Sided RDMA 全链路验证。

---

## 〇、硬件与系统要求

### 最低配置
| 项目 | 要求 |
|------|------|
| **网卡** | Mellanox ConnectX-4 Lx 或更高，支持 RoCEv2 |
| **网卡数量** | 至少 2 台机器各有 1 块，或 1 台机器有 2 个端口 |
| **链路** | 25Gbps 或更高，直连或通过交换机 |
| **CPU** | x86_64，支持 HugePages（2MB） |
| **OS** | Ubuntu 22.04+ / RHEL 9+ / Fedora 40+ |
| **内核** | 5.15+（支持 RDMA verbs） |
| **内存** | Server ≥ 16GB，Client ≥ 8GB |

### 软件依赖（自动安装脚本见 §A）
```
rdma-core >= 50.0        # libibverbs, librdmacm
MLNX_OFED (可选)          # 如使用 Mellanox 网卡
Rust 1.75+               # 编译本项目
libclang-dev              # bindgen 生成 FFI
```

### 系统配置
```bash
# 1. 配置 HugePages（Server 端）
echo 4096 > /proc/sys/vm/nr_hugepages    # 4096 × 2MB = 8GB

# 2. 确认网卡支持 RoCEv2
ibv_devinfo <device> | grep -i roce

# 3. 配置 RoCE Trust 模式（Mellanox）
# 略 — 见 §A 详细脚本
```

---

## 一、验证前检查清单（Day 1）

### 1.1 硬件自检
```bash
# 检查 RDMA 设备
ibv_devices
# 预期：列出 mlx5_0 或类似设备

# 检查端口状态
ibv_devinfo <device>
# 预期：state: PORT_ACTIVE, link_layer: Ethernet

# 测试连通性（两台机器间）
# Server:
ib_read_bw -d <device> -F
# Client:
ib_read_bw -d <device> -F <server_ip>
# 预期：输出带宽结果，无错误
```

### 1.2 项目代码编译
```bash
cd rdmas
cargo build --release
cargo test --release
# 预期：所有 220 测试通过
```

### 1.3 CAS 硬件验证（Gate-1 最终裁决）
```bash
cargo bench --bench cas
# 预期：
#   rdma_ops/cas_latency: < 2us
#   rdma_ops/read_latency: < 2us
#   rdma_ops/write_latency: < 2us
# 判定标准（设计文档 Gate-1）：
#   ✅ CAS 延迟 ≤ 2× READ 延迟 → One-Sided 路线确认
#   ❌ CAS 延迟 > 2× READ → 降级 Two-Sided + Server CPU 介入
```

---

## 二、部署架构

```
┌─────────────────────┐          ┌──────────────────────┐
│   Client Machine    │          │    Server Machine     │
│                     │          │                       │
│  ┌───────────────┐  │  RoCEv2  │  ┌─────────────────┐  │
│  │ Client Session │──│─────────│──│  Control Plane   │  │
│  │  (gRPC+RDMA)   │  │  25Gbps  │  │  (gRPC Server)   │  │
│  │               │  │          │  │  HugePage Region  │  │
│  │  RDMA READ ────│──│─────────│──│  Hash Table (64MB)│  │
│  │  RDMA WRITE ───│──│─────────│──│  Large Obj (1GB)  │  │
│  │  RDMA CAS ─────│──│─────────│──│  Free List Region │  │
│  └───────────────┘  │          │  └─────────────────┘  │
│                     │          │                       │
└─────────────────────┘          └──────────────────────┘
```

---

## 三、执行阶段（7 天，3 个 Phase）

### Phase 1 — 环境搭建与单机验证（Day 1–2）

| 任务 | 描述 | 验证 |
|------|------|------|
| **P1.1** 系统配置 | 安装 rdma-core，配置 HugePages，打开 RoCE | `ibv_devinfo` 输出正常 |
| **P1.2** 网卡直连 | 两台机器直连（或同交换机），配置 IP + 子网 | `ping` 通 |
| **P1.3** 带宽测试 | `ib_read_bw` 验证 RDMA 通路 | 达到 ~23Gbps (25G 网卡) |
| **P1.4** CAS 基准 | 本项目 CAS bench 自环测试 | 产出 latency 数据 |
| **P1.5** MR 注册 | Server 注册 HugePage MR，验证 lkey/rkey | MR 创建成功 |

### Phase 2 — 分布式数据面打通（Day 3–5）

| 任务 | 描述 | 涉及代码 |
|------|------|---------|
| **P2.1** 启动 Server | Server 分配 HugePages → 注册 MR → 启动 gRPC | `src/control/server.rs` |
| **P2.2** Client 连接 | Client discover → 建立 QP → INIT→RTR→RTS | `src/client/session.rs` |
| **P2.3** One-Sided READ | Client RDMA READ 读取 Server 桶 | `src/client/read.rs` |
| **P2.4** One-Sided CAS | Client RDMA CAS 写入 Server 桶 | `src/client/write.rs` |
| **P2.5** Kick Chain | 多 Client 并发 Cuckoo 踢出链 | `src/client/write.rs` |
| **P2.6** Extent READ | Client 读大对象（1KB~1MB） | `src/client/read.rs` |

#### P2 代码适配（将本地模拟替换为 RDMA 操作）

当前本地仿真中，`ClientReader::get` 的签名是：
```rust
pub fn get(key, buckets: &[HashBucket], large_objects, bucket_count)
```

需增加分布式版本：
```rust
pub async fn get_remote(
    key: &HashedKey,
    runtime: &RdmaRuntime,     // T1-C 的异步 RDMA 运行时
    hash_table_addr: u64,       // Server 哈希表起始 vaddr (从 MR 元数据获取)
    hash_table_rkey: u32,       // Server 哈希表 rkey
    large_obj_addr: u64,        // Server 大对象区 vaddr
    large_obj_rkey: u32,
    bucket_count: u64,
) -> Result<Option<Vec<u8>>, RdmaError>
```

核心改动：`buckets[idx]` 的读取 → `runtime.rdma_read(&mut bucket, hash_table_addr + idx*64, hash_table_rkey).await`

### Phase 3 — 性能对标与报告（Day 6–7）

| 任务 | 描述 | 目标 |
|------|------|------|
| **P3.1** 读延迟 | 测量 One-Sided READ P50/P99 | P50 < 5μs |
| **P3.2** 写延迟 | 测量 CAS 写 + Kick Chain 延迟 | P99 < 10μs |
| **P3.3** 吞吐量 | 并发 Client 最大 OPS 扫频 | ≥ 10M OPS (Inline) |
| **P3.4** Extent 吞吐 | 大对象读写带宽 | ≥ 90% 线速 |
| **P3.5** Server CPU | 数据面 CPU 占用 (perf stat) | = 0%（无中断/轮询） |
| **P3.6** 对标 | vs Redis/Memcached 通用 KV | 延迟对比报告 |
| **P3.7** 报告 | 最终性能报告 | 瓶颈分析 + 优化方向 |

---

## 四、关键代码适配清单

当前代码是**本地仿真模式**。实机测试需完成以下适配：

### 4.1 替换 ClientReader / ClientWriter 的远端路径

| 文件 | 当前（本地） | 需要（分布式） |
|------|-------------|---------------|
| `src/client/read.rs` | `buckets[idx]` 直接数组索引 | `runtime.rdma_read(addr + idx*64, rkey).await` |
| `src/client/write.rs` | `buckets[idx] = ...` 直接赋值 | `runtime.rdma_cas(addr + idx*64, old, new, rkey).await` |
| `src/client/session.rs` | 本地创建 `CuckooTable` | 从 gRPC 获取 `ServerMetadata`，含 vaddr/rkey |

### 4.2 Server 端启动流程

```rust
// 伪代码：server main
fn main() {
    // 1. 分配 HugePage 区域
    let pd = ProtectionDomain::allocate(&context)?;
    let hash_region = HugePageRegion::allocate(64 * 1024 * 1024, &pd)?; // 1M 桶 × 64B
    let large_region = HugePageRegion::allocate(1024 * 1024 * 1024, &pd)?; // 1GB
    
    // 2. 初始化 Cuckoo 表（零化所有桶）
    let engine = BootstrappedEngine::bootstrap(1 << 20, 1024 * 1024 * 1024, 16);
    
    // 3. 注册 MR → 获取 vaddr, rkey
    let hash_vaddr = hash_region.addr();
    let hash_rkey = hash_region.rkey().unwrap();
    let large_vaddr = large_region.addr();
    let large_rkey = large_region.rkey().unwrap();
    
    // 4. 启动 gRPC 控制面，广播 MR 元数据
    ControlServer::serve(
        hash_vaddr, hash_rkey,
        large_vaddr, large_rkey,
        1 << 20,  // bucket_count
    ).await?;
    
    // 5. 等待 Client 连接（Server CPU 不触碰数据面）
    // 后台线程：GC + 心跳 + 复制
}
```

### 4.3 QP 建立（两台机器）

```
Server                                    Client
─────────────────────────────────────────────────────
1. 创建 QP (RESET)
2. INIT (pkey, port, access_flags)  ──→  
3. 等待 Client RTR               ←──   3. create QP → INIT
                                       4. RTR (remote_qpn, remote_lid, GID)
                                       5. RTS (sq_psn, timeout)
6. 收到 Client RTS 通知
7. RTR (remote_qpn, remote_lid, GID)  ──→
8. RTS (sq_psn, timeout)
                                       
   ✅ QP 就绪，One-Sided 操作开始
```

---

## 五、5 个关键验证场景

### 场景 1：单 Key 基本读写
```
1. Client insert("hello", [1,2,3,4]) via RDMA CAS
2. Client get("hello") via RDMA READ → 返回 [1,2,3,4]
3. 验证：数据一致，Server CPU zero
```

### 场景 2：Cuckoo Kick Chain
```
1. 用小表（64 桶）连续 insert 200 个 Key
2. 观察 kick chain 触发（MAX_KICK=16）
3. 验证：所有 Key 可查，TableFull 至少出现一次
4. 验证：踢出过程中无数据损坏
```

### 场景 3：并发 Client 一致性
```
1. 3 个 Client 同时对同一 Key 进行 CAS 写入
2. 每个 Client 写不同 value
3. 验证：最终所有 Client 读到同一个 value（线性一致）
4. 验证：version 单调递增
```

### 场景 4：大对象存取（LMCache 预演）
```
1. Client insert_extent("tensor_01", 1MB_data) via RDMA WRITE to LargeObj
2. Client get("tensor_01") → 单次 RDMA READ 1MB
3. 验证：数据完整，延迟受带宽限制（1MB / 25Gbps ≈ 320μs 理论）
```

### 场景 5：Server CPU 数据面零参与
```
1. Client 持续 100K OPS 读写
2. Server 端运行: perf stat -p <server_pid>
3. 验证：数据面函数（ClientReader/ClientWriter 涉及的 Server 内存地址）
   的 CPU 采样计数 = 0
4. 控制面/GC 线程的 CPU 不计入
```

---

## 六、成功标准（Gate-7）

| 指标 | Gate 条件 | 测量工具 |
|------|----------|---------|
| **CAS 性能** | CAS latency ≤ 2× READ latency | `cargo bench --bench cas` |
| **读延迟 P50** | < 5μs | `criterion` + 手动测量 |
| **读延迟 P99** | < 10μs | histogram 分析 |
| **Inline 吞吐** | ≥ 10M OPS (纯读) | 自定义 bench |
| **Extent 带宽** | ≥ 90% 线速 (25G → 2.8GB/s) | `ib_read_bw` + 自定义 |
| **Server CPU** | 数据面 = 0% | `perf stat` |
| **线性一致性** | 多 Client 并发无损坏 | 场景 3 验证 |
| **稳定性** | 1 小时持续运行无泄漏无崩溃 | 脚本循环 |

---

## 七、风险与降级预案

| 风险 | 概率 | 应对 |
|------|------|------|
| **CAS 硬件不支持** | 低（CX-4+ 均支持） | 降级 Two-Sided Send/Recv，Server CPU 介入锁管理 |
| **RoCE 配置复杂（PFC/ECN）** | 中 | 使用直连绕过交换机 PFC 问题；备选 DCQCN |
| **QP 对端握手失败** | 中 | 详细日志输出 `ibv_modify_qp` 错误码；备选 `rdma_cm` 自动握手 |
| **MR 注册失败（HugePages 不足）** | 低 | 启动时检测 + 清晰错误提示 + 回退普通页 |
| **尾延迟 jitter 超标** | 中 | busy-poll 绑核 + 隔离 CPU + `perf sched` 分析唤醒源 |

---

## 八、交付物

| 阶段 | 交付物 |
|------|--------|
| Phase 1 | CAS bench 报告（硬件 vs SoftRoCE 对比） |
| Phase 2 | 分布式 One-Sided READ/WRITE/CAS 全链路打通 |
| Phase 3 | 性能报告：P50/P99/P999 延迟、OPS、带宽、CPU |
| 最终 | `WAVE7_REPORT.md` 含原始数据 + 图表 + 瓶颈分析 |

---

## 附录 A：环境自动化搭建脚本

```bash
#!/bin/bash
# rdmas_wave7_setup.sh
# 在 Server 和 Client 机器上分别执行

set -euo pipefail

echo "=== Step 1: Install RDMA dependencies ==="
sudo dnf install -y rdma-core-devel libibverbs-utils librdmacm-utils || \
sudo apt install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-utils

echo "=== Step 2: Configure HugePages ==="
echo 4096 | sudo tee /proc/sys/vm/nr_hugepages
grep Huge /proc/meminfo

echo "=== Step 3: Load RDMA modules ==="
sudo modprobe rdma_ucm
sudo modprobe ib_uverbs
# Mellanox specific
sudo modprobe mlx5_core 2>/dev/null || true
sudo modprobe mlx5_ib 2>/dev/null || true

echo "=== Step 4: Check devices ==="
ibv_devices
ibv_devinfo

echo "=== Step 5: Verify Rust ==="
rustc --version
cargo --version

echo "=== Done. Next: cargo build --release ==="
```

## 附录 B：两台机器 IP 配置示例

```
Server (10.0.0.1):
  sudo ip addr add 10.0.0.1/24 dev <rdma_interface>

Client (10.0.0.2):
  sudo ip addr add 10.0.0.2/24 dev <rdma_interface>
```

---

> **本计划设计为"插电即用"**：获得 RDMA 硬件后，按 §一 检查清单验证环境，
> 按 §三 三阶段推进，§五 场景验证，§六 Gate 判定通过即完成。
