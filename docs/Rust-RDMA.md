# 基于 One-Sided RDMA 的分布式 KV 存储：Rust 工程化方案 (v3)

> v3 在 v2（参考 HERD/DrTM，吸收缓存行对齐、Cuckoo 写路径状态机、异步与 CQ 整合、去中心化 GC、容错与租约）的基础上，新增**双模内存布局（Inline + Extent）**与**LMCache L2 存储适配层**，使本引擎可作为 [LMCache](https://github.com/ipconfiger/LMCache)（配合 vLLM 的 LLM KV cache 库）的可配置 L2 存储引擎接入。

---

## 〇、设计目标与核心原则

| 维度 | 目标 | 核心约束 |
|------|------|----------|
| **吞吐** | 单节点 10M+ OPS | Server CPU 在数据面读/写热路径**零参与**；GC、异步复制、控制面心跳属于后台任务，低频执行，不触碰读写热路径 |
| **延迟** | 读 P50 < 5μs，P99 < 10μs | 读路径最多 2 RTT，小 KV 降至 1 RTT |
| **正确性** | 无锁、无死锁、线性一致 | CAS + 版本号 + 租约 |
| **可靠性** | 节点宕机/网络分区可恢复 | 异步复制 + 租约过期接管 |
| **安全性** | `unsafe` 收敛在最小 FFI 边界 | 对外暴露纯 Safe API |
| **接口/集成** | 可配置为 LMCache L2 后端 | PyO3 原生绑定 + `native_plugin`，零拷贝 |

**五条不可违背的原则：**

1. **数据面零 CPU**：Server CPU 只做内存注册、GC 推进、控制面心跳，绝不触碰数据面热路径内存（规避缓存一致性深坑）。
2. **静态内存布局**：数据面严禁 `malloc`，初始化期用 HugePages 一次性规划完毕，所有结构 `#[repr(C)]` + `Pod` + 缓存行对齐。
3. **确定性读路径 + 双模负载**：索引选 Cuckoo Hashing，读最多 2 次探测；小 KV Inline 化（1 RTT），大对象（如 LMCache KV cache 张量）走 Extent 单次 READ。
4. **`unsafe` 最小化**：FFI 边界内大胆 `unsafe`，边界外只暴露 Safe API，用 `Drop` trait 管理资源生命周期。
5. **引擎与集成解耦**：RDMA KV 引擎是通用核心；LMCache 适配是薄 PyO3 层，按 `native_plugin` 契约对接，引擎不依赖 Python。

---

## 一、整体架构：控制面与数据面分离（增强容错）

One-Sided RDMA 的核心：**数据面所有操作（读、写、插入、删除）由 Client 通过 RDMA READ/WRITE/CAS 直接完成，Server CPU 零参与。**

### 1. 控制面 (Control Plane, CP)
- **职责**：节点发现、心跳、故障检测、**MR 元数据分发**、**Client 活跃时间戳收集**（供 GC 用）。
- **实现**：TCP/gRPC（Tonic），低频、非热路径。
- **关键动作**：Server 启动时分配 HugePages → 注册 MR → 获取 `RKEY` + 虚拟地址 → CP 广播给所有 Client。

**MR 元数据 Protobuf 消息：**
```protobuf
enum RegionType { HASH_TABLE = 0; LARGE_OBJECT = 1; FREE_LIST = 2; }
message RegionMetadata {
  uint64 vaddr = 1;
  uint32 rkey = 2;
  uint64 size = 3;
  RegionType type = 4;
  uint64 generation = 5;  // Server 重启递增
}
message ServerMetadata {
  uint64 generation = 1;
  repeated RegionMetadata regions = 2;
  uint64 bucket_count = 3;
}
```

### 2. 数据面 (Data Plane, DP)
- **职责**：纯 KV 读写。
- **实现**：`ibv_post_send` 发送 `RDMA_READ` / `WRITE` / `CMP_SWAP` (CAS)。
- **Server 状态**：CPU 仅处理控制面消息与后台 GC/复制线程，不处理数据面网络中断。

### 3. 容错层（v1 盲区，v2 新增）

| 故障场景 | 影响 | 应对 |
|----------|------|------|
| **Client 持 CAS 锁崩溃** | Hash 桶永久死锁 | 锁字段嵌入**过期时间戳**；锁过期后其他 Client 强制接管并修复结构（见 §三.3） |
| **Server 宕机** | 内存数据全丢 | 后台线程异步 RDMA WRITE 复制到备份节点 / NVMe-oF 持久化（类 SIMYR） |
| **网络分区** | One-Sided 操作超时 | Client 侧超时重试 + 控制面仲裁切换主备 |
| **Server 重启** | 所有 Client 持有的 MR 引用失效 | Server 启动时广播新 `generation_id`；Client 检测 generation 变化后丢弃所有未完成操作，重新获取 MR 元数据并重建 QP |

---

## 二、核心数据结构设计

### 1. 内存布局与缓存行对齐（致命细节 + v3 双模）

RDMA 网卡经 PCIe DMA 写内存，以 **Cache Line（64B）** 为单位。若 `HashBucket` 仅 16B，网卡会刷新同一 64B 行上的其他数据，造成**伪共享**和缓存破坏。

**改进：所有热路径结构强制对齐到 64 字节；桶支持双模（Inline 小值 / Extent 大对象）。**

```rust
use bytemuck::{Pod, Zeroable};

/// 一个 Hash 桶 = 恰好一个 Cache Line（64B）
/// 双模式（由 lock_version 的 mode 位决定）：
///   - Inline：小值（≤32B）内联进桶，读 1 RTT（通用小 KV）
///   - Extent：桶指向 Large Object Region 的 {offset, len}，单次 RDMA READ 整块
///             （LMCache KV cache 张量等 MB 级大块）
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C, align(64))]
struct HashBucket {
    // [63..32] version | [31..8] lease_ts(24b) | [7..3] 保留 | [2..0] mode+state
    //   bit0 = locked, bit1 = tombstone, bit2 = mode (0=Inline, 1=Extent)
    //   locked 与 tombstone 互斥：同一时刻最多一个置位；可能的合法状态：
    //     0b000 = 空闲(unlocked, alive, Inline)
    //     0b001 = 已加锁+Inline | 0b010 = tombstone | 0b100 = Extent模式(空闲)
    //     0b101 = 已加锁+Extent
    lock_version: u64,
    key_hash: u64,              // key 的 64-bit 哈希；LMCache = key 字符串哈希
    key_or_digest: [u8; 16],    // Inline=内联 key；Extent=完整 key 强摘要(冲突校验)
    body: [u8; 32],             // 按 mode 复用，见下方约定
    _pad: [u8; 8],
}
// body 复用约定（零额外 READ）：
//   Inline 模式 → 内联 value: [u8; 32]
//   Extent 模式 → ExtentRef { offset: u64, length: u64 }（+16B 预留）
const _: () = assert!(std::mem::size_of::<HashBucket>() == 64);
const _: () = assert!(std::mem::align_of::<HashBucket>() == 64);

/// Extent 元数据（存在于 Large Object Region 头部，仅 GC/分配器访问，非热路径）
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct ExtentHeader {
    length: u64,
    epoch_mark: u64,     // GC 死亡时间戳
    magic: u32,          // 完整性校验
    _pad: u32,
}
```

**为何用 Extent 取代 v2 的链式 DataSlot：** LMCache 的 value 是**连续大块张量字节**，链式拆分需 N 次 RDMA READ；Extent 给出 `{offset, length}`，客户端一次 READ 取整块，延迟与块数解耦。

**内存分区（初始化期一次性 mmap HugePages 规划）：**

| Region | 用途 |
|--------|------|
| `Hash Table Region` | Cuckoo 桶数组（2 的幂大小） |
| `Large Object Region` | Extent 大对象池（LMCache KV cache 张量；extent 分配器管理） |
| `Inline Value Region` | （v1 暂不实现）超限小值溢出区；v1 中 value > 32B 一律走 Extent 大对象区 |
| `Free List Region` | 空闲 Extent/Slot 偏移链表（无锁栈） |

### 2. 索引结构：Cuckoo Hashing

选 Cuckoo 的根本原因：**读路径确定性**（最多探测 2 桶）。Client 最多 2 次 `RDMA_READ` 即可判定 Key 存在性。

- **桶数量**：`BUCKET_COUNT = next_power_of_two(expected_max_keys × 2)`，确保负载因子 < 50% 以维持插入性能。初始化期固定，不可动态扩容（扩容走 Rehash 流程，见 §八）。
- **哈希函数**：
  - `key_hash`：XXH64（xxHash 64-bit），快速且分布均匀。
  - h1 = key_hash % BUCKET_COUNT，h2 = (key_hash >> 32) % BUCKET_COUNT | 1（确保 h2 ≠ h1，最低位置 1 保证奇数偏移）。
- **读**：算 h1、h2 两个候选桶 → 最多 2 次 READ（可合并/并发）。
- **写（踢出）**：见 §三，v2 重点深挖的内容。

### 3. 并发控制：CAS + 租约（防死锁）

v1 只用 CAS 锁会因 Client 崩溃导致**永久死锁**。v2 引入**租约（Lease）**：

```rust
/// lock_version 字段的位域布局（64 位）
/// [63..32] version（单调递增，每次写后 +1，用于乐观读校验）
/// [31.. 8] lease_ts（24 位时间戳，单位 ms，约 4.6 小时回绕）
/// [ 7.. 3] 保留
/// [ 2.. 0] mode(1b) + state(locked/tombstone)
/// LEASE_TIMEOUT_MS = 5 × P99 网络 RTT + max_clock_skew
/// 典型 100Gbps RoCEv2 环境下 P99 RTT ≈ 2μs，设 max_clock_skew = 10ms
/// 则 LEASE_TIMEOUT_MS = 101ms，保守取 100ms
/// 所有节点须经 NTP/PTP 同步时钟，最大偏差不超过 10ms
const LEASE_TIMEOUT_MS: u32 = 100;
fn is_locked(lv: u64) -> bool { (lv & 0x01) != 0 }
fn is_expired(lv: u64, now_ms: u32) -> bool {
    let lease = ((lv >> 8) & 0xFFFFFF) as u32;
    now_ms.wrapping_sub(lease) > LEASE_TIMEOUT_MS   // 例如 100ms
}
```

/// 合法状态说明：
///   - 空闲 (UNLOCKED, alive): bit[0]=0, bit[1]=0
///   - 已加锁 (LOCKED): bit[0]=1, bit[1]=0（加锁期间不能为 tombstone）
///   - 墓碑 (TOMBSTONE): bit[0]=0, bit[1]=1（墓碑不能被加锁）
///   bit2 独立决定 mode（Inline/Extent），与 locked/tombstone 正交

**加锁协议（CAS 两步）：**
1. Client 读取当前 `lock_version`，构造 `新值 = version | (now<<8) | mode | LOCKED`。
2. `RDMA_CAS(addr, 期望旧值, 新值)`：成功即持锁；失败则重试或（若过期）强制接管。
3. 操作完成后 `RDMA_WRITE` 写回：`version+1 | cleared lease | UNLOCKED`。
4. **version 单调递增**：乐观读 Client 在读前后比对 version，若变化则重试，保证线性一致。

### 4. 去中心化 Epoch GC（v1 违背"Server CPU 零参与"，v2 修正）

- 每个 Client 本地维护 `active_ts`（每次操作前刷新）。
- Server GC 线程（控制面，极低频）收集所有 Client 的 `min(active_ts)`。
- `min_active_ts` 之前标记为死亡（`epoch_mark < min_active_ts`）的 Extent 可安全回收归还 Free List。
- **删除**操作不立即释放 Extent，只置 `tombstone` + `epoch_mark = 当前 ts`。
- **GC 扫描间隔**：每秒一次（可配置）。每次扫描：收集所有 Client 的 min(active_ts) → 遍历 ExtentHeader.epoch_mark < min_active_ts 的 Extent → 归还 Free List；同时清理 tombstone 桶（CAS 置零）。
- **Client active_ts 上报**：每次数据面操作后异步向 Server 控制面上报（或随心跳携带），更新间隔 ≤ 1s。

---

## 三、Cuckoo 写路径：分布式状态机（v2 重点）

v1 轻描淡写了"踢出"。分布式环境下这是**多步状态机**，必须保证 Client A 踢出 Client B 的 Key 时，并发读 Client B 看到一致结果。

### 状态机（持锁 → 搬迁 → 覆写 → 解锁）

```
插入 key=K, K 应在桶 B1，但 B1 满 → 踢出 B1 中的 K' 到其备选桶 B2

简单情况（B2 空）：
  ┌─────────┐   CAS锁B1    ┌─────────┐   CAS锁B2    ┌─────────┐
  │ 初始    │ ──────────▶  │ 持B1锁  │ ──────────▶  │ 持B1+B2 │
  └─────────┘             └─────────┘             └────┬────┘
                                                        │
                              READ旧K'值 ◀──────────────┤
                                        │               │
                              WRITE K'到B2空位           │
                                        │               │
                              WRITE 新K到B1              │
                                        │               │
                              version+1解锁B1,B2 ◀───────┘

链式踢出（B2 也满，需踢出 K'' 到 B3，递归）：
  每步：锁定当前及目标桶 → 搬迁 → 继续下一跳 → 尾部桶为空时终止。
  若链长达到 MAX_KICK(=16)，执行全部回滚：
    依次解锁已持锁桶（不修改数据），返回 KV_FULL。
  加锁顺序严格按 h1 桶地址升序，避免死锁。
```

**一致性保证（读侧）：** 并发读 B1/B2 的 Client，一旦观察到 `locked` 置位或读前后 `version` 变化，立即**重试整条读路径**。这会增加写冲突期的尾延迟，但保证线性一致。

**踢出链过长保护：** 设置 `MAX_KICK = 16`，超过则全部回滚（不解锁已持锁桶，不修改数据），返回 `KV_FULL`。**v1 不实现自动 Rehash**：运维可通过控制面命令触发离线 Rehash（暂停写入 → 分配新表 → 搬迁 → 切换）；自动 Rehash 列入后续版本。

---

## 四、Rust 工程实现

### 1. 依赖与封装层

| Crate | 用途 |
|-------|------|
| `rdma-sys` | 底层 `libibverbs` FFI（不自写绑定） |
| `bytemuck` | 零拷贝结构体 ↔ `&[u8]`（禁 `serde`，避免序列化开销） |
| `crossbeam` / `crossbeam-epoch` | 无锁 channel + 本地 epoch 原语 |
| `tokio` | 异步运行时（仅控制面与回调分发） |
| `pyo3` (cdylib) | **LMCache L2 适配**：暴露 `native_plugin` connector（见 §五） |

**封装原则：** `MemoryRegion` 拥有所有权，`Drop` 自动调用 `ibv_dereg_mr`，杜绝泄漏。

**引擎核心公开 Trait（稳定 API 契约）：**

```rust
/// 引擎核心与 LMCache Connector 之间的稳定接口
pub trait KvEngine: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RdmaError>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), RdmaError>;
    async fn delete(&self, key: &[u8]) -> Result<(), RdmaError>;
    async fn exists(&self, key: &[u8]) -> Result<bool, RdmaError>;
    fn batch_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Vec<u8>>, RdmaError>>;
    fn batch_put(&self, kvs: &[(&[u8], &[u8])]) -> Vec<Result<(), RdmaError>>;
}
```

### 2. 异步与 Completion Queue (CQ) 整合（v1 未涉及，v2 关键）

RDMA `poll_cq` 是阻塞/轮询的，不能在 Tokio worker 线程里直接调用。

**方案 A（推荐，低延迟）：轮询线程 + 无锁队列**
- 独立核心绑定一个线程死循环 `poll_cq`（busy-poll，消除中断延迟）。
- 取到完成事件后，经 `crossbeam` 无锁 channel 投递给 Tokio 任务，由 `tokio::sync::oneshot` 唤醒对应 `Future`。

**方案 B（高吞吐）：`AsyncFd` + 中断模式**
- 网卡 CQ 暴露 fd，用 `epoll` 监听，融入 Tokio 的 `AsyncFd`。适合非延迟敏感的高并发场景。

```rust
/// 方案 A 骨架：轮询线程 → 唤醒 Future
struct RdmaRuntime {
    cq: CompletionQueue,
    pending: ConcurrentHashMap<u64, oneshot::Sender<RdmaResult>>, // wr_id → sender
}
impl RdmaRuntime {
    fn poll_loop(self: Arc<Self>) {
        // 绑核：core_affinity::set_for_current(...)
        loop {
            for wc in self.cq.poll() {
                if let Some(tx) = self.pending.remove(&wc.wr_id) {
                    let _ = tx.send(wc.into());   // 唤醒等待的 async fn
                }
            }
        }
    }
    async fn rdma_read(&self, ...) -> Result<...> {
        let (tx, rx) = oneshot::channel();
        let wr_id = register_pending(tx);
        self.qp.post_read(wr_id, ...)?;
        rx.await.map_err(...)?
    }
}
```

### 3. 异步 Buffer 生命周期：所有权转移（v2 关键坑）

异步完成回调中，借用本地 Buffer 的生命周期无法被借用检查器表达。

**解法：所有权转移（Box::into_raw / Box::from_raw）**
- `post_send` 时把 Buffer 装箱 `Box::into_raw` 作为 `wr_id`（或上下文）传入 FFI。
- CQ 完成回调中 `Box::from_raw` 恢复所有权，交还调用方。
- **异常路径（超时/错误）必须有兜底回收**，否则内存泄漏。建议封装一个 `PendingTracker`，超时扫描并 `from_raw` 回收。

### 4. 客户端 One-Sided 读（双模完整流程）

```rust
impl RdmaKvClient {
    /// GET：Inline 小值 = 1 RTT；Extent 大对象 = 2 RTT（桶 + 整块 READ）
    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let h = hash_key(key);
        let b1 = (h % BUCKET_COUNT) as usize;
        let b2 = ((h >> 32) % BUCKET_COUNT | 1) as usize;
        let mut bucket = HashBucket::zeroed();

        // 乐观读 B1
        self.qp.rdma_read(&mut bucket, self.remote.hash_table_addr + b1 * 64).await.ok()?;
        let ver1 = bucket.lock_version;
        if is_locked(ver1) || (bucket.key_hash != h && bucket.key_hash != 0) {
            // B1 被锁或不匹配 → 探 B2
            self.qp.rdma_read(&mut bucket, self.remote.hash_table_addr + b2 * 64).await.ok()?;
        }
        if is_locked(bucket.lock_version) || bucket.key_hash != h { return self.retry_get(key).await; }

        if is_inline(bucket.lock_version) {
            let result = (bucket.key_or_digest[..key.len()] == *key).then(|| bucket.body.to_vec());
            // 读后重校验 version（乐观读一致性）
            self.qp.rdma_read(&mut bucket, self.remote.hash_table_addr + b1 * 64).await.ok()?;
            if bucket.lock_version != ver1 { return self.retry_get(key).await; }
            return result;
        }

        // Extent 模式：读取大对象 + 读后重校验 version
        let (off, len) = parse_extent_ref(&bucket.body);
        let mut buf = vec![0u8; len as usize];
        self.qp.rdma_read(&mut buf, self.remote.large_obj_addr + off as usize).await.ok()?;
        // 读后重校验：bucket version 未变且未被锁
        self.qp.rdma_read(&mut bucket, self.remote.hash_table_addr + b1 * 64).await.ok()?;
        if is_locked(bucket.lock_version) || bucket.lock_version != ver1 {
            return self.retry_get(key).await;
        }
        (verify_digest(key, &bucket.key_or_digest)).then_some(buf)
    }
}
```

### 5. 错误类型与重试策略

```rust
#[derive(Debug, Clone)]
enum RdmaError {
    Timeout,              // 可重试
    CasFailed,            // 可重试（CAS 竞争失败）
    VersionMismatch,      // 可重试（乐观读版本变化）
    KvFull,               // 不可重试（表满）
    InvalidKey,           // 不可重试
    HardwareError,        // 不可重试（致命硬件错误）
    NotConnected,         // 可重试（重连后）
}

impl RdmaError {
    fn is_retriable(&self) -> bool {
        matches!(self, Timeout | CasFailed | VersionMismatch | NotConnected)
    }
}
```

---

## 五、LMCache L2 存储适配层（v3 新增）

使本引擎可作为 LMCache 的可配置 L2 存储引擎。LMCache 是配合 vLLM 的 LLM KV cache 库，存的是**模型 KV cache 张量的原始字节**（按 chunk 切分，单块可达 MB 级）。

### 1. 集成路径：`native_plugin` + PyO3（与官方 `rust/raw_block` 同构）

LMCache 新架构（MP 模式）提供 [`native_plugin`](https://github.com/LMCache/LMCache) 类型，专为对接外部原生存储引擎设计；其 `NativeConnectorL2Adapter` 已内置 **demux / eventfd / 锁**的全部桥接逻辑，**Rust 侧零 Python 样板**。

| 维度 | PyO3 原生绑定（✅ 采用） | 网络适配层（HTTP/gRPC） |
|------|--------------------------|--------------------------|
| 数据路径 | Python `memoryview` → `void*` **零拷贝**直传，GIL 释放 | 需序列化/拷贝 + 网络往返 |
| 完成通知 | 原生 `eventfd`，`NativeConnectorL2Adapter` 自动 demux | 需自起 asyncio 轮询 |
| 工作量 | 仅写 Rust（6 方法 + worker 线程池 + eventfd） | Python adapter + 协议 + 服务端 |
| 适用 | Python(vLLM) 进程内嵌 connector | 仅当需非 Python 进程共享 L2 时补充 |

> 不用网络层包一层：会丢失 `eventfd + 零拷贝` 的全部性能收益。本引擎的 client-server 架构体现在 **Rust connector 内部用 RDMA verbs 与 Server 通信**，Python 侧无感知。

### 2. Connector 接口契约（Rust 实现 6 方法，PyO3 暴露）

| 方法 | PyO3 暴露签名 | 语义 |
|------|---------------|------|
| `event_fd` | `fn(&self) -> i64` | 返回完成通知 eventfd（Linux `eventfd`，macOS `pipe` 回退） |
| `submit_batch_get` | `fn(&self, keys: Vec<String>, mvs: Vec<PyObject>) -> u64` | 异步批量读，向调用方提供的 memoryview 缓冲区写数据；返回 `future_id` |
| `submit_batch_set` | `fn(&self, keys: Vec<String>, mvs: Vec<PyObject>) -> u64` | 异步批量写 |
| `submit_batch_exists` | `fn(&self, keys: Vec<String>) -> u64` | 异步批量存在性 |
| `submit_batch_delete` | `fn(&self, keys: Vec<String>) -> u64` | 异步批量删除（可选） |
| `drain_completions` | `fn(&self) -> Vec<(u64,bool,String,Option<Vec<bool>>)>` | 拉取完成事件：`(future_id, ok, error, per_key_bools)` |
| `close` | `fn(&self)` | 释放资源 |

**完成模型：** 同步提交（线程安全，被 LMCache store/prefetch controller 并发调用）+ `eventfd` 异步通知。零拷贝：`memoryview` 在 `py::gil_scoped_release` 后透传 `void*` 给 RDMA WR，缓冲区生命周期由 Python 侧保证。

### 3. Rust 侧架构：worker 线程池 + eventfd

镜像 LMCache `ConnectorBase` 模板（`csrc/storage_backends/connector_base.h`）：

```
Python(vLLM) ─submit_batch_*─▶ RDMANativeConnector
                                   │  入请求队列 SQ（按 batch_chunk_num_bytes 聚合）
                                   ▼
                         N×worker 线程（每线程一个 RDMA QP）
                                   │  One-Sided RDMA READ/WRITE/CAS
                                   ▼
                              完成 CQ ──▶ drain_completions() ──▶ 写 eventfd
                                                                          │
                          LMCache demux 线程 ◀──────────────────────────┘
```

- 每 worker 绑核，独占 QP，从 SQ 取批量请求 → `post_send` → `poll_cq`。
- 完成后填 `Completion{future_id, ok, bytes, error}`，触发 eventfd；`drain_completions` 由 LMCache demux 线程在 eventfd 触发时调用。
- `batch_chunk_num_bytes`：聚合阈值。含义：
  - 当 SQ 中积累的请求总字节数 ≥ `batch_chunk_num_bytes` 时，worker 立即 flush 一批 `post_send`。
  - 同时设置最大等待时间 `MAX_BATCH_WAIT_US = 50μs`：即使总字节数未达标，超时后也立即 flush 当前积压，保证尾延迟。
  - 典型值：16MB（LMCache 配置示例中的 16777216）。

### 4. Key / Value 映射（关键适配）

**Key**：LMCache `ObjectKey` 序列化为字符串
```
<model_name>@<kv_rank:08x>@<object_group_id hex>@<chunk_hash hex>[@<cache_salt>]
# 例: llama-7b@0000000c@0@a1b2c3d4   （变长，可达 ~160B）
```
- 映射：对该字符串求 **64-bit XXH64 哈希** → Cuckoo 桶的 `key_hash`；字符串的**强摘要（16B XXH128）**存入 `key_or_digest` 用于冲突校验。
- **关键约束**：LMCache Key 长度可达 ~160B，超出 Inline 模式的 16B key 容量，因此 LMCache 场景下**必须使用 Extent 模式**。Inline 模式仅适用于通用小 KV（key ≤ 16B, value ≤ 32B）。
- LMCache Key 自身含 `chunk_hash`，天然抗冲突；结合 16B digest 校验，冲突概率可忽略。

**Value**：LMCache 存的是 **KV cache 张量的原始字节（无头、无序列化）**，单块可达 MB 级。
- 映射：走 **Extent 模式**（§二.1）。`submit_batch_set` 把字节写入 Large Object Region，桶记录 `{offset, length}`；`submit_batch_get` 单次 RDMA READ 取整块。serde（如 fp8 量化）由 LMCache 在 `--l2-adapter` 配置层处理，引擎透明存原始字节。

### 5. 配置示例（LMCache CLI）

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
  "eviction": {"eviction_policy": "LRU", "trigger_watermark": 0.8},
  "serde": {"type": "fp8", "fp8_dtype": "float8_e4m3fn"}
}'
```

### 6. 数据流

```
vLLM decode/prefill → LMCache L1(CPU/GPU) miss → L2 查询
   → RDMANativeConnector.submit_batch_get(keys, memoryviews)
   → Rust worker: RDMA READ(Server Large Object Region) → eventfd
   → drain_completions() → memoryview 被填入 KV cache 字节
   → LMCache 返回 vLLM，零 CPU 拷贝
```

> 参考实现：LMCache 官方 `rust/raw_block`（PyO3 `cdylib`）、`csrc/storage_backends/fs/connector.cpp`（最简原生 connector）、`examples/lmc_external_native_connector/`（第三方 pybind 样例）。

---

## 六、关键"坑"与避坑指南

### 1. CPU 缓存一致性（最致命）
- **问题**：RDMA 经 PCIe 直写物理内存，**不更新** Server CPU L1/L2 Cache。Server CPU 后续读会命中脏数据。
- **方案**：Server CPU **绝对不读**数据面内存；仅做分配与 GC。若必须读（Compaction），用 `MAP_UNCACHED` 或读前 `clflush`。
- **注**：本版本暂不实现 Compaction。Extent 分配器使用 buddy/slab 算法预防外部碎片化；若碎片率超过 30%，触发运维告警而非自动整理。Compaction 列入后续版本规划。

### 2. 内存对齐与 Atomic 语义
- **问题**：RDMA CAS 要求 8 字节对齐 + 原生类型。Rust 含 `Vec`/`String` 的结构布局不确定。
- **方案**：MR 内所有结构 `#[repr(C, align(64))]` + `Pod`。`AtomicU64` 是**本地 CPU 内存模型**，不能直接跨节点当 RDMA 原子用——必须用 `RDMA_CAS` 原语，本地仅作布局占位。

### 3. 小消息尾延迟
- **方案**：① Inline Data（KV < 32B 内联，1 RTT）；② Batching（SGE 批量 `post_send`，LMCache `batch_chunk_num_bytes`）；③ busy-poll CQ（方案 A）。

### 4. 借用检查器 vs 远端内存
- **方案**：FFI 边界内大胆 `unsafe`；对外 Safe API；`Drop` 管理资源；异步 Buffer 用所有权转移（§四.3）。

### 5. LMCache 集成零拷贝契约（v3 新增）
- **契约**：`submit_batch_*` 的 memoryview 生命周期由 Python 侧保证；Rust 侧在 `drain_completions` 返回前不得释放/复用对应缓冲区。
- **坑**：GIL 释放后多 worker 并发写不同 memoryview，必须保证 buffer 指针→future_id 的映射无歧义；超时 future 的 buffer 须由 `PendingTracker` 兜底。

### 6. PendingTracker 超时回收
- **数据结构**：`BTreeMap<Instant, Vec<(u64, *mut u8)>>` 按超时时间分组。
- **扫描间隔**：每 100ms 扫描一次。
- **超时阈值**：5 倍 P99 网络 RTT（≈ 10μs × 5 = 50μs，保守取 1ms 以覆盖异常）。
- **回收动作**：对超时条目逐一 `Box::from_raw(buffer_ptr)` 释放内存，记录警告日志。

---

## 七、开发路线图（9–10 周冲刺）

| 阶段 | 周次 | 目标 | 里程碑（可验证） |
|------|------|------|------------------|
| **P1 基础设施** | 1–2 | Safe Rust ↔ RDMA 硬件通路；**优先验证网卡 CAS 性能** | 双机/双进程完成异步 RDMA READ/WRITE 乒乓 |
| **P2 内存引擎** | 3–4 | 本地 Cuckoo + CAS 并发 + **双模（Inline/Extent）布局** | 多线程并发 100 万 KV 插入/查询，无泄漏无竞争 |
| **P3 分布式数据面** | 5–6 | 指针 → `rdma_read`；原子 → `rdma_cas`；踢出状态机 | 多 Client 并发读写一致，**Server CPU ≈ 0%** |
| **P4 容错+GC+尾延迟** | 7 | CAS 租约过期、Epoch GC、Inline/SGE 优化 | 24h 稳定性无死锁/无内存耗尽，**P99 < 10μs** |
| **P5 LMCache L2 适配** | 8 | PyO3 connector 6 方法 + worker 线程池 + eventfd；native_plugin 接入 | vLLM+LMCache 端到端跑通 KV cache 存取，零拷贝验证 |
| **P6 性能压测** | 9–10 | `perf` 热点；对标 Redis/Memcached + **LMCache 工作负载** | **10M+ OPS，P50 < 5μs**；LMCache L2 命中延迟达标 |

---

## 八、核心风险与应对

| 风险 | 级别 | 应对 |
|------|------|------|
| **RDMA CAS 硬件兼容性**：部分低端 RoCEv2 网卡 CAS 支持差/性能差 | 🔴 高 | P1 优先验证。不支持则降级为 Two-Sided Send/Recv + Server CPU 介入（牺牲 One-Sided 优势，保可用） |
| **Rust 异步 + Buffer 生命周期**：完成回调借用难管理 | 🟠 中 | 所有权转移（`Box::into_raw/from_raw`）+ `PendingTracker` 超时兜底回收 |
| **缓存一致性**：Server 误读数据面 | 🟠 中 | 纪律性禁止 Server 读数据面；必要时 `MAP_UNCACHED`/`clflush` |
| **写尾延迟**：Cuckoo 踢出 + 并发重试 | 🟡 中低 | `MAX_KICK` 上限防活锁；写热点桶时退避；监控 P99 |
| **Client 崩溃死锁** | 🟠 中 | 租约过期强制接管（§二.3） |
| **复制延迟导致数据丢失**：主节点宕机时未复制数据丢失 | 🔴 高 | 明确可接受丢失窗口（如 <1s）；异步复制 + 备份宕机时降级为无复制模式，记录告警 |
| **脑裂（Split-Brain）**：网络分区导致双主同时写入 | 🔴 高 | 控制面基于 Raft/Paxos 多数派仲裁；仅主节点持有写入租约 |
| **LMCache 接口漂移**（v3 新增）：`native_plugin` 契约随上游版本变化 | 🔴 高 | 锁定 LMCache git tag（非 commit）；CI 每提交编译验证与锁定版本的兼容性；仅接受新增可选方法，强制方法签名变更须升级适配层 |
| **PyO3 GIL 与零拷贝**（v3 新增）：worker 并发写 buffer 的所有权边界 | 🟠 中 | `gil_scoped_release` + `future_id→buffer` 严格映射 + 超时兜底回收 |
| **Extent 碎片化**：长期分配/回收导致 Large Object Region 外部碎片，大块分配失败 | 🔴 高 | Extent 分配器使用 buddy system 预防碎片；碎片率 > 30% 触发告警；Compaction 列后续版本 |
| **网络带宽瓶颈**（Extent 大对象场景）：MB 级 value 吞吐受 100Gbps 线速限制 | 🟠 中 | Inline 小 KV 目标 10M OPS；Extent 大对象单独以 GB/s 吞吐度量，目标 ≥ 90% 线速 |
| **HugePages 配置不足**：部署环境未预配足够大页，Server 无法启动 | 🟠 中 | T1-B 启动时检测可用 HugePages，不足时输出明确错误及所需数量；文档记录最低要求 |
| **时钟偏差**：节点间 NTP 时钟偏差超过 LEASE_TIMEOUT_MS 缓冲 | 🟡 低 | 所有节点强制 NTP/PTP 同步；LEASE_TIMEOUT_MS 内含 10ms 偏差缓冲；监控节点时钟偏差 |

---

## 九、相对 v1 的改进清单

1. **缓存行对齐**：`HashBucket`/`ExtentHeader` 强制 64B 对齐 + 编译期静态断言，消除伪共享。
2. **双模内存布局（v3）**：桶支持 Inline（小值，1 RTT）与 Extent（大对象，单次 READ），取代 v2 链式 DataSlot。
3. **锁 + 租约**：CAS 锁嵌入过期时间戳，解决 Client 崩溃死锁；version 单调递增保证乐观读线性一致。
4. **写路径状态机**：明确 Cuckoo 踢出的"锁→搬→写→解"四步与读侧重试协议。
5. **去中心化 GC**：Client 维护活跃时间戳，Server 仅收集最小值，回归"CPU 零参与数据面"。
6. **异步与 CQ 整合**：busy-poll 轮询线程 + 无锁 channel 唤醒 Future，或 `AsyncFd` 中断模式。
7. **Buffer 所有权转移**：解决异步完成回调的借用生命周期难题。
8. **容错层**：Server 宕机异步复制/持久化、网络分区仲裁切换。
9. **LMCache L2 适配（v3）**：PyO3 `native_plugin` connector（6 方法 + worker 线程池 + eventfd），零拷贝接入 LMCache；key 字符串哈希、value 走 Extent 大对象区。
10. **路线图细化**：每阶段可验证里程碑，P1 即卡硬件 CAS 风险，P5 端到端打通 LMCache。

本方案将 HERD/DrTM 的学术思想、Rust 现代工程实践、分布式系统的容错现实、以及 LLM 推理 KV cache 的实际负载深度结合。按"封装 → 本地逻辑 → 分布式映射 → 容错优化 → LMCache 适配"推进，可打造出世界级的 Rust RDMA 存储引擎。
