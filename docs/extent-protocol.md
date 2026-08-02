# Extent 分布式分配协议设计文档

> **文档编号**：T9-A  
> **所属波次**：Wave 9A — Extent 分布式协议设计（Oracle 审查）  
> **协议代号**：CAS Bump Allocator（CAS 推进式分配器）  
> **状态**：Oracle 已审查通过，待进入 T9-B 实现  
> **关联文档**：`Rust-RDMA.md`、`改进开发计划-Wave8-11-v2.md`、`proto/control.proto`、`src/engine/extent.rs`、`src/engine/layout.rs`

---

## 目录

1. [问题陈述](#1-问题陈述)
2. [协议概述](#2-协议概述)
3. [FreeList 区域共享内存布局](#3-freelist-区域共享内存布局)
4. [ExtentHeader V2 — 32 字节新版头](#4-extentheader-v2--32-字节新版头)
5. [分配流程 (Allocation)](#5-分配流程-allocation)
6. [回收流程 (Reclamation)](#6-回收流程-reclamation)
7. [校验和协议 (Checksum Protocol)](#7-校验和协议-checksum-protocol)
8. [gRPC 协议扩展](#8-grpc-协议扩展)
9. [并发安全分析](#9-并发安全分析)
10. [性能分析](#10-性能分析)
11. [故障模型与降级](#11-故障模型与降级)
12. [边界情况与测试策略](#12-边界情况与测试策略)
13. [附录 A：方案对比（为何选择 CAS Bump Allocator）](#附录-a方案对比如何选择-cas-bump-allocator)
14. [附录 B：完整代码示例](#附录-b完整代码示例)

---

## 1. 问题陈述

### 1.1 当前实现的局限性

`LargeObjectRegion`（`src/engine/extent.rs`，781 行代码）在 Wave 2 实现了本地内存引擎的 extent 分配器，其核心数据结构如下：

```rust
pub struct LargeObjectRegion {
    buffer: Vec<u8>,
    free_list: VecDeque<(u64, u64)>,
    allocated: HashSet<u64>,
    next_offset: u64,
    size: u64,
}
```

**分配策略**：
1. 先在 `free_list`（`VecDeque`）中搜索 ≥ 所需大小的空闲 extent。
2. 若无空闲 extent，则通过 `next_offset` bump pointer 推进分配。

**这在分布式 One-Sided RDMA 场景下完全不可行**，原因如下：

| 问题 | 具体表现 | 违反原则 |
|------|----------|----------|
| **多 Client 并发分配无协调** | `next_offset` 是本地变量，多个 Client 同时推进会导致 extent 区间重叠 | 核心原则 #1：数据面零 CPU |
| **Free list 不可见** | `VecDeque` 在 Client 本地进程中，其他 Client 不知道哪些 extent 已被回收 | 原则上 Client 应可自主获取空闲 extent |
| **无原子 CAS 目标** | 没有共享内存中的原子变量供所有 Client 竞争 | CAS 是实现无锁分配的唯一方式 |
| **FREE_LIST 区域未实现** | `proto/control.proto` 声明了 `RegionType::FREE_LIST = 2`，但 `server.rs` 中 `vaddr: 0, rkey: 0`，仅为占位符 | Wave 2 漏项 |

### 1.2 设计目标

解决上述问题的分布式 extent 分配协议必须满足：

1. **数据面零 CPU**：Server CPU 不参与分配热路径。所有分配操作由 Client 通过 One-Sided RDMA 自主完成。
2. **无锁无冲突**：多个 Client 并发分配时，extent 区间绝不重叠。
3. **GC 回收可重用**：被 GC 清扫的空闲 extent 能被后续分配复用，不浪费空间。
4. **降低分配延迟**：分配操作应当快速（目标 < 10μs），不成为瓶颈。
5. **部分失败可处理**：Client 在分配中途崩溃，不应永久占用 extent。

---

## 2. 协议概述

### 2.1 CAS Bump Allocator 原理

**核心思想**：在 RDMA 共享内存的 `FREE_LIST` 区域头部放置一个 64 字节缓存行对齐的 `bump_offset` 原子变量，所有 Client 通过 **RDMA CAS（Compare-And-Swap）** 原子推进该指针来分配连续区间。

```
┌─────────────────────────────────────────────────────────┐
│ FREE_LIST Region (RDMA-accessible shared memory)        │
├─────────────────────────────────────────────────────────┤
│ [bump_offset: u64 | _pad: [u8; 56]]  ← CAS target (64B)│
├─────────────────────────────────────────────────────────┤
│ [ 已分配 extent #1 ]                                     │
│ [ 已分配 extent #2 ]                                     │
│ [ 已分配 extent #3 ]                                     │
│ [  ... 剩余空闲空间 ... ]                                 │
│ [  ... 剩余空闲空间 ... ]                                 │
└─────────────────────────────────────────────────────────┘
```

**流程概括**：

1. Client 通过 `RDMA READ` 读取当前 `bump_offset` 的值（设旧值为 `old`）。
2. Client 计算本次 extent 的总大小 `total = extent_total(data_len)`。
3. Client 通过 `RDMA CAS` 尝试将 `bump_offset` 从 `old` 更新为 `old + total`。
4. 若 CAS 成功：该 Client 获得区间 `[old, old+total)` 的独占使用权。
5. 若 CAS 失败：说明其他 Client 抢先分配，回到步骤 1 重试。
6. Client 在独占区间内通过 `RDMA WRITE` 写入 `ExtentHeaderV2` 与数据。

**关键特性**：

| 特性 | 说明 |
|------|------|
| **无 ABA 问题** | `bump_offset` 是单调递增的，不会出现 "值回到旧值" 的情况 |
| **无锁** | 完全基于 CAS 原子操作，无自旋锁、无死锁风险 |
| **O(1) 竞争** | CAS 操作是 O(1)，仅在极高并发分配率下才可能产生竞争回退 |
| **不存在释放后重用竞态** | 被 GC 回收的 extent 通过控制面 `SyncFreeList` RPC 推送，不走共享 CAS |

### 2.2 与旧方案的对比

| 维度 | 旧实现（`LargeObjectRegion`） | 新协议（CAS Bump Allocator） |
|------|-------------------------------|----------------------------|
| 分配协调 | 本地 `next_offset` 变量 | 共享内存 `bump_offset` + RDMA CAS |
| Free List | 本地 `VecDeque` | 控制面 `SyncFreeList` RPC → 客户端本地缓存 |
| 并发安全 | 无 (单进程) | CAS 原子操作确保多 Client 无冲突 |
| 崩溃安全 | N/A | `checksum=0` 标记写入中，其他 Client 可忽略未完成的 extent |
| Server CPU 参与 | 无 | 无 (GC 回收路径除外，控制面低频执行) |

---

## 3. FreeList 区域共享内存布局

### 3.1 区域定义

`FREE_LIST` 区域是 Server 端的一块 RDMA 注册的共享内存区域。在现有 `proto/control.proto` 中已有定义但未实现：

```protobuf
enum RegionType {
    HASH_TABLE = 0;
    LARGE_OBJECT = 1;
    FREE_LIST = 2;    // ← 已有定义，待实现
}

message RegionMetadata {
    uint64 vaddr = 1;   // ← 目前为 0，需填实
    uint32 rkey = 2;    // ← 目前为 0，需填实
    uint64 size = 3;
    RegionType type = 4;
    uint64 generation = 5;
}
```

### 3.2 FreeListHeader 结构

```rust
use bytemuck::{Pod, Zeroable};

/// FreeList 区域头部：64 字节缓存行对齐的 CAS 目标。
///
/// 整个 `FREE_LIST` 区域是一个扁平的 extent 分配区。头部仅含一个
/// `bump_offset` 字段（8 字节），其余为 padding 确保缓存行对齐，
/// 防止 RDMA 网卡在 CAS 操作时引入伪共享。
///
/// # Layout (64 bytes, align-64)
///
/// | Offset | Field         | Size |
/// |--------|---------------|------|
/// | 0      | `bump_offset`  | 8    |
/// | 8      | `_pad`         | 56   |
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C, align(64))]
pub struct FreeListHeader {
    /// 当前分配指针：指向 FreeList 区域中下一个未分配的字节偏移。
    /// 所有 Client 通过 RDMA CAS 原子推进此值，获取独占 extent 区间。
    /// 初始值为 `size_of::<FreeListHeader>()`（跳过头部本身，从 64 字节开始）。
    pub bump_offset: u64,

    /// 填充到 64 字节，避免与紧跟的数据产生伪共享。
    pub _pad: [u8; 56],
}

// 编译期断言
const _: () = assert!(core::mem::size_of::<FreeListHeader>() == 64);
const _: () = assert!(core::mem::align_of::<FreeListHeader>() == 64);
```

### 3.3 区域初始化

在 `BootstrappedEngine::bootstrap()` 中，FreeList 区域初始化为：

```rust
/// 初始化 FreeList 区域。
fn init_free_list_region(buf: &mut [u8]) {
    let header = FreeListHeader {
        // bump_offset 初始值为 64，即跳过头部本身
        bump_offset: 64,
        _pad: [0u8; 56],
    };
    let header_bytes: &[u8] = bytemuck::bytes_of(&header);
    buf[0..64].copy_from_slice(header_bytes);

    // 剩余区域清零（可选，安全起见）
    buf[64..].fill(0);
}
```

### 3.4 区域容量

FreeList 区域的总大小由 Server 启动时配置（例如 4 GiB）。可用空间为：

```
可用空间 = FreeList 区域大小 - 64 字节（头部）
```

所有分配的 extent 都直接写入 FreeList 区域的 `[bump_offset_old, bump_offset_new)` 区间，头部的 64 字节永远不用于分配。

**注意**：此 "FreeList" 区域实际上是一个扁平的 extent 数据存储区，而非传统的空闲链表。名称源于它取代了旧 `LargeObjectRegion` 中 `VecDeque<(u64, u64)>` 的角色，但实现方式完全不同。

---

## 4. ExtentHeader V2 — 32 字节新版头

### 4.1 为什么需要 V2

当前的 `ExtentHeader`（V1，24 字节）缺少两个关键字段：

1. **`checksum`**：数据校验和，用于检测部分写入（Client 崩溃时可能出现）。
2. **`version`**：版本号，用于区分 V1/V2 格式，支持滚动升级。

在分布式 One-Sided RDMA 环境中，Client 写入 extent 时可能在任意时刻崩溃（写入 header 后、写入数据前等），导致 Reader 读到不完整的数据。没有校验和，Reader 无法区分 "写入完成" 和 "写入中断" 两种状态。

### 4.2 V2 结构定义

```rust
use bytemuck::{Pod, Zeroable};

/// Extent 头部 V2（32 字节，8 字节对齐）。
///
/// 每个 extent 在共享内存中以此头部开头，后跟 payload 数据。
///
/// # Layout (32 bytes, align-8)
///
/// | Offset | Field        | Size | 说明                              |
/// |--------|--------------|------|-----------------------------------|
/// | 0      | `magic`      | 4    | 魔数：`EXTENT_MAGIC = 0x52444D41` |
/// | 4      | `version`    | 1    | 版本号：1 = V2                    |
/// | 5      | `data_len`   | 4    | 负载数据长度（字节）               |
/// | 8      | `epoch_mark` | 8    | GC 死亡时间戳（epoch）             |
/// | 16     | `checksum`   | 8    | XXH64 of payload（0 = 写入中）     |
/// | 24     | `_pad`       | 8    | 显式填充到 32 字节                 |
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct ExtentHeaderV2 {
    /// 魔数常量：`EXTENT_MAGIC = 0x52444D41`（ASCII "RDMA"）。
    pub magic: u32,

    /// 头部格式版本：1 表示 ExtentHeaderV2。
    /// 0 保留给旧版 ExtentHeader (V1, 24 字节)。
    pub version: u8,

    /// 负载数据长度（字节），不含本头部。使用 u32 以节省空间，
    /// 最大支持 4 GiB 单个 extent（对于 KV cache 张量足够）。
    pub data_len: u32,

    /// GC 死亡时间戳（epoch）。Server GC 线程设置，表示该 extent
    /// 在给定 epoch 开始失效。0 表示未标记为 GC。
    pub epoch_mark: u64,

    /// 负载数据的 XXH64 校验和。写入过程中先保持为 0，数据写入
    /// 完成后再写入实际值。0 表示 "写入进行中"，Reader 应重试或等待。
    pub checksum: u64,

    /// 填充到 32 字节，保持 8 字节对齐。
    pub _pad: [u8; 7],
}

// 编译期断言
const HEADER_V2_SIZE: usize = core::mem::size_of::<ExtentHeaderV2>();
const _: () = assert!(HEADER_V2_SIZE == 32);
const _: () = assert!(HEADER_V2_SIZE % 8 == 0);

/// V2 头部大小（用于 extent_total 计算）。
pub const HEADER_SIZE_V2: u64 = 32;
```

### 4.3 V1 兼容策略

为支持滚动升级，保留旧版 `ExtentHeader`（V1，24 字节），但所有**新创建**的 extent 默认使用 V2 格式。读取时的兼容策略：

```rust
/// 从共享内存中读取 extent header，自动检测 V1/V2 版本。
pub enum ExtentHeaderDecoded {
    V1 {
        data_len: u64,
        epoch_mark: u64,
    },
    V2 {
        data_len: u32,
        epoch_mark: u64,
        checksum: u64,
    },
}

fn decode_header(offset: u64, buf: &[u8]) -> Option<ExtentHeaderDecoded> {
    let off = offset as usize;

    // 先读 magic 和 version（前 5 字节）
    let magic = u32::from_le_bytes(buf[off..off+4].try_into().ok()?);
    if magic != EXTENT_MAGIC {
        return None;
    }

    let version = buf[off + 4];
    match version {
        0 => {
            // V1: 24 字节
            let data_len = u64::from_le_bytes(buf[off+0..off+8].try_into().ok()?);
            let epoch_mark = u64::from_le_bytes(buf[off+8..off+16].try_into().ok()?);
            Some(ExtentHeaderDecoded::V1 { data_len, epoch_mark })
        }
        1 => {
            // V2: 32 字节
            let data_len = u32::from_le_bytes(buf[off+5..off+9].try_into().ok()?);
            let epoch_mark = u64::from_le_bytes(buf[off+8..off+16].try_into().ok()?);
            let checksum = u64::from_le_bytes(buf[off+16..off+24].try_into().ok()?);
            Some(ExtentHeaderDecoded::V2 { data_len, epoch_mark, checksum })
        }
        _ => None, // 未知版本
    }
}
```

### 4.4 extent_total 更新

```rust
/// 计算 extent 总占用大小（header + data，8 字节对齐）。
/// V2 使用 32 字节头部。
#[inline]
pub fn extent_total(data_len: u32) -> u64 {
    align_up(HEADER_SIZE_V2 + data_len as u64, 8)
}
```

---

## 5. 分配流程 (Allocation)

### 5.1 完整流程

```
Client                                               Server (RDMA Memory)
  │                                                       │
  │  ① RDMA READ bump_offset                              │
  │ ──────────────────────────────────────────────────►   │
  │   ← old_offset (e.g. 64)                              │
  │                                                       │
  │  ② 本地计算 new_offset = old_offset + extent_total    │
  │     old_offset = 64, total = 96 → new_offset = 160    │
  │                                                       │
  │  ③ RDMA CAS bump_offset: (expect: old, write: new)    │
  │ ──────────────────────────────────────────────────►   │
  │   ← CAS_SUCCESS                                       │
  │   (区间 [64, 160) 已为该 Client 独占)                  │
  │                                                       │
  │  ④ RDMA WRITE ExtentHeaderV2 (checksum=0)             │
  │     @ offset 64                                       │
  │ ──────────────────────────────────────────────────►   │
  │                                                       │
  │  ⑤ RDMA WRITE payload data                            │
  │     @ offset 64 + 32                                  │
  │ ──────────────────────────────────────────────────►   │
  │                                                       │
  │  ⑥ RDMA WRITE checksum (XXH64 of payload)             │
  │     @ offset 64 + 16 (checksum field in header)       │
  │ ──────────────────────────────────────────────────►   │
  │                                                       │
  │  ⑦ 将 extent offset 写入 HashBucket.body              │
  │     完成 KV 插入                                       │
```

### 5.2 伪代码实现

```rust
/// 分布式 extent 分配器（Client 侧）。
pub struct DistributedExtentAllocator {
    /// FreeList 区域起始虚拟地址（从 Discover 获取）。
    free_list_vaddr: u64,
    /// FreeList 区域 rkey。
    free_list_rkey: u32,
    /// FreeList 区域总大小。
    free_list_size: u64,
    /// 本地空闲 extent 缓存（来自 Server SyncFreeList RPC）。
    local_free_list: VecDeque<u64>,
}

impl DistributedExtentAllocator {
    /// 在远程 FreeList 区域中分配一个 extent，写入给定的数据。
    ///
    /// 返回值：extent 在 FreeList 区域中的字节偏移量。
    pub async fn allocate(
        &mut self,
        rdma: &RdmaContext,
        data: &[u8],
    ) -> Result<u64, ExtentError> {
        let total = extent_total(data.len() as u32);

        // --- 路径 A：优先尝试本地空闲列表 ---
        if let Some(free_offset) = self.try_local_free(rdma, data, total).await? {
            return Ok(free_offset);
        }

        // --- 路径 B：CAS Bump 分配 ---
        let bump_addr = self.free_list_vaddr; // HEADER 在区域起始

        loop {
            // ① RDMA READ 读取当前 bump_offset
            let old_offset = rdma_read_u64(
                rdma,
                bump_addr,
                self.free_list_rkey,
            ).await?;

            let new_offset = old_offset + total;

            // 检查是否超出区域
            if new_offset > self.free_list_size {
                return Err(ExtentError::OutOfSpace);
            }

            // ③ RDMA CAS 原子推进 bump_offset
            let cas_result = rdma_cas_u64(
                rdma,
                bump_addr,
                self.free_list_rkey,
                expected: old_offset,
                desired: new_offset,
            ).await?;

            if cas_result == old_offset {
                // CAS 成功：获得独占区间 [old_offset, new_offset)
                let extent_base = self.free_list_vaddr + old_offset;

                // ④⑤⑥ 写入 extent（header + data + checksum）
                self.write_extent_ordered(rdma, extent_base, data).await?;

                return Ok(extent_base - self.free_list_vaddr);
            }
            // CAS 失败 → 重试
        }
    }
}
```

### 5.3 CAS 失败重试

当多个 Client 并发分配时，CAS 可能失败（另一个 Client 抢先推进了 `bump_offset`）。这是**预期的正常行为**，不是错误。

```rust
// CAS 失败后重新读取新值并重试
// CAS 竞争仅在高并发分配率下显著（见 §10 性能分析）
//
// 重试上限（防止活锁）：
const MAX_ALLOC_RETRIES: u32 = 32;

loop {
    // ... 读取 + CAS ...

    if cas_result == old_offset {
        // 成功，退出循环
        break;
    }

    retries += 1;
    if retries >= MAX_ALLOC_RETRIES {
        return Err(ExtentError::AllocContention);
    }
    // 无需显式退避：RDMA RTT 本身已提供自然随机延迟
}
```

### 5.4 本地空闲列表优先策略

为提高空间利用率和降低碎片，Client 收到 Server `SyncFreeList` 推送的空闲 offset 后，**优先**从本地列表分配：

```rust
/// 尝试从本地空闲列表中分配。
/// 使用 CAS 操作验证 offset 上的 header magic 是否仍为 0（表示空闲），
/// 若成功则写入新的 header magic，获得所有权。
async fn try_local_free(
    &mut self,
    rdma: &RdmaContext,
    data: &[u8],
    total: u64,
) -> Result<Option<u64>, ExtentError> {
    while let Some(offset) = self.local_free_list.pop_front() {
        let extent_base = self.free_list_vaddr + offset;

        // CAS 在 magic 字段上：确保该 extent 尚未被其他 Client 占用
        // （magic 为 0 表示空闲，CAS 写入 EXTENT_MAGIC 以抢占）
        let magic_addr = extent_base; // magic 在 header 偏移 0
        let cas_result = rdma_cas_u32(
            rdma,
            magic_addr,
            self.free_list_rkey,
            expected: 0u32,
            desired: EXTENT_MAGIC,
        ).await?;

        if cas_result == 0 {
            // 成功抢占该空闲 extent
            self.write_extent_ordered(rdma, extent_base, data).await?;
            return Ok(Some(offset));
        }
        // 该 extent 已被其他 Client 抢占 → 跳过，继续试下一个
    }
    Ok(None)
}
```

---

## 6. 回收流程 (Reclamation)

### 6.1 概述

在 CAS Bump Allocator 协议中，**已被 GC 回收的 extent 区间无法通过 `bump_offset` 自动重用**（因为 bump pointer 只会向前推进）。回收路径走**控制面**：

```
Server (GC 线程)                          Client
     │                                       │
     │  ① sweep: 收集过期 extent 的 offsets   │
     │                                       │
     │  ② SyncFreeList RPC (推送)             │
     │ ───────────────────────────────────►   │
     │     freed_offsets: [128, 512, 1024]   │
     │                                       │
     │  ③                           更新 local_free_list
     │                                  push [128, 512, 1024]
     │                                       │
     │  ④ 下次 allocate 时优先使用            │
     │     本地空闲列表中的 offset             │
```

### 6.2 SyncFreeList RPC

详见 §8.2 gRPC 协议扩展。

### 6.3 Server 侧 GC 流程

```rust
/// Server 侧 GC 线程：周期性清扫过期 extent 并推送给 Client。
impl GcThread {
    async fn run(&mut self, ctx: &ServerContext) {
        loop {
            tokio::time::sleep(GC_INTERVAL).await;

            let min_active_epoch = self.compute_min_active_epoch(&ctx.clients);
            let freed_offsets = ctx.engine.sweep_extents(min_active_epoch);

            if !freed_offsets.is_empty() {
                // 推送给所有已连接的 Client
                for client in &ctx.clients {
                    client.send_sync_free_list(&freed_offsets).await;
                }
            }

            // GC 也负责推进全局 epoch
            ctx.engine.advance_epoch();
        }
    }
}
```

### 6.4 Client 缓存管理

Client 接收 `SyncFreeList` 推送后，将 offset 加入本地 `VecDeque`。为防止本地缓存无限制增长，设置上限：

```rust
/// 本地空闲 extent 列表最大容量。
/// 超过此值时丢弃最旧的（已缓存的 extent 可能被其他 Client 先抢占）。
const MAX_LOCAL_FREE_LIST: usize = 1024;

fn merge_free_list(&mut self, new_offsets: &[u64]) {
    for &offset in new_offsets {
        self.local_free_list.push_back(offset);
    }

    // 超出上限时丢弃多余条目
    while self.local_free_list.len() > MAX_LOCAL_FREE_LIST {
        self.local_free_list.pop_front();
    }
}
```

### 6.5 回收竞态分析

回收路径不存在与传统分配路径的竞态：

| 场景 | 处理方式 |
|------|----------|
| **GC 推送的 offset 被两个 Client 同时使用** | 通过 CAS 在 `magic` 字段上竞争：只有 CAS 成功的 Client 获得所有权 (§5.4)；失败的 Client 跳过该 offset。 |
| **Client 使用回收 offset 时，offset 已被写入新数据** | CAS 失败 (`magic != 0`)，Client 自动跳过。 |
| **回收期间 bump_offset 推进到该区间** | 不可能：GC 回收走控制面推送，不会影响 bump_offset；分配器优先用本地空闲列表，bump_offset 不会主动回退。 |

---

## 7. 校验和协议 (Checksum Protocol)

### 7.1 问题

One-Sided RDMA 存在一个关键隐患：**RDMA WRITE 操作之间不保证顺序**。如果 Client 在写入 extent 的过程中崩溃（或网络断开），另一 Client 通过 `RDMA READ` 可能读到**不完整或损坏的数据**。

传统方案（先写数据再写 header magic）无法区分以下两种状态：
- **状态 A**：写入已完成，header + 数据都正确。
- **状态 B**：写入中途崩溃，header 有效但数据不完整 / 部分为旧值。

### 7.2 三阶段写入协议（修正版）

经过 Oracle 审查修正后的写入顺序：

```
阶段 1：写入 ExtentHeaderV2
   ┌────────────────────────────────────┐
   │ 写入 header，其中 checksum = 0     │
   │ (magic=EXTENT_MAGIC, version=1,    │
   │  data_len=N, epoch_mark=0,         │
   │  checksum=0)                       │
   └────────────────────────────────────┘
              │
              ▼
阶段 2：写入负载数据
   ┌────────────────────────────────────┐
   │ RDMA WRITE payload @ offset+HEADER_SIZE │
   └────────────────────────────────────┘
              │
              ▼
阶段 3：写入校验和
   ┌────────────────────────────────────┐
   │ RDMA WRITE checksum @ offset+16    │
   │ checksum = XXH64(payload)          │
   └────────────────────────────────────┘
```

### 7.3 为何此顺序是关键的

RDMA WRITE 操作之间没有顺序保证（不同于 TCP 的 FIFO 顺序）。因此：

- **先写 checksum 再写数据**：Reader 可能读到 valid-checksum + stale-data（先到达的 checksum 与后到达的数据不匹配），产生**数据撕裂**。
- **先写数据再写 checksum**：Reader 见到 checksum=0 时知道数据可能不完整，等待重试；见到非零 checksum 且匹配时，数据已经完整写入（因为 checksum 最后写入，它到达时数据必然已到达）。

### 7.4 读取验证流程

```rust
/// 从 extent 读取数据，带校验和验证。
async fn read_extent_verified(
    rdma: &RdmaContext,
    extent_base: u64,
    rkey: u32,
) -> Result<Vec<u8>, ExtentError> {
    const MAX_RETRIES: u32 = 4;

    for _ in 0..MAX_RETRIES {
        // ① RDMA READ 整个 header
        let header_bytes = rdma_read(rdma, extent_base, rkey, 32).await?;
        let header: &ExtentHeaderV2 = bytemuck::from_bytes(&header_bytes);

        // ② 验证 magic
        if header.magic != EXTENT_MAGIC {
            return Err(ExtentError::InvalidMagic);
        }

        // ③ 验证 version
        if header.version != 1 {
            return Err(ExtentError::UnsupportedVersion(header.version));
        }

        // ④ 检查 checksum
        if header.checksum == 0 {
            // 写入进行中 → 短暂等待后重试
            tokio::time::sleep(Duration::from_micros(10)).await;
            continue;
        }

        // ⑤ RDMA READ 负载数据
        let data = rdma_read(
            rdma,
            extent_base + HEADER_SIZE_V2,
            rkey,
            header.data_len as usize,
        ).await?;

        // ⑥ 验证校验和
        let computed = xxhash64(&data, 0);
        if computed != header.checksum {
            // 校验和不匹配 → 数据撕裂
            return Err(ExtentError::ChecksumMismatch {
                expected: header.checksum,
                got: computed,
            });
        }

        // ⑦ 数据完整
        return Ok(data);
    }

    Err(ExtentError::WriteInProgress) // 超时：写入可能已失败/崩溃
}
```

### 7.5 校验和算法选择：XXH64

选择 [XXH64](https://github.com/Cyan4973/xxHash)（非加密哈希）而非 CRC32 或 SHA-256，原因：

| 算法 | 速度 (GB/s) | 碰撞概率 | 适用场景 |
|------|-------------|----------|----------|
| XXH64 | ~13 GB/s (8 字节输出) | 2^-64 | 数据完整性检测（非对抗性） |
| CRC32 | ~4 GB/s (4 字节输出) | 2^-32 | 传输层校验（不够安全） |
| SHA-256 | ~0.5 GB/s (32 字节输出) | 2^-256 | 需要密码学抗碰撞性 |

XXH64 对 MB 级 KV cache 张量性能友好，64 位输出对于检测 RDMA 传输中的位翻转或部分写入已足够。**注意**：XXH64 不提供密码学安全性；如果将来需要对抗对抗性篡改，升级到 BLAKE3。

---

## 8. gRPC 协议扩展

### 8.1 现有协议分析

`proto/control.proto` 当前定义了三个 RPC：

```protobuf
service ControlPlane {
    rpc Discover(DiscoverRequest) returns (DiscoverResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Deregister(DeregisterRequest) returns (DeregisterResponse);
}
```

`RegionType::FREE_LIST` 和 `LARGE_OBJECT` 已声明但未正确初始化（`vaddr: 0, rkey: 0`）。

### 8.2 新增 SyncFreeList RPC

```protobuf
// ====== 新增消息 ======

/// Server 推送回收的空闲 extent offset 列表。
/// 由控制面发起（server push），非请求/响应模式。
message SyncFreeListRequest {
    /// 回收的 extent 在 FreeList 区域中的字节偏移量列表。
    repeated uint64 freed_offsets = 1;

    /// Server 当前 generation，用于 Client 校验一致性。
    uint64 generation = 2;
}

/// Client 确认收到回收列表。
message SyncFreeListResponse {
    /// Client ID，用于 Server 确认。
    uint64 client_id = 1;
}

// ====== 扩展现有服务 ======
service ControlPlane {
    // 已有 RPC ...
    rpc Discover(DiscoverRequest) returns (DiscoverResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Deregister(DeregisterRequest) returns (DeregisterResponse);

    // 新增：Server 推送回收 extent 列表给 Client
    //
    // 双向流式 RPC：Server 在 GC 周期内推送，Client 确认接收。
    // 非请求/响应模式——Server 主动推送。
    rpc SyncFreeList(SyncFreeListRequest) returns (SyncFreeListResponse);
}
```

**设计要点**：

| 决策 | 理由 |
|------|------|
| 使用**双向流式 RPC**而非单次请求/响应 | GC 是周期性事件，双向流允许 Server 随时推送而不需 Client 轮询 |
| `freed_offsets` 使用 `repeated uint64` | 一次推送多个 offset，减少 RPC 调用次数（每 GC 周期通常仅 1 次推送） |
| 包含 `generation` 字段 | Server 重启后 generation 变化，Client 可据此丢弃旧数据并重新同步 |
| 频率：每个 GC 周期推送一次 | GC 周期通常为 100ms–1s，远低于数据面操作频率 |

### 8.3 RegionMetadata vaddr/rkey 修正

Server 侧实现中，`FREE_LIST` 和 `LARGE_OBJECT` 区域的 `RegionMetadata` 必须填写实际的 MR 元数据：

```rust
/// 在 Server 启动时注册内存区域并填充元数据。
fn build_region_metadata(
    hash_table_mr: &MemoryRegion,
    large_object_mr: &MemoryRegion,
    free_list_mr: &MemoryRegion,
    generation: u64,
    bucket_count: u64,
) -> ServerMetadata {
    ServerMetadata {
        generation,
        bucket_count,
        regions: vec![
            RegionMetadata {
                vaddr: hash_table_mr.vaddr(),
                rkey: hash_table_mr.rkey(),
                size: hash_table_mr.size(),
                r#type: RegionType::HashTable as i32,
                generation,
            },
            RegionMetadata {
                vaddr: large_object_mr.vaddr(),  // ← 之前为 0
                rkey: large_object_mr.rkey(),     // ← 之前为 0
                size: large_object_mr.size(),
                r#type: RegionType::LargeObject as i32,
                generation,
            },
            RegionMetadata {
                vaddr: free_list_mr.vaddr(),      // ← 之前为 0
                rkey: free_list_mr.rkey(),         // ← 之前为 0
                size: free_list_mr.size(),
                r#type: RegionType::FreeList as i32,
                generation,
            },
        ],
    }
}
```

### 8.4 完整的 proto 文件（变更部分）

```protobuf
// 对 proto/control.proto 的增量变更

// ====== 修改 RegionMetadata ======
// 原来：message RegionMetadata { ... }
// 保持不变，但 Server 实现中 vaddr/rkey 必须填写实际值

// ====== 新增消息 ======
message SyncFreeListRequest {
    repeated uint64 freed_offsets = 1;
    uint64 generation = 2;
}

message SyncFreeListResponse {
    uint64 client_id = 1;
}

// ====== 扩展 ControlPlane ======
service ControlPlane {
    rpc Discover(DiscoverRequest) returns (DiscoverResponse);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Deregister(DeregisterRequest) returns (DeregisterResponse);
    // 新增
    rpc SyncFreeList(stream SyncFreeListRequest) returns (SyncFreeListResponse);
}
```

---

## 9. 并发安全分析

### 9.1 CAS Bump Allocator 安全性证明

**命题**：CAS Bump Allocator 确保任意时刻，任意两个 Client 获取的 extent 区间不重叠。

**证明**（反证法）：

1. 假设 Client A 和 Client B 同时获得有重叠的区间。
2. 设 Client A 的 CAS 操作为 `CAS(bump_offset, old_A → old_A + total_A)`。
3. 设 Client B 的 CAS 操作为 `CAS(bump_offset, old_B → old_B + total_B)`。
4. 区间重叠意味着 `[old_A, old_A+total_A)` 与 `[old_B, old_B+total_B)` 有交集。
5. 由于 `bump_offset` 在每个 CAS 操作间保持不变或被成功推进，两次成功的 CAS 的 `old` 值必然满足 `old_A < old_B` 或 `old_B < old_A`。
6. 若 CAS_A 先成功，则 CAS_B 的 `expected` 值必须是 `old_A + total_A`（CAS_A 将 bump_offset 更新为此值），因此 `old_B = old_A + total_A`，区间为 `[old_A+total_A, old_A+total_A+total_B)`，与 A 的区间不重叠。
7. 若 CAS_B 先成功，同理不重叠。
8. 矛盾，故假设不成立。 ∎

### 9.2 无 ABA 问题

`bump_offset` 是单调递增的（只在 CAS Bump 路径中使用），从不递减或回卷到已使用过的值。因此不存在经典的 ABA 问题（CAS 的 expected 值在两次读取之间被修改然后又改回原值）。

```
时间线：
  t0: bump_offset = 100    ← Client A reads
  t1: bump_offset = 200    ← Client B CAS success
  t2: bump_offset = 300    ← Client C CAS success
  t3: Client A CAS expected=100 ← 失败！bump_offset 已经是 300
  t4: bump_offset = 100    ← 不可能！bump_offset 从不回退
```

### 9.3 本地空闲列表的并发安全

当 Client 从本地空闲列表分配时（路径 A），使用**对 magic 字段的 CAS** 来防止冲突：

```rust
// 两个 Client 同时尝试使用 offset 128：
//
// Client A: CAS(magic@128, expect=0, desired=EXTENT_MAGIC) → success
// Client B: CAS(magic@128, expect=0, desired=EXTENT_MAGIC) → fail (magic already EXTENT_MAGIC)
//
// Client B 随后跳过 offset 128，尝试下一个。
```

这本质上是一个**每 extent 粒度的 CAS 锁**。因为 magic 字段只在写入 header 时被原子设置为 `EXTENT_MAGIC`，且在 GC 回收时被清零，所以不存在 ABA 问题（清空 magic 的操作由 Server GC 线程执行，与 Client 分配不存在竞态）。

### 9.4 校验和协议的并发安全

Reader 的校验和验证不涉及任何原子操作或锁，是纯乐观读（读取→验证→重试）：

```
Reader:
  checksum = read(header.checksum)
  if checksum == 0: retry
  data = read(payload)
  if xxh64(data) != checksum: reject
  else: accept
```

不变量：由于 checksum 是最后写入的字段（阶段 3），Reader 看到非零 checksum 时，数据一定已经写入完成。违反此不变量的唯一情况是 RDMA 网络乱序使得 checksum WRITE 先于 data WRITE 到达——但因为校验和写入是**最后一个操作**（阶段 3），在 Client 完成所有三个阶段之前 checksum 保持为 0。

---

## 10. 性能分析

### 10.1 单次分配延迟分解

| 步骤 | 操作 | 估计延迟 (μs) | 累计 (RTT) |
|------|------|---------------|-----------|
| ① | RDMA READ bump_offset | 2 | 1 RTT |
| ② | 本地计算 extent_total | < 0.01 | — |
| ③ | RDMA CAS bump_offset | 3 | 2 RTT |
| ④ | RDMA WRITE header (32B) | 1.5 | 2.5 RTT |
| ⑤ | RDMA WRITE data (变长) | bandwidth-limited | — |
| ⑥ | RDMA WRITE checksum (8B) | 1.5 | 3.5 RTT |

**总计**（不含数据传输时间）：约 **4 RTT × 2μs/RTT = 8μs** 的分配开销。

对于 100KB 的 extent，数据传输时间（以 100Gbps 计）约 8μs，总延迟约 16μs。

### 10.2 与本地分配对比

| 维度 | 本地 `LargeObjectRegion` | CAS Bump Allocator | 倍数 |
|------|--------------------------|-------------------|------|
| 分配延迟 | < 1μs (内存操作) | ~8μs (4 RTT) | ~8× |
| 适用范围 | 单进程（测试） | 多 Client 分布式 | — |
| 并发安全 | 外部需加锁 | CAS 原子操作，无外部锁 | — |
| 回收复用 | 本地 free_list 搜索 | 控制面推送 + 本地缓存 | — |

**结论**：8μs 的分配开销在 extent 分配的上下文中完全可以接受，因为 extent 分配仅在 KV 第一次写入时发生（相对低频），且 extent 数据通常为 MB 级大对象，传输时间主导总延迟。

### 10.3 CAS 竞争分析

**场景**：N 个 Client 以速率 R 分配 extent。

`bump_offset` 的 CAS 竞争模型为 **M/G/1 队列的退避**：CAS 操作 RTT 约 3μs，冲突窗口约 3μs。

| 分配速率 (per Client) | Client 数 | 总分配速率 | 冲突概率 | 平均 CAS 尝试次数 |
|------------------------|-----------|------------|----------|-------------------|
| 10K/s | 4 | 40K/s | < 0.01% | ~1.00 |
| 50K/s | 4 | 200K/s | < 0.1% | ~1.01 |
| 500K/s | 4 | 2M/s | ~1% | ~1.03 |
| 5M/s | 4 | 20M/s | ~10% | ~1.15 |

**结论**：在典型的 extent 分配场景（< 10K/s per Client），CAS 竞争开销可忽略不计。

### 10.4 内存效率

- **头部开销**：32 字节 / extent。对于 1MB extent，开销比为 0.003%。
- **对齐浪费**：平均 4 字节 / extent（8 字节对齐）。
- **碎片化**：bump 分配本身不产生碎片（所有 extent 连续分配）。回收路径引入 hole 碎片，但由于优先使用本地空闲列表，碎片率可控。

---

## 11. 故障模型与降级

### 11.1 Client 写入过程中崩溃

**场景**：Client 在阶段 1（写入 header）或阶段 2（写入数据）之间崩溃，checksum 未写入或为 0。

**处理**：
- Reader 读到 `checksum == 0`，等待重试（最多 4 次，每次 10μs）。
- 超时后返回 `ExtentError::WriteInProgress`。
- 该 extent interval 实际上被"泄漏"——bump_offset 已经推进，但数据不可用。
- **缓解**：Server GC 线程可周期扫描 FreeList 区域，将 `checksum == 0` 且超过一定时间的 extent 标记为废弃，并通过下次 `SyncFreeList` 推送回收其 offset。

### 11.2 CAS 操作网络超时

**场景**：RDMA CAS 操作因网络故障超时。

**处理**：
- Client 无法确定 CAS 是否在远端实际执行。
- **安全策略**：假设 CAS **未执行**（保守假设），使用一个新的 `RDMA READ` 读取 `bump_offset` 的最新值。
  - 若最新值 ≤ `old`：CAS 确实未执行 → 重试。
  - 若最新值 ≥ `old + total`：CAS 可能已执行（已分配） → 跳过该 extent。
- 对于可能已被分配的 interval，后续 GC 扫描可将其标记为废弃并回收。

### 11.3 Server 重启

**场景**：Server 重启后 FreeList 区域内容丢失或重分配了新的 MR。

**处理**：
- Server 启动时广播新 `generation_id`（递增），并重建 FreeList 区域（`bump_offset = 64`）。
- Client 通过心跳检测到 `generation` 变化后：
  1. 丢弃所有本地缓存的 extent offset。
  2. 重新执行 `Discover` 获取新的 `vaddr`/`rkey`。
  3. 重新打开所有 QP。

### 11.4 FreeList 区域耗尽

**场景**：所有 extent 区间被分配完毕（`bump_offset + total > free_list_size`），且本地空闲列表为空。

**处理**：
- 返回 `ExtentError::OutOfSpace`。
- 上层（Cuckoo 哈希插入）返回 `KV_FULL`。
- 触发 LRU 淘汰 (Wave 10) 以释放空间。

---

## 12. 边界情况与测试策略

### 12.1 边界情况清单

| 边界情况 | 预期行为 | 测试方法 |
|----------|----------|----------|
| 空数据 extent（`data_len = 0`） | 分配成功，仅含 header | 单元测试 |
| single byte extent（`data_len = 1`） | extent_total = 40 (32+1 对齐到 40) | 单元测试 |
| FreeList 区域仅够一个 header | OutOfSpace | 单元测试 |
| CAS 竞争（4 Client 并发分配） | 所有分配无重叠 | 集成测试 |
| 本地空闲列表耗尽后回退到 CAS bump | 正确的回退行为 | 集成测试 |
| GC 推送的 offset 被两个 Client 同时尝试 | CAS 竞争，仅一个成功 | 并发测试 |
| Client 崩溃后 Reader 读到 checksum=0 | 重试后超时，返回 WriteInProgress | 故障注入 |
| 校验和 RAED 比数据 READ 先到达 | 校验和验证失败 → 重试 | 网络模拟 |
| V1 header 格式 extent 被读取 | 正确识别 version=0，按 24 字节解析 | 兼容性测试 |
| bump_offset 推进到区域边界（最后 1 字节） | OutOfSpace（因为最小 extent_total 为 32） | 单元测试 |
| 极端碎片化（1K+ hole） | 本地空闲列表 > 上限，丢弃最旧 | 压力测试 |

### 12.2 测试用例骨架

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// 测试：CAS bump 分配无重叠。
    /// 4 个 Client 并发分配 1000 个 extent，验证所有 offset 不重叠。
    #[tokio::test]
    async fn test_concurrent_cas_no_overlap() {
        let server = TestServer::start().await;
        let clients = (0..4).map(|_| TestClient::connect(&server).await).collect::<Vec<_>>();

        let handles: Vec<_> = clients.iter().map(|c| {
            tokio::spawn(async move {
                let mut offsets = Vec::new();
                for _ in 0..1000 {
                    let data = rand_data(1..1024);
                    let offset = c.allocate(&data).await.unwrap();
                    offsets.push((offset, extent_total(data.len() as u32)));
                }
                offsets
            })
        }).collect();

        let all_offsets: Vec<_> = futures::future::join_all(handles).await
            .into_iter()
            .flat_map(|r| r.unwrap())
            .collect();

        assert_no_overlap(&all_offsets);
    }

    /// 测试：校验和协议——写入中崩溃后 Reader 能正确处理。
    #[tokio::test]
    async fn test_checksum_write_in_progress() {
        let server = TestServer::start().await;
        let client = TestClient::connect(&server).await;
        let reader = TestClient::connect(&server).await;

        // 模拟部分写入（只写 header，不写 checksum）
        let offset = client.write_header_only(b"test data").await.unwrap();

        // Reader 尝试读取 → 应检测到 checksum=0 并重试
        let result = reader.read_extent(offset).await;
        assert!(matches!(result, Err(ExtentError::WriteInProgress)));
    }

    /// 测试：V1 兼容性。
    #[tokio::test]
    async fn test_v1_compatibility() {
        // 手动构造一个 V1 格式的 extent
        let server = TestServer::start().await;
        let v1_data = create_v1_extent(b"v1 data");
        let offset = write_raw(&server, &v1_data).await;

        let reader = TestClient::connect(&server).await;
        let result = reader.read_extent(offset).await.unwrap();
        assert_eq!(result, b"v1 data");
    }
}
```

---

## 13. 附录 A：方案对比（为何选择 CAS Bump Allocator）

Oracle 审查了三类方案：

### 方案 A：CAS Bump Allocator ✅（选定）

**机制**：共享 `bump_offset` + RDMA CAS 原子推进。

| 维度 | 评价 |
|------|------|
| Server CPU 参与 | 零（数据面） |
| 并发安全 | CAS 原子操作，数学可证无冲突 |
| 碎片化 | bump 路径无碎片；回收路径有 hole，通过本地空闲列表缓解 |
| 实现复杂度 | 中（新增 ~300 行 Rust + proto 扩展） |
| 性能 | 4 RTT，竞争率低 |

### 方案 B：Server Pre-Alloc ❌（排除）

**机制**：Server CPU 预分配 extent 区间并推送给 Client。

| 维度 | 评价 |
|------|------|
| Server CPU 参与 | **有**（违反核心原则 #1） |
| 并发安全 | Server 预分配避免冲突，但无法动态适应负载 |
| 决策理由 | **违反核心原则：Server CPU 在数据面零参与** |

### 方案 C：Partitioned Extent ❌（排除）

**机制**：按 Client ID 哈希分区，每 Client 拥有专属 extent 区域。

| 维度 | 评价 |
|------|------|
| Server CPU 参与 | 零 |
| 并发安全 | 完全无冲突 |
| 空间利用率 | **低**：分区固定，部分 Client 空闲时浪费空间 |
| 决策理由 | 空间利用率不可控，LMCache 场景需要弹性大对象分配 |

### 最终选择

**CAS Bump Allocator** 是唯一同时满足三个核心约束的方案：
1. 数据面零 CPU — 分配路径无 Server 参与。
2. 无冲突 — CAS 原子推进，数学可证。
3. 空间利用率高 — 所有 Client 共享一个大区域。

---

## 14. 附录 B：完整代码示例

### 14.1 Client 侧完整分配函数

```rust
/// 远端 extent 分配器的完整实现。
///
/// 整合了两条分配路径：
/// - 路径 A：本地空闲列表（来自 SyncFreeList），CAS 在 magic 上抢占。
/// - 路径 B：CAS bump 分配。
pub struct DistributedExtentAllocator {
    free_list_vaddr: u64,
    free_list_rkey: u32,
    free_list_size: u64,
    local_free_list: VecDeque<u64>,
}

impl DistributedExtentAllocator {
    /// 分配一个 extent 并写入给定数据。
    ///
    /// 返回 extent 在 FreeList 区域内的 **相对偏移量**
    /// （即相对于 `free_list_vaddr` 的偏移）。
    pub async fn allocate_and_write(
        &mut self,
        rdma: &dyn RdmaTransport,
        data: &[u8],
    ) -> Result<u64, ExtentError> {
        let data_len = data.len() as u32;
        let total = extent_total(data_len);

        // ── 路径 A：本地空闲列表 ──
        if let Some(free_offset) = self.allocate_from_local_free(rdma, data, total).await? {
            return Ok(free_offset);
        }

        // ── 路径 B：CAS bump ──
        self.allocate_by_cas_bump(rdma, data, total).await
    }

    /// 路径 A：从本地空闲列表分配。
    async fn allocate_from_local_free(
        &mut self,
        rdma: &dyn RdmaTransport,
        data: &[u8],
        total: u64,
    ) -> Result<Option<u64>, ExtentError> {
        const MAX_LOCAL_TRIES: usize = 16;

        for _ in 0..MAX_LOCAL_TRIES.min(self.local_free_list.len()) {
            let offset = self.local_free_list.pop_front().unwrap();
            let extent_vaddr = self.free_list_vaddr + offset;

            // CAS 在 magic 字段上抢占
            let cas_ok = rdma.cas_u32(
                extent_vaddr,               // magic 在 offset 0
                self.free_list_rkey,
                0u32,                       // expected: magic 为 0（空闲）
                EXTENT_MAGIC,
            ).await?;

            if cas_ok {
                // 抢占成功
                self.write_extent_ordered(rdma, extent_vaddr, data).await?;
                return Ok(Some(offset));
            }
            // 抢占失败，跳过此 offset
        }
        Ok(None)
    }

    /// 路径 B：CAS bump 分配。
    async fn allocate_by_cas_bump(
        &self,
        rdma: &dyn RdmaTransport,
        data: &[u8],
        total: u64,
    ) -> Result<u64, ExtentError> {
        const MAX_RETRIES: u32 = 32;
        let header_vaddr = self.free_list_vaddr;  // FreeListHeader 在区域起始

        for retry in 0..MAX_RETRIES {
            // ① Read bump_offset
            let old_offset = rdma.read_u64(
                header_vaddr,  // bump_offset 在 FreeListHeader 偏移 0
                self.free_list_rkey,
            ).await?;

            // ② 检查空间
            let new_offset = old_offset + total;
            if new_offset > self.free_list_size {
                return Err(ExtentError::OutOfSpace);
            }

            // ③ CAS bump_offset
            let cas_ok = rdma.cas_u64(
                header_vaddr,
                self.free_list_rkey,
                old_offset,
                new_offset,
            ).await?;

            if cas_ok {
                // CAS 成功：拥有区间 [old_offset, new_offset)
                let extent_vaddr = self.free_list_vaddr + old_offset;
                self.write_extent_ordered(rdma, extent_vaddr, data).await?;
                return Ok(old_offset);
            }
            // CAS 失败 → 继续重试
        }

        Err(ExtentError::AllocContention)
    }

    /// 三段式写入：header(checksum=0) → data → checksum。
    async fn write_extent_ordered(
        &self,
        rdma: &dyn RdmaTransport,
        extent_vaddr: u64,
        data: &[u8],
    ) -> Result<(), ExtentError> {
        let checksum = xxhash64(data, 0);

        // 阶段 1: 写入 header (checksum = 0)
        let header = ExtentHeaderV2 {
            magic: EXTENT_MAGIC,
            version: 1,
            data_len: data.len() as u32,
            epoch_mark: 0,
            checksum: 0,            // ← 标记写入进行中
            _pad: [0u8; 7],
        };
        let header_bytes: &[u8] = bytemuck::bytes_of(&header);
        rdma.write(extent_vaddr, self.free_list_rkey, header_bytes).await?;

        // 阶段 2: 写入负载数据
        rdma.write(
            extent_vaddr + HEADER_SIZE_V2,
            self.free_list_rkey,
            data,
        ).await?;

        // 阶段 3: 写入校验和 (仅 8 字节，覆盖 checksum 字段)
        let checksum_bytes = checksum.to_le_bytes();
        rdma.write(
            extent_vaddr + 16, // checksum 字段在 header 中的偏移
            self.free_list_rkey,
            &checksum_bytes,
        ).await?;

        Ok(())
    }

    /// 从 SyncFreeList RPC 合并 Server 推送的回收 offset。
    pub fn merge_freed_offsets(&mut self, offsets: &[u64]) {
        for &offset in offsets {
            self.local_free_list.push_back(offset);
        }
        // 上限裁剪
        while self.local_free_list.len() > 1024 {
            self.local_free_list.pop_front();
        }
    }
}
```

### 14.2 Server 侧 FreeList 区域初始化

```rust
/// 在 Server 启动时为 FreeList 区域分配 HugePages 并注册 RDMA MR。
pub fn init_free_list_region(
    ctx: &RdmaContext,
    pd: &ProtectionDomain,
    size: usize,
) -> Result<MemoryRegion, RdmaError> {
    // 1. 分配 HugePages 内存
    let huge_pages = HugePages::allocate(size)?;
    let buf = huge_pages.as_slice_mut();

    // 2. 初始化 FreeListHeader：bump_offset = 64（头部大小）
    let header = FreeListHeader {
        bump_offset: 64u64.to_le(),  // ← 注意内存序：Shared memory 使用 little-endian
        _pad: [0u8; 56],
    };
    let header_bytes: &[u8] = bytemuck::bytes_of(&header);
    buf[..64].copy_from_slice(header_bytes);
    buf[64..].fill(0u8);  // 剩余区域清零

    // 3. 注册 MR
    let mr = pd.register_memory_region(
        buf.as_ptr() as *mut c_void,
        size,
        ibv_access_flags::LOCAL_WRITE
            | ibv_access_flags::REMOTE_READ
            | ibv_access_flags::REMOTE_WRITE
            | ibv_access_flags::REMOTE_ATOMIC,  // ← FREE_LIST 需要 REMOTE_ATOMIC 以支持 CAS
    )?;

    Ok(mr)
}
```

### 14.3 Server 侧 SyncFreeList gRPC Handler

```rust
/// Server 端 SyncFreeList handler。
/// GC 线程在扫出回收的 extent 后调用此 handler 推送给所有连接的 Client。
pub async fn handle_sync_free_list(
    clients: &Arc<RwLock<ClientRegistry>>,
    freed_offsets: &[u64],
    generation: u64,
) {
    let request = SyncFreeListRequest {
        freed_offsets: freed_offsets.to_vec(),
        generation,
    };

    let clients = clients.read().await;
    for client in clients.iter_active() {
        // 异步发送：不阻塞 GC 线程
        let req = request.clone();
        let mut stream = client.free_list_stream.clone();
        tokio::spawn(async move {
            if let Err(e) = stream.send(req).await {
                tracing::warn!(client_id = client.id, error = %e,
                    "Failed to push SyncFreeList to client");
            }
        });
    }
}
```

---

## 文档评审记录

| 版本 | 日期 | 评审者 | 评审结论 | 变更摘要 |
|------|------|--------|----------|----------|
| v1 | 2026-08-02 | Oracle (AI Agent) | 通过，建议修正校验和写入顺序 | 初始版本 |
| v1 | 2026-08-02 | — | Oracle 修正已采纳 | 将校验和写入从 "先写 checksum→后写数据"修正为"先写 header(checksum=0)→后写数据→最后写 checksum" |

---

> **下一步**：本协议文档通过 Oracle 审查后，进入 T9-B 实现阶段（`src/engine/layout.rs` 新增 FreeListHeader + ExtentHeaderV2、`src/engine/extent.rs` 新增 DistributedExtentAllocator、`src/client/write.rs` 集成 extent 分配、`proto/control.proto` 新增 SyncFreeList RPC、`src/control/server.rs` 补全 FREE_LIST 区域元数据）。
