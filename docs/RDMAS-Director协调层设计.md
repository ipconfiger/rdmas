# RDMAS PD 分离协调层设计

> 借鉴 Mooncake Conductor 架构，在 RDMAS 存储引擎之上构建轻量级 PD（Prefill-Decode）分离协调器

---

## 一、Mooncake Conductor 架构回顾

Mooncake Conductor 是 Mooncake 的 KV-cache 感知路由器。**它不移动数据，只维护索引**。

```
                    POST /query (token_ids, model)
Router ─────────────────────────────────────────────→ Conductor
                                                          │
        ┌─────────────────────────────────────────────────┤
        │  PrefixCacheTable (prefix hash → instance map)  │
        │  EventManager (HTTP + 动态注册 + ZMQ 订阅)        │
        └─────────────────────────────────────────────────┘
                                                          │
        ┌─────────────────────────────────────────────────┘
        │  ZMQClient ← 订阅 ← vLLM/SGLang KV 事件流
        │  (BlockStored / BlockRemoved)
        ▼
  返回: "vllm-prefill-node3 有最长 prefix 命中, 3 个 tier"
```

### Conductor 核心组件

| 组件 | 职责 |
|------|------|
| `EventManager` | HTTP 服务、动态注册、服务映射、查询分发 |
| `ZMQClient` | 从推理引擎订阅 KV 事件 (BlockStored/BlockRemoved) |
| `KVEventHandler` | 将引擎事件标准化为 Conductor 内部事件 |
| `PrefixCacheTable` | 维护 prefix hash → instance 的多维索引 |

### Conductor 不做什么

- ❌ 不存储 KV cache 本身（数据在 Store 或推理引擎本地）
- ❌ 不参与数据传输（由 Transfer Engine 负责）
- ❌ 不做路由决策（由 Router 根据查询结果自行决策）

---

## 二、RDMAS 协调层设计：`RDMAS-Director`

### 定位

在 RDMAS One-Sided RDMA 存储引擎之上，构建一个**轻量级 KV-cache 感知调度层**，使 PD 分离推理系统能智能选择缓存命中率最高的 RDMAS 节点。

### 与 Mooncake Conductor 的差异

| 维度 | Mooncake Conductor | RDMAS Director |
|------|-------------------|----------------|
| **事件订阅** | ZMQ 直连推理引擎 | 通过 LMCache connector 内置上报 |
| **数据传输** | Transfer Engine（有 CPU 协调） | One-Sided RDMA（零 CPU） |
| **索引粒度** | Block hash → instance | Block hash → RDMAS server node |
| **语言** | Go | Rust（复用现有代码库） |
| **部署** | 独立进程 | 可选嵌入 ControlServer 或独立 |
| **prefix 计算** | token IDs → block hash | 同（通过 LMCache 传递） |

### 架构

```
                          Director HTTP/gRPC
Router ──query(token_ids)──→ ┌──────────────────────┐
                              │  PrefixIndex          │
                              │  block_hash → node    │
                              │  tenant → nodes       │
                              └──────────┬───────────┘
                                         │
                    更新索引              │ 查询
                                         │
     ┌───────────────────────────────────┤
     │                                   │
     ▼                                   ▼
  LMCache Connector               RDMAS Control Plane
  (put/get 事件上报)               (节点注册/心跳/发现)
     │                                   │
     │ One-Sided RDMA                    │
     ▼                                   ▼
  RDMAS Node 0   RDMAS Node 1   ...   RDMAS Node N
  HugePage KV    HugePage KV          HugePage KV
```

### 数据流

```
vLLM Prefill:
  1. 生成 KV cache block
  2. LMCache Connector: submit_batch_set(keys, blocks)
     → RDMAS Director 上报: "block_hash_X stored on node_3"
  3. RDMAS 客户端 One-Sided RDMA WRITE → Node 3 HugePage

vLLM Decode (新请求):
  1. Router 提取 token_ids, 计算 prefix
  2. POST /query → RDMAS Director: "prefix_ABC 在哪?"
  3. Director 返回: "node_3 命中最多, node_5 次之"
  4. Router 将请求调度到 node_3 附近的 decode worker
  5. Decode worker 通过 LMCache Connector 从 Node 3 One-Sided RDMA READ
     → 零拷贝加载 KV cache, 继续生成
```

---

## 三、Director 核心数据结构

### PrefixIndex

```rust
/// 全局 prefix 索引：prefix hash → 命中的节点列表
struct PrefixIndex {
    /// (tenant, model, block_size, salt) → per-context index
    contexts: HashMap<ModelContext, ContextIndex>,
}

struct ModelContext {
    tenant_id: String,
    model_name: String,
    block_size: u32,
    additional_salt: String,
}

struct ContextIndex {
    /// block_hash → (node_ids, replica_count, last_access)
    entries: HashMap<u64, BlockEntry>,
}

struct BlockEntry {
    /// 存储该 block 的 RDMAS 节点 ID 列表
    node_ids: Vec<NodeId>,
    /// 该 block 的访问热度（用于 eviction 决策）
    access_count: AtomicU64,
    /// 最后访问时间戳
    last_access: AtomicU64,
}
```

### Query API

```protobuf
message QueryRequest {
    string tenant_id = 1;
    string model_name = 2;
    repeated uint64 token_ids = 3;  // prompt tokens
    uint32 block_size = 4;
    string lora_name = 5;
    string cache_salt = 6;
}

message QueryResponse {
    message NodeHit {
        string node_id = 1;
        uint32 matched_blocks = 2;       // 连续命中 block 数
        repeated uint32 dp_ranks = 3;    // DP rank 命中的 block 数
    }
    repeated NodeHit hits = 1;  // 按命中数降序排列
}
```

---

## 四、事件上报机制

### 相比 Conductor 的改进

Mooncake Conductor 用 ZMQ 从推理引擎拉取事件。RDMAS Director 可以**内嵌在 LMCache Connector 中**，每次 `submit_batch_set`/`submit_batch_delete` 自动上报索引变更。

```rust
impl RDMANativeConnector {
    fn submit_batch_set(&self, keys: Vec<String>, mvs: Vec<PyObject>) -> u64 {
        // 原有存储逻辑...
        let future_id = ...;

        // 新增：上报 Director
        if let Some(director) = &self.director {
            for key in &keys {
                let block_hash = hash_key(key);
                director.report_store(
                    self.node_id,
                    block_hash,
                    &self.model_context,
                );
            }
        }

        future_id
    }
}
```

### 优势

| | Mooncake Conductor | RDMAS Director |
|--|-------------------|----------------|
| 上报方式 | ZMQ 外部拉取（需额外进程） | Connector 内嵌推送（零额外进程） |
| 延迟 | ZMQ 网络 + 反序列化 | 本地 gRPC / 内存写入 |
| 故障隔离 | ZMQ 连接断开需重连 | 内存操作，天然可靠 |
| 部署复杂度 | Conductor 独立部署 + ZMQ 端口 | 随 Connector 一起部署 |

---

## 五、提高 PD 分离效率的具体手段

### 5.1 Cache-Aware 调度

```
传统 PD 分离:
  Prefill Worker → 生成 KV → 发送到任意 Decode Worker
  问题: Decode Worker 可能没有相关 KV cache → 冷启动

RDMAS Director 调度:
  Prefill Worker → 生成 KV → 存储到 RDMAS Node X
  Director 记录: prefix_ABC → Node X
  新请求到来:
    Router 查询 Director → "Node X 有 prefix_ABC"
    → 调度到 Node X 附近的 Decode Worker
    → One-Sided RDMA READ 零拷贝加载
    → Decode Worker 热缓存启动
```

### 5.2 多 Tier 感知

```rust
// Director 返回命中信息包含 tier 信息
struct NodeHit {
    node_id: String,
    matched_blocks: u32,
    tiers: Vec<CacheTier>,  // DRAM / SSD / remote
}

enum CacheTier {
    Dram,       // RDMAS One-Sided RDMA < 5μs
    Ssd,         // 未来: NVMe-oF
    RemoteNode,  // 跨节点 RDMAS
}
```

Router 可以根据 tier 信息做调度：
- `Dram` 命中 → 直接调度到该节点（最快）
- `Ssd` 命中 → 调度 + 预取到 DRAM（温启动）
- `RemoteNode` 命中 → 调度 + 跨节点 RDMA READ（但仍比冷启动快）

### 5.3 与 LMCache 的深度整合

```
LMCache L2 后端选择:
  if (director.query(token_ids).best_node == local_node) {
    使用 RDMAS local connector（零网络延迟）
  } else {
    使用 RDMAS remote connector（One-Sided RDMA READ）
  }
```

RDMAS Director 本质上成为 LMCache 的**智能路由层**，让 LMCache 不必自己猜测哪个节点有缓存。

---

## 六、实现路线图

| Phase | 内容 | 改动量 | 预计时间 |
|-------|------|--------|---------|
| **P1: 内嵌上报** | Connector 中 `report_store/report_remove` 原型 | ~100 行 | 0.5 天 |
| **P2: PrefixIndex** | `PrefixIndex` 内存数据结构 + 查询 API | ~300 行 | 1 天 |
| **P3: gRPC 接口** | Director Proto + Server + Client | ~200 行 | 1 天 |
| **P4: LMCache 集成** | LMCache adapter 选择最佳 RDMAS 节点 | ~150 行 | 1 天 |
| **P5: 多 Tier** | SSD tier + 跨节点 tier 支持 | ~200 行 | 2 天 |

总计约 5.5 天可以完成一个可用的 RDMAS Director 原型。

---

## 七、与 Mooncake Conductor 的本质差异

| | Mooncake | RDMAS Director |
|--|----------|----------------|
| **数据面** | Transfer Engine (CPU 协调) | One-Sided RDMA (零 CPU) |
| **index 更新** | ZMQ 外部拉取 | Connector 内嵌推送 |
| **index 延迟** | ZMQ 网络 RTT | 同进程内存写入 |
| **部署模型** | 独立 Go 进程 + etcd | 可选嵌入或独立 Rust 进程 |
| **prefix 计算** | token IDs → block hash | 同，LMCache 传入 |
| **核心价值** | 通用 KVCache 索引 | RDMAS 专用 + 零 CPU 数据面 |

**Mooncake Conductor 是通用方案，RDMAS Director 是专用深度优化。**
两者的核心哲学一致：用轻量级元数据索引驱动高效的数据路由，但 RDMAS Director 因为内嵌在 Connector 中且数据走 One-Sided RDMA，理论上上报延迟更低、数据面更高效。

---

## 八、PD 角色配置与动态比例调整

### 8.1 核心原则：RDMAS 节点不区分 P/D

RDMAS 节点是**共享存储层**，不承担推理角色。P/D 分离是推理引擎的部署职责：

```
                ┌──────────┐  ┌──────────┐
  Prefill Pool  │ P0 │ P1  │  │ P2 │ P3  │   ← vLLM/SGLang prefill workers
                └──┬───┴──┬─┘  └──┬───┴──┬─┘
                   │ RDMA │       │ RDMA │
                   ▼      ▼       ▼      ▼
                ┌──────────────────────────┐
                │      RDMAS Cluster       │  ← 共享存储，不区分 P/D
                │   Node0  Node1  Node2    │
                └──────────────────────────┘
                   ▲      ▲       ▲      ▲
                   │ RDMA │       │ RDMA │
                ┌──┴───┴──┬─┐  ┌──┴───┴──┬─┐
  Decode Pool   │ D0 │ D1  │  │ D2 │ D3  │   ← vLLM/SGLang decode workers
                └──────────┘  └──────────┘
```

### 8.2 如何在 Director 中注册角色

Director 通过注册接口感知哪个 instance 是什么角色：

```protobuf
message RegisterRequest {
    string instance_id = 1;     // "prefill-node-0"
    InstanceRole role = 2;     // PREFILL / DECODE / BOTH
    string tenant_id = 3;
    string model_name = 4;
    string rpc_endpoint = 5;   // 推理引擎的 RPC 地址
    uint32 dp_rank = 6;
    string node_id = 7;        // 绑定的 RDMAS 节点（可选）
    uint32 block_size = 8;
}

enum InstanceRole {
    PREFILL = 0;   // 只做 prefill（生成 KV cache）
    DECODE = 1;    // 只做 decode（消费 KV cache）
    BOTH = 2;      // 混合模式
}
```

### 8.3 8 实例集群配置示例

#### 场景 A: 4P + 4D（均衡）

```json
// Director 注册
[
  {"instance_id": "p0", "role": "PREFILL",  "node_id": "rdmas-0"},
  {"instance_id": "p1", "role": "PREFILL",  "node_id": "rdmas-0"},
  {"instance_id": "p2", "role": "PREFILL",  "node_id": "rdmas-1"},
  {"instance_id": "p3", "role": "PREFILL",  "node_id": "rdmas-1"},
  {"instance_id": "d0", "role": "DECODE",   "node_id": "rdmas-0"},
  {"instance_id": "d1", "role": "DECODE",   "node_id": "rdmas-0"},
  {"instance_id": "d2", "role": "DECODE",   "node_id": "rdmas-1"},
  {"instance_id": "d3", "role": "DECODE",   "node_id": "rdmas-1"}
]
```

#### 场景 B: 6P + 2D（写多读少，长输出场景）

```json
[
  {"instance_id": "p0".."p5", "role": "PREFILL",  "node_id": "rdmas-0"},
  {"instance_id": "d0".."d1", "role": "DECODE",   "node_id": "rdmas-0"},
]
```

#### 场景 C: 动态切换（峰值弹性）

```json
// 白天：4P+4D 均衡
// 夜间：2P+6D 节省 prefill 算力
POST /admin/reconfigure
{
  "instances": {
    "p2": {"role": "DECODE"},  // prefill 转为 decode
    "p3": {"role": "DECODE"}
  }
}
```

### 8.4 Director 如何利用角色信息优化路由

```
Router 请求: POST /query { token_ids: [...], model: "llama-70b" }

Director 查询流程:
  1. 查 PrefixIndex → prefix 命中在 rdmas-0, rdmas-1
  2. 查 InstanceRegistry → rdmas-0 附近有哪些 Prefill? 哪些 Decode?
  3. 返回分级命中信息:

Response:
{
  "hits": [
    {
      "node_id": "rdmas-0",
      "matched_blocks": 128,
      "nearby_prefill": ["p0", "p1"],   // 可继续 prefill
      "nearby_decode":  ["d0", "d1"]     // 可接手 decode
    },
    {
      "node_id": "rdmas-1",
      "matched_blocks": 96,
      "nearby_prefill": ["p2", "p3"],
      "nearby_decode":  ["d2", "d3"]
    }
  ]
}
```

Router 据此决策：
- 如果是 prefill 请求 → 调到 `nearby_prefill` 最多的节点
- 如果是 decode 请求 → 调到 `nearby_decode` 最多的节点
- 如果命中率相近 → 选负载最低的节点

### 8.5 与传统 PD 分离的差异

| | 传统 PD（KV cache 直接传输） | RDMAS Director PD |
|---|---|---|
| **KV cache 在哪** | 内存中，P 传 D | 共享 RDMAS 节点，P 写入，D 读取 |
| **P:D 比例限制** | 受内存和带宽限制 | 不受限（RDMAS 节点独立扩展） |
| **P 崩溃** | KV cache 丢失 | RDMAS 节点仍持有，新 P 可接管 |
| **D 扩容** | 需等待 KV cache 传输 | 直接从 RDMAS One-Sided READ，零等待 |
| **比例切换** | 需重启或重新分配内存 | Runtime 注册角色即可 |

## 九、部署编排层：谁来做，能不能自己做

### 9.1 当前生态中"没人做"——经 Mooncake 源码验证

**验证方法**：全文检索 Mooncake 仓库（1,805 个文件），搜索 `prefill` / `decode` / `orchestrat` / `rebalance` / `auto-scale` / `scheduler` 等关键词，逐文件检查 `master_service.cpp` (11,015行)、`master.cpp`、`master_admin_service.cpp`、Conductor 设计文档和 API 规范、vLLM/SGLang 集成文档。

**验证结论**：

| 关键词 | Mooncake 源码命中数 | 说明 |
|--------|-------------------|------|
| `prefill` / `decode` | **0** | 仅存在于一篇 vLLM 集成文档中 |
| `orchestrat` | **0** | 全局无此概念 |
| `rebalance` | **0** | 不存在 |
| `auto-scale` | **0** | 不存在 |
| `scheduler` | 2 | `deadline_scheduler.h`，仅用于存储端回调定时器 |
| `ratio` | 55 | 全部是 eviction ratio（缓存淘汰比例）、error ratio、hit ratio |

**各组件职责确认**：

| 组件 | 实际做什么 | 不做什么 |
|------|-----------|---------|
| **Mooncake Conductor** | Go 实现的 KV-cache 索引器。订阅 ZMQ 事件(BlockStored/BlockRemoved)，维护 PrefixCacheTable，回答 `POST /query`  | ❌ 不做路由决策（文档原文："The router selects the best target instance"） |
| **Mooncake Store Master** | 分布式 KV-cache 存储元数据管理。segment/replica/tenant/eviction/HA/snapshot | ❌ 不管理 P/D 角色。11,015 行 C++ 中无 prefill 或 decode 概念 |
| **vLLM 集成** | `--kv-transfer-config '{"kv_role":"kv_producer"}'` 或 `"kv_consumer"` — 纯 CLI 参数，per-node 静态配置 | ❌ 不会在运行时改变 |
| **SGLang 集成** | `--disaggregation-mode prefill|decode` — 启动参数决定。Router 端 `--decode` 参数列出 decode URL | ❌ P:D 比例 = 手动列了多少个 URL |

**LMCache 做了哪些相关的**：
- ✅ 多 vLLM 实例间共享 prefix cache（通过 lmcache-server）
- ✅ L1/L2 多级缓存自动存储和迁移
- ✅ store/prefetch controller 管理缓存生命周期

**LMCache 没做的**：
- ❌ 不决定哪个实例是 P 还是 D（这是 vLLM 启动参数的事）
- ❌ 不根据 cache 命中率动态调整 P:D 比例
- ❌ 不做 cache-aware 请求路由

### 9.2 我们自己做的方案：`RDMAS-Orchestrator`

在 Director 之上增加一个轻量级编排层，负责：

```
                   ┌─────────────────────────────────┐
  vLLM/SGLang      │       RDMAS-Orchestrator        │
  ┌─────────┐     │  ┌───────────┐  ┌────────────┐  │
  │ Prefill │─────┼─→│ Director  │  │ Scheduler  │  │
  │ Workers │     │  │ (cache索引)│  │ (P/D比例)  │  │
  └─────────┘     │  └─────┬─────┘  └─────┬──────┘  │
  ┌─────────┐     │        │              │         │
  │ Decode  │←────┼────────┼──────────────┘         │
  │ Workers │     │        │                        │
  └─────────┘     │        ▼                        │
                  │  ┌───────────┐                  │
                  │  │ RDMAS     │                  │
                  │  │ Nodes     │                  │
                  │  └───────────┘                  │
                  └─────────────────────────────────┘
```

#### Orchestrator 核心功能

**1. 动态 P:D 比例**

```protobuf
message PDRatio {
    uint32 prefill_count = 1;   // 当前 prefill worker 数
    uint32 decode_count = 2;    // 当前 decode worker 数
    uint32 total_instances = 3; // 总实例数
}

service Orchestrator {
    // 查询当前 PD 比例
    rpc GetPDRatio(GetPDRatioRequest) returns (PDRatio);

    // 调整 PD 比例
    rpc SetPDRatio(SetPDRatioRequest) returns (SetPDRatioResponse);

    // 自动调整（基于 cache 命中率）
    rpc AutoTune(AutoTuneRequest) returns (AutoTuneResponse);
}
```

**2. 自动调优逻辑**

```rust
impl Orchestrator {
    /// 周期性运行：根据 cache 命中率决定是否调整 P:D 比例
    async fn auto_tune_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;

            let stats = self.director.query_stats().await;
            // stats: { hit_rate, avg_prefix_length, prefill_q_depth, decode_q_depth }

            if stats.hit_rate < 0.3 {
                // 缓存命中率低 → 需要更多 prefill 生成新缓存
                self.scale_prefill_up();
            } else if stats.hit_rate > 0.8 && stats.decode_q_depth > 100 {
                // 缓存命中率高但 decode 队列长 → 需要更多 decode
                self.scale_decode_up();
            }
        }
    }
}
```

**3. 部署配置示例**

```json
{
  "cluster": {
    "total_instances": 8,
    "rdmas_nodes": ["rdmas-0", "rdmas-1"],
    "default_pd_ratio": "4:4"
  },
  "auto_tune": {
    "enabled": true,
    "min_prefill": 2,
    "max_prefill": 6,
    "hit_rate_threshold_up": 0.3,
    "hit_rate_threshold_down": 0.8,
    "evaluation_interval_secs": 30
  }
}
```

### 9.3 对比：自己做 vs 依赖现有工具

| 方案 | 优点 | 缺点 |
|------|------|------|
| **K8s HPA** | 成熟，自动扩缩 | 不知道 cache 语义，扩缩的是 pod 数不是角色 |
| **Mooncake Conductor + nginx** | 解耦，各司其职 | 两个组件协调，无自动 P:D 调优 |
| **vLLM 内置 PD** | 官方支持 | 静态配置，无动态调优 |
| **RDMAS-Orchestrator** | 内置 cache 感知 + 自动调优 + π形角色切换 | 自己维护 |

### 9.4 为什么我们可以做

RDMAS 有三个结构优势让编排变得简单：

**优势 1: 共享存储 = P 和 D 之间没有数据依赖**

传统 PD 分离中，P 的内存里有 KV cache，D 必须从 P 复制。RDMAS 模式下，P 把 KV cache 写入 RDMAS 节点，D 从 RDMAS 节点读取——P 和 D 完全解耦。切换角色不需要拷贝数据。

**优势 2: One-Sided RDMA = D 扩容零等待**

传统模式增加 Decode worker 需要等待 KV cache 从 P 传输。RDMAS 模式下，新的 D worker 直接 One-Sided RDMA READ → 秒级就绪。

**优势 3: Director 已内置 cache 索引**

编排器不需要重新构建 cache 索引——Director 已经有 `PrefixIndex`。编排器只是在上层加一个决策循环。

### 9.5 实现路线

| Phase | 内容 | 新增代码 | 时间 |
|-------|------|---------|------|
| P1 | `Orchestrator` 结构体 + `/admin/reconfigure` API | ~300 行 | 1 天 |
| P2 | `auto_tune_loop` 决策循环 + 统计收集 | ~200 行 | 1 天 |
| P3 | 与 vLLM/SGLang API 对接（远程调 P/D） | ~200 行 | 1 天 |
| P4 | K8s operator（可选）| 外部项目 | 3 天 |

**总计**：3 天可以做一个能用的原型，运行在 RDMAS 集群旁边做自动 P:D 调优。这是当前生态中的空白，Mooncake 也没有提供这个能力。

## 十、结论

在 RDMAS 之上构建 Director 协调层，可以让 PD 分离推理系统获得：

1. **Cache-aware 调度** — Router 知道哪个节点有最佳 prefix 命中
2. **零 CPU 数据面** — 命中后的数据加载走 One-Sided RDMA，无 CPU 参与
3. **内嵌上报** — Connector 自动维护索引，无需额外进程
4. **与原架构兼容** — Director 是可选组件，不启用时 RDMAS 回退为普通 L2 存储
