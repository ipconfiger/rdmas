# RDMAS 改进开发计划：Wave 8–11（v2 — Oracle 审查修订版）

> 基于 `docs/final_report.md` 中双报告交叉验证确认的 10 个真实问题（P1–P10）制定。
> 承接 Waves 1–7（已完成 211 tests，本地引擎 + LMCache 适配完整），编号从 Wave 8 开始。
>
> **采用波次（Wave）+ 并行轨道（Track）+ 关卡门禁（Gate）结构。**
>
> ⚠️ **v2 修订**：经 Oracle 战略审查，修复 4 个阻塞问题、6 个重大依赖错误，补全遗漏项。参见文末 [修订记录](#附录-b修订记录)。

---

## 执行模型说明

- **波次（Wave）**：一个时间盒（1–2 周），结束时必须通过**关卡门禁（Gate）**方可解锁下一波。
- **轨道（Track）**：波次内可并行的独立工作单元，写作用域不重叠。
- **关卡门禁（Gate）**：客观可验证的通过标准（测试通过、指标达成、风险清除）。
- **风险卡点（Risk Check）**：前置高风险项，必须在波次早期消除。

**全程铁律：**
1. **TDD**：生产代码先写测试再写实现
2. **改后必验**：任何编译型改动必须 `cargo build` + `cargo test` 通过
3. **设计审查**：Wave 9 涉及架构变更，**T9-A 协议设计必须经 Oracle 审查**后方可进入 T9-B 实现
4. **原则不可违背**：任何设计不得违反"Server CPU 数据面零参与"的核心原则

---

## 改进路线图总览

```
Wave 8A (Week 1)                Wave 8B (Week 2)              Wave 9A (Week 3)
├── T8-A CI/CD 自动化 🥇          ├── T8-B QP 错误状态恢复       ├── T9-B Extent 协议实现
├── T8-C CQ 事件 (Client-only)   ├── T9-A 协议设计 + Oracle      │   (CAS bump allocator)
├── T8-D 生产部署文档             ├── T8-E FreeList 区域实现      ├── T9-D 乐观读加固
├── T8-F FFI 绑定补齐             │                               │   (+ ExtentHeader V2)
│                                │                               │
Wave 9B (Week 4)                Wave 10 (Weeks 5-6)            Wave 11 (Weeks 7-8)
├── T9-C Slab/Chunk 分配器       ├── T10-A LRU 淘汰策略          ├── T11-A 传输加密（设计+实现）
│   (复用 T9-B 分配协议)          ├── T10-B GPUDirect RDMA       ├── T11-B 压力测试套件
│                                ├── T10-C 内存水位监控          ├── T11-C 多租户隔离
│                                ├── T10-D 连接保活增强           ├── T11-D 完整运维手册
│                                │   (依赖 T8-B)                 │
│                                └── T10-E gRPC 协议版本协商      └── T11-E 跨波次集成测试
```

---

## 🌊 Wave 8A — 工程化基础（第 1 周）

**波次目标：** CI/CD 自动化 + FFI 绑定补齐 + Client 侧 CQ 事件 + 生产部署文档。最高优先级、最大价值/投入比。所有轨道可并行。

| 轨道 | 职责 | 写作用域 | 解决 |
|------|------|----------|------|
| **🥇 T8-A CI/CD 自动化** | 添加 `.github/workflows/ci.yml`：`cargo check` + `cargo test --workspace` + `cargo clippy -- -D warnings` + `cargo fmt --check`；添加 `Dockerfile`（含 `--device=/dev/infiniband/*` + `--network=host` 用于 RoCE）；添加 `docker-compose.yml` 用于多节点 SoftRoCE 测试 | `.github/`, `Dockerfile`, `docker-compose.yml` | CI/CD 缺失 |
| **T8-C CQ 异步事件处理（Client-only）** | 仅在 Client 侧新增 `CqEventChannel`：使用 `ibv_create_comp_channel` + `ibv_get_cq_event` 作为 busy-poll 的补充模式（非替换）；CQ overrun 检测与告警。**Server 侧不做**——Server 数据面零 CPU 参与，CQ 事件仅对双面操作有意义 | `src/rdma/cq.rs`, `src/runtime/poller.rs` | P6 |
| **T8-D 生产部署文档** | 编写 `docs/deployment.md`：PFC 配置（优先级流控 / 交换机 settings）、ECN 配置（WRED 阈值 / CNP 标记）、HugePages 调优（1GB vs 2MB 页）、memlock ulimit、RDMA 网卡固件升级、多节点拓扑建议 | `docs/deployment.md` (新) | P9, P8 |
| **T8-F FFI 绑定补齐** | 在 `crates/ibverbs-sys/` 补齐缺失的动词绑定：`ibv_query_qp`（T8-B 依赖）、`ibv_create_comp_channel` / `ibv_get_cq_event` / `ibv_get_async_event`（T8-C 依赖）、`ibv_req_notify_cq` 已有但需验证。产出 `crates/ibverbs-sys/FFI_CHECKLIST.md` | `crates/ibverbs-sys/` | P5/P6 前置 |

**关卡门禁 Gate-8A：**
- [ ] CI 流水线全绿：`cargo check` + `cargo test --workspace` + `cargo clippy` + `cargo fmt`
- [ ] T8-F：`FFI_CHECKLIST.md` 中所有缺失绑定已补全并通过编译
- [ ] T8-F：`ibv_query_qp` 可在 SoftRoCE 环境成功查询 QP 状态
- [ ] T8-D：按照 `deployment.md` 在 SoftRoCE 环境完成从头部署，所有步骤可复现
- [ ] T8-C：Client 侧 CQ overrun 注入测试通过（生产者超速 → 异步事件捕获 → 日志告警）

---

## 🌊 Wave 8B — 可靠性基础（第 2 周）

**波次目标：** QP 错误状态自动恢复、Extent 分布式协议设计与 Oracle 审查、FreeList 区域实现（Wave 9 前置依赖）。

| 轨道 | 职责 | 写作用域 | 可并行 | 解决 |
|------|------|----------|--------|------|
| **T8-B QP 错误状态恢复** | 在 `src/rdma/qp.rs` 旁新增 `src/rdma/qp_recovery.rs`：**单所有者模型**（非后台线程，避免 `ibv_query_qp` 与并发 post 的 UB）——`QpGuard` 封装 QP，在每次 `post_send`/`post_recv` 前检查 QP 状态，若 ERROR 则自动触发销毁+重建流程；`Transport` trait 通过新增 `ReconnectableTransport` 子 trait 提供 `async fn reconnect(&mut self)`；在途 WR 语义：ERROR 时丢失的 WR 视为失败，由上层 `retry.rs` 重试 | `src/rdma/qp_recovery.rs` (新), `src/rdma/qp.rs`, `src/transport/mod.rs` | ✅ | P5 |
| **T9-A Extent 分布式协议设计与 Oracle 审查** | 设计文档 `docs/extent-protocol.md`，评估 **两种方案**（排除 Server Pre-Alloc——违反零 CPU 原则）：**方案 A 推荐—CAS Bump Allocator**：FreeList 区域头部放一个 `bump_offset: AtomicU64`，Client 通过 RDMA CAS 原子推进分配指针，获独占 extent 区间后 RDMA WRITE 数据；GC 回收的 extent 由 Server 控制面 `SyncFreeList` RPC 推送给 Client。**方案 B—Partitioned Extent**：按 Client ID 哈希预分配固定分区，零冲突但空间利用率低。Oracle 审查后选定方案并产出最终协议文档 | `docs/extent-protocol.md` (新) | ✅（纯设计） | P1 |
| **T8-E FreeList 区域实现** | `proto/control.proto` 已声明 `RegionType::FREE_LIST` 但 `server.rs:66-72` 仅占位（`vaddr: 0, rkey: 0`）。将其实现为真正的 RDMA 可访问共享内存区域：初始化为 `[bump_offset: u64(0), _pad: [u8; 56]]`（64B 对齐的 CAS 目标），注册到 MR 并通过 Discover RPC 分发给 Client。这是 T9-B 的前置依赖 | `src/engine/layout.rs`, `src/engine/bootstrap.rs`, `src/control/server.rs`, `proto/control.proto` | ❌ 微依赖 T9-A（需确认布局） | P1 前置 |

**关卡门禁 Gate-8B：**
- [ ] T8-B：模拟 QP ERROR 注入测试通过（`ibv_modify_qp` 触发 ERROR → 下次 post 时自动恢复 → ≤3s 恢复完成）
- [ ] T8-B：在途 WR 丢失测试（QP ERROR 时 5 个未完成 WR → 全部返回错误 → `retry.rs` 重试成功）
- [ ] T9-A：Oracle 审查通过 `docs/extent-protocol.md`，方案选定并记录决策理由
- [ ] T8-E：FreeList 区域可通过 RDMA READ 从 Client 读取 `bump_offset`
- [ ] T8-E：FreeList 区域集成到 `BootstrappedEngine::bootstrap()`
- [ ] CI 全绿（T8-A 持续运行）

**🟡 风险卡点（Wave 8B Day 3 前）：**
- T9-A Oracle 审查：必须在 Day 3 前完成审查决策，否则 T9-B 无法在第 3 周启动。
- 若 Oracle 推荐方案 B（Partitioned）：需额外 1 天评估分区数对空间利用率的影响。

---

## 🌊 Wave 9A — Extent 分布式实现（第 3 周）

**波次目标：** 基于 T9-A 选定的协议实现分布式 Extent 分配，加固乐观读协议（含 ExtentHeader V2 升级）。

**解决 P1（Extent 分配器分布式一致性）、P7（数据撕裂缓解）**

> ⚠️ **前置条件**：Gate-8B 通过，T9-A 协议已选定。

**选定方案（CAS Bump Allocator）下的代码变更：**

| 轨道 | 职责 | 写作用域 | 解决 |
|------|------|----------|------|
| **T9-B Extent 协议实现** | **①** `src/engine/layout.rs`：新增 `FreeListHeader { bump_offset: u64, _pad: [u8; 56] }`（Cache-line 对齐的 CAS 目标）。**②** `src/engine/extent.rs`：拆分为 `LocalExtentAllocator`（保留现有逻辑，供本地测试）和 `DistributedExtentAllocator`（新增，基于 CAS bump 分配 + `SyncFreeList` RPC 回收）。**③** `src/client/write.rs`：将第 234–241 行 `Err(Internal("Extent mode not yet supported"))` 替换为 `allocate_extent_remote()` 调用。**④** `proto/control.proto`：新增 `SyncFreeList` RPC。**⑤** `src/control/server.rs`：补全 `FREE_LIST` 区域的 `vaddr`/`rkey`（目前均为 0），新增 `sync_free_list` handler | `src/engine/layout.rs`, `src/engine/extent.rs`, `src/client/write.rs`, `proto/control.proto`, `src/control/server.rs` | P1 |

| **T9-D 乐观读加固 + ExtentHeader V2** | **①** `src/engine/layout.rs`：新增 `ExtentHeaderV2`（32 字节，比 V1 的 24 字节多一个 `checksum: u64` 字段）。更新 `HEADER_SIZE` = 32。**②** 校验和协议（已修正写入顺序）：**(a)** 先清零 checksum → **(b)** RDMA WRITE 数据 → **(c)** RDMA WRITE checksum（XXH64 of payload）。读取时：checksum == 0 → 写入进行中 → 重试；checksum != 0 且匹配 + 版本号一致 → 数据完整。**③** 保留 `ExtentHeader`（V1）以支持迁移过渡；默认创建新 extent 使用 V2。**④** `ExtentHeader` 增加 `version: u8` 字段用于滚动升级检测 | `src/engine/layout.rs`, `src/engine/extent.rs`, `src/engine/concurrency.rs`, `src/client/read.rs` | P7（缓解） |

> ⚠️ **破坏性变更提醒**：`HEADER_SIZE` 从 24 → 32 影响 `extent.rs` 的 `extent_total()`（L46）、`allocate()`（L109–148）、`read()`（L171–205）、`write_extent()`（L305–338）、`extent_ref()`（`layout.rs:208`），以及 ~24 个 extent 相关测试和 2 个 GC 测试。`BootstrappedEngine::bootstrap()` 需要适配新类型。

**关卡门禁 Gate-9A：**
- [ ] T9-B：多 Client 并发 CAS bump 分配测试（4 Client 并发分配 1000 个 extent，零冲突、零重叠）
- [ ] T9-B：Write 路径集成测试——`ClientWriter::insert` 在 Extent 模式下成功完成远端分配 + 写入
- [ ] T9-B：Server 重启后 Client 重新 Discover 获取新 FreeList vaddr/rkey，CAS 分配恢复正常
- [ ] T9-D：数据撕裂测试——写入中途 Kill Client → checksum 为 0 → Reader 乐观读拒绝 → 重试后成功
- [ ] T9-D：`HEADER_SIZE` 和 `extent_total()` 回归验证——所有 extent 测试适配新大小
- [ ] 回归：`cargo test --workspace` 全绿，bench 无退化 > 5%

---

## 🌊 Wave 9B — Slab/Chunk 分配器（第 4 周）

**波次目标：** 基于 T9-B 的分配协议，实现定长 Chunk 分配器对齐 vLLM Block Size。

**解决 P2（Slab 分配器 / vLLM Block Size 对齐）**

| 轨道 | 职责 | 写作用域 | 解决 |
|------|------|----------|------|
| **T9-C Slab/Chunk 分配器** | 新增 `src/engine/slab.rs`：复用 T9-B 的 CAS bump 分配协议（同 FreeList 区域的分配语义）。支持可配置 Chunk 大小（默认对齐 vLLM KV Block：16 × hidden_dim × dtype_size）。`BootstrappedEngine` 新增 `chunk_size` 参数。与变长 extent 分配器共存：小对象走 Slab（定长高效），大对象走 Extent（变长灵活）。新增 Chunk 级别碎片率统计 | `src/engine/slab.rs` (新), `src/engine/bootstrap.rs`, `src/engine/mod.rs` | P2 |
| **T9-E ExtentHeader V2 迁移收尾** | 确保所有 extent 分配走 V2 格式。`BootstrappedEngine::bootstrap()` 设置默认使用 V2。添加迁移辅助：读取时兼容 V1 格式（检测 `version` 字段 = 0 → 按 24 字节 header 解析；= 1 → 按 32 字节解析） | `src/engine/layout.rs`, `src/engine/extent.rs`, `src/engine/bootstrap.rs` | P7 |

**关卡门禁 Gate-9B：**
- [ ] T9-C：Chunk 分配器集成测试——分配 N 个固定大小 Chunk，零重叠、零碎片（理想情况下）
- [ ] T9-C：与 LMCache Block Size 对齐验证——`lmcache-connector` 集成测试通过（KV Cache Block 大小匹配）
- [ ] T9-E：V1 格式 extent 读取兼容测试（创建 V1 extent → 升级后仍可正确读取）
- [ ] 回归：`cargo test --workspace` 全绿，Slab 分配性能 bench 达标

---

## 🌊 Wave 10 — LMCache 生产级特性（第 5–6 周）

**波次目标：** 补齐 LMCache 场景的关键缺失特性。**所有轨道可并行**（Oracle 确认 LRU 不依赖 Chunk 大小，Chunk 仅影响淘汰粒度调优）。

**解决 P6（LRU 淘汰）、P3（GPUDirect RDMA）、P4（水位监控）、gRPC 版本协商**

| 轨道 | 职责 | 写作用域 | 解决 |
|------|------|----------|------|
| **T10-A LRU 淘汰策略** | `src/engine/lru.rs`（新）：`LruTracker` 用 `crossbeam::sync::SegQueue` 无锁记录 key 访问时间戳；`KvEngine` trait 新增 `evict(n: usize) -> usize`（向后兼容——Gate-3 稳定的 trait 只增不减）；EpochGc 扩展为同时支持 tombstone GC 和 LRU 驱逐。⚠️ 不依赖 T9-C：LRU 基于 key 粒度，与 Chunk 大小无关 | `src/engine/lru.rs` (新), `src/engine/gc.rs`, `src/api.rs`, `crates/lmcache-connector/` | P6 |
| **T10-B GPUDirect RDMA** | `src/transport/gdr.rs`（新）：`RdmaGdrTransport` 实现 `Transport` trait，`ibv_reg_mr` 支持 GPU 显存地址（需 `nvidia-peermem` 内核模块）；`crates/lmcache-connector/` 新增 `use_gdr: bool` 配置，RDMA READ 直接写入 GPU 显存。⚠️ Docker 需 `--gpus all`。延迟预算：GDR 路径额外 ~1-2μs（vs CPU 路径 ~10-30μs cudaMemcpy） | `src/transport/gdr.rs` (新), `crates/ibverbs-sys/`, `crates/lmcache-connector/` | P3 |
| **T10-C 内存水位监控** | `src/engine/watermark.rs`（新）：后台线程监控哈希表负载因子 + extent 使用率 + Chunk 空闲率；超阈值通过 `proto/control.proto` 新增的 `NotifyWatermark` RPC 通知 Client；`lmcache-connector` 接收告警触发 L2→L3 降级或拒绝新分配 | `src/engine/watermark.rs` (新), `proto/control.proto`, `src/control/server.rs`, `crates/lmcache-connector/` | P4 |
| **T10-D 连接保活增强** | ⚠️ **前置依赖**：依赖 T8-B 的 `ReconnectableTransport` trait。`ClientSession` 增加：心跳中附带 `generation` 校验（Server 重启后 generation 变化 → 触发重连）；QP 断开时自动清理本地 MR 映射；重连时先 `Discover` 获取最新 `ServerMetadata` 再调 `reconnect()` | `src/client/session.rs`, `src/control/client.rs` | -- |
| **T10-E gRPC 协议版本协商** | `proto/control.proto` 新增 `GetVersion` RPC 返回 `service_version: uint32`；`HeartbeatResponse` 新增 `server_version` 字段；Client 在 `connect()` 时校验版本兼容性；版本不匹配时返回明确错误（非静默失败） | `proto/control.proto`, `src/control/server.rs`, `src/client/session.rs` | -- |

**关卡门禁 Gate-10：**
- [ ] T10-A：LRU 淘汰测试——灌满哈希表 → 触发淘汰 → 验证最少访问的 key 被驱逐
- [ ] T10-B：GDR 基准——RDMA READ → GPU vs CPU+cudaMemcpy（目标：GDR 延迟 ≤ CPU 60%）
- [ ] T10-C：水位模拟——80% 内存使用 → Client 收到 NotifyWatermark → connector 拒绝新 Put
- [ ] T10-D：Server 重启测试——Client 检测 generation 变化 → 自动重连 → MR 元数据刷新 → 读写恢复正常
- [ ] T10-E：版本不匹配测试——旧 Client + 新 Server → 明确错误信息，非崩溃/静默失败
- [ ] 端到端：LMCache connector 完整流程测试（含 GDR + LRU + 水位告警 + 版本协商）

---

## 🌊 Wave 11 — 安全加固与生产就绪（第 7–8 周）

**波次目标：** 补齐安全性、压力测试、多租户基础隔离、完整运维手册、跨波次集成测试。

**解决 P10（传输加密）、压测、多租户隔离、运维文档**

| 轨道 | 职责 | 写作用域 | 解决 |
|------|------|----------|------|
| **T11-A 传输加密** | 设计文档 `docs/security.md`：评估三种方案——① 仅控制面 mTLS（gRPC 双向 TLS）+ 数据面可信内网不加密（推荐，零延迟影响）；② IPsec over RoCE（内核层透明，需网卡/交换机支持）；③ 应用层 AES-GCM（Client 加密后 RDMA WRITE + 读回解密，增加 ~2–5μs）。产出评估后实施方案①（至少）。⚠️ 延迟预算影响需在文档中量化 | `docs/security.md` (新)；实现取决于选型 | P10 |
| **T11-B 压力测试套件** | `tests/stress/` 新增：**长稳测试**（24h 持续读写 4 Client × 100K ops/s，监控内存泄漏/QP 泄漏/CQ overrun）、**高竞争测试**（64 Client 并发 CAS 同一桶 72h → 零活锁/零死锁）、**故障注入**（随机触发 QP ERROR / Server 进程 Kill / RoCE 丢包 → 验证恢复时间 <3s）、**吞吐饱和**（逐步增加 Client 数 → 找到瓶颈点） | `tests/stress/` (新) | -- |
| **T11-C 多租户隔离** | 选择**命名空间隔离**方案（Oracle 审查意见：物理隔离每个租户独立 MR 过于复杂，当前阶段不必要）。`src/control/tenant.rs`（新）：XXH64 混入 `tenant_id` 作为 seed → 同一物理表但不同租户的 key 映射到不同 slot；`KvEngine` trait 新增 `fn namespaced(tenant_id: u64) -> Self`（注意：是构造器返回新实例，非方法修改现有实例）。不做物理 MR 隔离（成本高，Wave 11 阶段不必要） | `src/control/tenant.rs` (新), `src/api.rs` | -- |
| **T11-D 完整运维手册** | 合并 T8-D 部署文档 → `docs/operations.md`：**监控**（Prometheus exporter for QP 状态/CQ 深度/内存水位/GC 频率/Chunk 碎片率）、**告警规则**（QP ERROR > 0 → P1 / 内存水位 > 85% → P2 / GC 延迟 > 1s → P3）、**故障排查**（QP 故障流程图 / 连接超时排查 / CAS 冲突率高排查）、**性能调优**（HugePages 调优/MTU 调优/QP 数量调优） | `docs/operations.md` (新) | P9 |
| **T11-E 跨波次集成测试** | `tests/integration/cross_wave.rs`（新）：验证跨波次组合场景——"QP ERROR + Server 重启 + 版本协商 + LRU + 水位告警" 端到端流程。确保各波次独立实现的功能组合后无交互 bug | `tests/integration/cross_wave.rs` (新) | -- |

**关卡门禁 Gate-11（最终质量门禁）：**
- [ ] T11-B：24h 长稳测试——零 panic、零内存泄漏（valgrind/heaptrack）、零不可恢复错误
- [ ] T11-B：64 Client × 72h 高竞争 CAS——零活锁、零死锁
- [ ] T11-B：故障注入——Server Kill 后 <5s 恢复、QP ERROR 后 <3s 恢复
- [ ] T11-A：至少控制面 mTLS 实现并验证
- [ ] T11-C：命名空间隔离——租户 A 的 key 无法被租户 B 读取
- [ ] T11-D：运维手册覆盖所有已知故障场景（10+ 场景）
- [ ] T11-E：跨波次端到端集成测试全绿
- [ ] 全量回归：`cargo test --workspace` 全绿（≥ 300 tests 预期），bench 无退化 > 5%

---

## 波次依赖关系图

```
Wave 8A (Week 1) ───────────────────────────────────────────────────────────────┐
├── T8-A CI/CD 🥇 ────────────── 全部后续波次的前置（最先执行）────────────────────┤
├── T8-C CQ 事件 (Client) ───── 独立                                              │
├── T8-D 部署文档 ───────────── Wave 11 T11-D (合并) ────────────────────────────┤
└── T8-F FFI 绑定 ───────────── T8-B (依赖 ibv_query_qp) ───────────────────────┤
                              ↓↓                                                  │
Wave 8B (Week 2)                                                                 │
├── T8-B QP 恢复 ────────────── T10-D (依赖 ReconnectableTransport) ─────────────┤
├── T9-A 协议设计 ──→ Oracle审查 → T9-B (选定方案后实现) ─────────────────────────┤
└── T8-E FreeList 实现 ──────── T9-B (CAS bump 的共享内存基础)                    │
                              ↓↓                                                  │
Wave 9A (Week 3)                                                                 │
├── T9-B Extent 协议实现 ────── T9-C (复用 CAS bump 协议) ────────────────────────┤
└── T9-D 乐观读 + Header V2 ── T9-E (迁移收尾)                                   │
                              ↓↓                                                  │
Wave 9B (Week 4)                                                                 │
├── T9-C Slab 分配器 ────────── Wave 10 (可独立，仅调优粒度依赖) ──────────────────┤
└── T9-E Header V2 迁移 ───── 收尾                                               │
                              ↓↓                                                  │
Wave 10 (Weeks 5-6) — 四条轨道全部可并行                                           │
├── T10-A LRU ───────────────── 独立 (不依赖 T9-C)                                │
├── T10-B GPUDirect ─────────── 独立                                              │
├── T10-C 水位监控 ──────────── 独立                                              │
├── T10-D 连接保活 ──────────── 依赖 T8-B                                         │
└── T10-E 版本协商 ──────────── 独立                                              │
                              ↓↓                                                  │
Wave 11 (Weeks 7-8) ────────── Gate-11 最终质量门禁 ─────────────────────────────┘
```

**关键并行化收益：**
- Wave 8A 四条轨道可并行 → Week 1 效率最大化
- Wave 8B 三条轨道可并行（T9-A 与 T8-E 有微依赖，但可在 T9-A 早期决策后并行推进）
- Wave 10 五条轨道全部可并行 → 这是并行度最高的波次（Oracle 确认 T10-A 不依赖 T9-C）
- **总周期**：8 周（未增加，通过并行弥补 T9-B 的 3 周化拆分）

---

## 附录 A：问题编号对照表

| 编号 | 问题 | 解决波次 | 轨道 | 严重性 |
|------|------|---------|------|--------|
| P1 | Extent 分配器分布式一致性 | 8B + 9A | T9-A, T8-E, T9-B | 🔴 高 |
| P2 | Slab 分配器 / vLLM Block Size 对齐 | 9B | T9-C | 🟡 中 |
| P3 | GPUDirect RDMA 缺失 | 10 | T10-B | 🟡 中 |
| P4 | PCIe 带宽竞争 / 水位监控 | 10 | T10-C | 🟡 中 |
| P5 | QP 错误状态自动恢复 | 8B | T8-B | 🟡 中 |
| P6 | 无 LRU 缓存淘汰策略 | 10 | T10-A | 🟡 中 |
| P7 | 大对象数据撕裂风险 | 9A | T9-D (缓解) | 🟡 中 |
| P8 | CQ 异步事件处理缺失 | 8A | T8-C (Client-only) | 🟢 低 |
| P9 | PFC/ECN 部署文档缺失 | 8A + 11 | T8-D, T11-D | 🟢 低 |
| P10 | 传输层加密 | 11 | T11-A | 🟢 低 |
| — | CI/CD 流程全缺 | 8A | T8-A 🥇 | 🔴 高 |
| — | gRPC 协议缺少版本协商 | 10 | T10-E | 🟡 中 |
| — | FreeList 区域仅有占位 | 8B | T8-E | 🔴 高 |
| — | FFI 绑定不完整 | 8A | T8-F | 🔴 高 |
| — | 跨波次集成测试缺失 | 11 | T11-E | 🟡 中 |

---

## 附录 B：修订记录

**v2（Oracle 审查修订）— 2026-08-02：**

| 类别 | 变更 | 原因 |
|------|------|------|
| 🔴 阻塞修复 | 新增 T8-F "FFI 绑定补齐" | `ibv_query_qp` / `ibv_create_comp_channel` 等绑定缺失，T8-B/T8-C 无法启动 |
| 🔴 阻塞修复 | T8-B 改为单所有者模型（`QpGuard`），移除后台监控线程 | 原设计的后台线程与 QP 并发 `post_send` 会导致 UB |
| 🔴 阻塞修复 | `Transport` trait 重构为 `ReconnectableTransport` 子 trait | 原设计的 `reconnect()` 方法在 trait 上无接收者 |
| 🔴 阻塞修复 | T9-A 移除方案 B（Server Pre-Alloc） | 违反核心原则 #1 "Server CPU 数据面零参与" |
| 🟡 修正 | T9-C 从 "可与 T9-A 并行" 改为 "依赖 T9-B 选定协议后实现" | Slab 分配器使用与 Extent 相同的分配协议，必须先决定协议 |
| 🟡 修正 | T10-A 移除对 T9-C 的依赖 | LRU 基于 key 粒度，与 Chunk 大小无关（Oracle 确认） |
| 🟡 修正 | T10-D 标注依赖 T8-B | 连接保活依赖 QP 恢复的 `ReconnectableTransport` |
| 🟡 修正 | T9-D 校验和写入顺序修正 | 原方案 "先写校验和→再写数据" 因 RDMA 操作间无序会导致读取到 valid-checksum+stale-data |
| 🟡 修正 | T9-D 增加 `ExtentHeaderV2` + `HEADER_SIZE = 32` 说明 | 原方案未考虑 header 大小变化对 ~24 个测试的破坏性影响 |
| 🟡 修正 | T8-C 限定为 Client-only | Server 侧 CQ 事件对单边 RDMA 无意义（Server CPU 零参与） |
| ➕ 遗漏补全 | 新增 T8-E "FreeList 区域实现" | `FREE_LIST` 区域目前仅占位（vaddr=0,rkey=0），是 T9-B 前置 |
| ➕ 遗漏补全 | 新增 T10-E "gRPC 协议版本协商" | `proto/control.proto` 无版本字段，波次间 RPC 增加会静默不兼容 |
| ➕ 遗漏补全 | 新增 T9-E "ExtentHeader V2 迁移收尾" | 确保 V1→V2 迁移在 Wave 9 内完整闭环 |
| ➕ 遗漏补全 | 新增 T11-E "跨波次集成测试" | 确保各波次独立实现组合后无交互 bug |
| ➕ 遗漏补全 | T11-C 改为命名空间隔离方案 | 原物理隔离（每租户独立 MR）过于复杂，Wave 11 阶段不必要 |
| 📐 重排 | Wave 8 拆分为 8A/8B；Wave 9 拆分为 9A/9B | 并行化：T9-A 设计提升到 Week 2 给 Oracle 审查更多时间；T9-C 延迟到 Week 4 复用 T9-B 协议 |
| 📐 重排 | T11-A 增加延迟预算分析要求 | 应用层加密增加 ~2–5μs 延迟，需在设计文档中量化 |

---

> **参考文档：**
> - `docs/final_report.md` — 双报告交叉验证的综合分析
> - `docs/Rust-RDMA.md` — v3 完整技术设计方案
> - `docs/开发执行计划.md` — Waves 1–7 原始计划
> - `docs/进度报告.md` — 当前状态（211 tests, Waves 1–7 完成）
> - `docs/改进开发计划-Wave8-11.md` — v1 原始版（被本 v2 替代）
