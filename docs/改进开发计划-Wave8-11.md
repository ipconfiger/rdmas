# RDMAS 改进开发计划：Wave 8–11

> 基于 `docs/final_report.md` 中双报告交叉验证确认的 10 个真实问题（P1–P10）制定的多波次改进计划。
> 承接 Waves 1–7（已完成 211 tests，本地引擎 + LMCache 适配完整），编号从 Wave 8 开始。
>
> **采用波次（Wave）+ 并行轨道（Track）+ 关卡门禁（Gate）结构。**

---

## 执行模型说明

- **波次（Wave）**：一个时间盒（1–2 周），结束时必须通过**关卡门禁（Gate）**方可解锁下一波。
- **轨道（Track）**：波次内可并行的独立工作单元，写作用域不重叠。
- **关卡门禁（Gate）**：客观可验证的通过标准（测试通过、指标达成、风险清除）。
- **风险卡点（Risk Check）**：前置高风险项，必须在波次早期消除。

**全程铁律：**
1. **TDD**：生产代码先写测试再写实现
2. **改后必验**：任何编译型改动必须 `cargo build` + `cargo test` 通过
3. **设计审查**：Wave 9 涉及架构变更，须经 Oracle 审查后方可进入

---

## 改进路线图总览

```
Wave 8 (工程化基础)          Wave 9 (分布式一致性)        Wave 10 (生产特性)          Wave 11 (加固)
├── CI/CD 自动化              ├── Extent 分布式一致性        ├── LRU 淘汰策略            ├── 传输加密
├── QP 错误状态恢复           ├── Slab/Chunk 分配器         ├── GPUDirect RDMA           ├── 压力测试套件
├── CQ 异步事件处理           │   (vLLM Block Size 对齐)    ├── 内存水位监控             ├── 多租户隔离基础
└── PFC/ECN 部署文档          │                             │                            └── 完整运维手册
                              └── Gate-9 强制 Oracle 审查   └── Gate-10 实机验证
```

---

## 🌊 Wave 8 — 工程化基础与运维就绪（第 1–2 周）

**波次目标：** 填补 CI/CD、QP 恢复、CQ 事件处理和生产部署文档的空白，为后续波次提供自动化验证和稳定基础。

**解决 P5（QP 错误恢复）、P6（CQ 异步事件）、CI/CD 缺失、P9（部署文档）**

| 轨道 | 职责 | 写作用域 | 可并行 | 解决 |
|------|------|----------|--------|------|
| **T8-A CI/CD 自动化** | 添加 `.github/workflows/ci.yml`：`cargo check` + `cargo test --workspace` + `cargo clippy -- -D warnings` + `cargo fmt --check`；添加 `Dockerfile` 用于 RDMA 容器化构建（含 `rdma-core-devel`）；添加 `docker-compose.yml` 用于多节点测试 | `.github/`, `Dockerfile`, `docker-compose.yml` | ✅ | CI/CD 缺失 |
| **T8-B QP 错误状态恢复** | 在 `src/rdma/qp.rs` 新增 `QpStateMonitor`：后台线程周期性 `ibv_query_qp` 检测 ERROR 状态；`QpRecoveryHandle` 封装销毁+重建+重连完整流程；`Transport` trait 新增 `reconnect()` 方法 | `src/rdma/qp.rs`, `src/rdma/qp_recovery.rs` (新), `src/transport/` | ✅（独立于 T8-A/C） | P5 |
| **T8-C CQ 异步事件处理** | 在 `src/rdma/cq.rs` 新增 `CqEventChannel`：使用 `ibv_create_comp_channel` + `ibv_get_cq_event` 替换纯 busy-poll；新增 `AsyncPoller` 同时支持 busy-poll（低延迟）与事件驱动（低 CPU）；CQ overrun 检测与告警 | `src/rdma/cq.rs`, `src/runtime/poller.rs` | ✅ | P6 |
| **T8-D 生产部署文档** | 编写 `docs/deployment.md`：PFC 配置（优先级流控 / 交换机 settings）、ECN 配置（WRED 阈值 / CNP 标记）、HugePages 调优（1GB 页 vs 2MB 页）、memlock ulimit 配置、RDMA 网卡固件升级指南、多节点拓扑建议 | `docs/deployment.md` (新) | ✅ | P9, P8 |

**关卡门禁 Gate-8：**
- [ ] CI 流水线全绿：`cargo check` + `cargo test --workspace` + `cargo clippy` + `cargo fmt` 全部通过
- [ ] T8-B：模拟 QP ERROR 注入测试通过（进程间 `ibv_modify_qp` 触发 ERROR → 3s 内自动恢复）
- [ ] T8-C：CQ overrun 注入测试通过（生产者超速 → 异步事件捕获 → 日志告警）
- [ ] T8-D：按照 `deployment.md` 在 SoftRoCE 环境完成从头部署，所有步骤可复现

---

## 🌊 Wave 9 — 分布式 Extent 一致性（第 3–4 周）

**波次目标：** 解决 P1（最严重问题）：将 `LargeObjectRegion` 的 extent 分配从服务端本地操作升级为 Client 可通过单边 RDMA 安全参与的分布式协议。同时解决 P2（Slab 分配器对齐 vLLM Block Size）。

**解决 P1（Extent 分配器分布式一致性）、P2（Slab/Chunk 分配器）**

> ⚠️ **架构变更波次**：涉及数据面和控制面交互协议的重新设计，必须在 Gate-9 前通过 Oracle 审查。

| 轨道 | 职责 | 写作用域 | 可并行 | 解决 |
|------|------|----------|--------|------|
| **T9-A Extent 分布式协议设计** | 设计 Client 无冲突 Extent 分配协议：方案 A—"Reserve-Before-Write"（Client 先 CAS 预留 extent 槽位，再 RDMA WRITE）；方案 B—"Server Pre-Alloc"（Server 预分配 extent 区域并定时推送 free list 快照给 Client）；方案 C—"Partitioned Extent"（按 Client ID 哈希分区，每个 Client 拥有专属 extent 区域，无冲突）。产出 `docs/extent-protocol.md` 设计文档 | `docs/extent-protocol.md` (新) | ✅（纯设计，无代码） | P1 |
| **T9-B Extent 协议实现** | 基于选定的协议方案实现：扩展 `proto/control.proto` 增加 Extent 状态同步 RPC；修改 `src/engine/extent.rs` 支持选定的分配模式；修改 `src/client/write.rs` 中的 Extent 写入路径；`LARGE_OBJECT` 区域的 `RegionMetadata` 增加 extent 分配状态字段 | `proto/control.proto`, `src/engine/extent.rs`, `src/client/write.rs`, `src/control/server.rs` | ❌（依赖 T9-A 选型） | P1 |
| **T9-C Slab/Chunk 分配器** | 新增 `src/engine/slab.rs`：支持定长 Chunk 分配（对齐 vLLM KV Block 大小：16 × hidden_dim × dtype_size）；Chunk 大小在 `BootstrappedEngine::bootstrap()` 中可配置；与变长 extent 分配器共存；新增 chunk 级别碎片率统计 | `src/engine/slab.rs` (新), `src/engine/bootstrap.rs`, `src/engine/mod.rs` | ✅（可与 T9-A 并行） | P2 |
| **T9-D 乐观读加固** | 增强 `src/engine/concurrency.rs` 乐观读协议：大对象读增加校验和（XXH64 of payload stored in ExtentHeader）；写入时先写校验和→再写数据→写校验和清零标记完成；读取时校验和匹配 + 版本号一致 = 数据完整。部分缓解 P7（数据撕裂） | `src/engine/concurrency.rs`, `src/engine/layout.rs`, `src/client/read.rs` | ✅ | P7（缓解） |

**🟡 风险卡点（Wave 9 Day 3 前）：**
- T9-A 协议选型：Oracle 审查三种方案，选定一种后 T9-B 方可进入实现。
- 若方案 A（Reserve-Before-Write）被选：需验证 `RDMA_CMP_SWAP` 在 extent header 上的性能（目标：< 2× RDMA READ 延迟）。
- 若方案 C（Partitioned Extent）被选：需验证分区数是否满足预期 Client 数（分区太多浪费空间，太少导致冲突）。

**关卡门禁 Gate-9：**
- [ ] Oracle 审查通过 `docs/extent-protocol.md`
- [ ] T9-B：多 Client 并发写入不同 extent 无冲突（集成测试：4 Client 并发写 1000 个大对象，零分配冲突）
- [ ] T9-C：Chunk 分配器与 LMCache Block Size 对齐验证（connector 集成测试通过）
- [ ] T9-D：校验和+版本号双重验证，大对象数据撕裂测试（写入中途 Crash → 读到不一致数据 → 乐观读拒绝 → 重试成功）
- [ ] 回归：`cargo test --workspace` 全绿，无性能退化（bench 比较）

---

## 🌊 Wave 10 — LMCache 生产级特性（第 5–6 周）

**波次目标：** 补齐 LMCache 场景的关键缺失特性：LRU 淘汰、GPUDirect RDMA、内存水位监控。

**解决 P6（LRU 淘汰）、P3（GPUDirect RDMA）、P4（PCIe 竞争/水位监控）**

| 轨道 | 职责 | 写作用域 | 可并行 | 解决 |
|------|------|----------|--------|------|
| **T10-A LRU 淘汰策略** | 在 `src/engine/` 新增 LRU 淘汰：基于 EpochGc 扩展，新增 `LruTracker` 用 `crossbeam::sync::SegQueue` 无锁记录访问时间；`KvEngine` trait 新增 `evict(n: usize) -> usize` 方法；内存满时自动触发淘汰（配置 `trigger_watermark`）；通过 LMCache connector 暴露淘汰回调 | `src/engine/gc.rs`, `src/engine/lru.rs` (新), `src/api.rs`, `crates/lmcache-connector/src/lib.rs` | ✅ | P6 |
| **T10-B GPUDirect RDMA** | 在 `crates/ibverbs-sys/` 新增 GDR 相关 FFI 绑定（`ibv_reg_mr` 支持 GPU 显存地址）；`src/transport/rdma.rs` 新增 `RdmaGdrTransport` 支持 `cudaMalloc` 分配的 GPU 缓冲区作为 RDMA MR；`crates/lmcache-connector/` 新增 `use_gdr` 配置项，直接 RDMA READ → GPU 显存 | `crates/ibverbs-sys/`, `src/transport/gdr.rs` (新), `crates/lmcache-connector/` | ✅（独立模块） | P3 |
| **T10-C 内存水位监控** | 在 `src/engine/` 新增 `WatermarkMonitor`：后台线程监控哈希表负载因子 + 大对象区使用率 + 空闲 Chunk 数；超阈值时通过 `ControlPlane::NotifyWatermark` RPC 通知 Client；`crates/lmcache-connector/` 接收水位告警并触发 LMCache L2→L3 降级或拒绝新分配 | `src/engine/watermark.rs` (新), `proto/control.proto`, `src/control/server.rs`, `crates/lmcache-connector/` | ✅ | P4 |
| **T10-D 连接保活增强** | `ClientSession` 增加 MR 元数据有效性验证：心跳中附带 `generation` 校验；QP 断开时自动清理本地缓存的远端 MR 映射（`remote_regions.clear()`）；`reconnect()` 时先 `Discover` 获取最新 MR 再重建 QP | `src/client/session.rs`, `src/control/client.rs` | ✅ | -- |

**关卡门禁 Gate-10：**
- [ ] T10-A：LRU 淘汰测试——灌满哈希表 → 触发淘汰 → 验证最少访问的 N 个 key 被驱逐
- [ ] T10-B：GDR 基准——RDMA READ → GPU 显存 vs RDMA READ → CPU 内存 + cudaMemcpy，延迟对比（目标：GDR 延迟 ≤ CPU 路径 60%）
- [ ] T10-C：水位监控——模拟 80% 内存使用 → 验证 Client 收到 NotifyWatermark → connector 拒绝新 Put
- [ ] T10-D：连接保活——模拟 Server 重启 → Client 自动重连并刷新 MR 元数据（generation 变化 → 丢弃旧 QP → Discover → 重建）
- [ ] 端到端：LMCache connector 完整流程测试（含 GDR + LRU + 水位告警）

---

## 🌊 Wave 11 — 安全加固与生产就绪（第 7–8 周）

**波次目标：** 补齐安全性（P10）、压力测试、多租户基础隔离，完成完整运维手册。

**解决 P10（传输加密）、多租户隔离、压测、完整文档**

| 轨道 | 职责 | 写作用域 | 可并行 | 解决 |
|------|------|----------|--------|------|
| **T11-A RDMA 传输加密** | 调研并实现 RDMA 加密方案：方案 A—IPsec over RoCE（内核层，透明）；方案 B—应用层加密（写入前 AES-GCM 加密 → 写入远端 → 读回后解密，对 RDMA 透明）；方案 C—仅控制面 TLS（gRPC mTLS）+ 数据面在可信内网不加密。产出 `docs/security.md` 安全设计文档并实施选定方案 | 取决于选型 | ✅（设计先行） | P10 |
| **T11-B 压力测试套件** | `tests/stress/` 新增：长时间稳定性测试（24h 持续读写，监控内存泄漏/QP 泄漏/CQ overrun）、高并发竞争测试（64 Client 并发 CAS 同一桶，验证无活锁/无死锁）、网络故障注入（模拟 QP ERROR/RoCE 丢包/Server 宕机 → 验证恢复时间）、吞吐量饱和测试（寻找系统瓶颈点） | `tests/stress/` (新) | ✅ | -- |
| **T11-C 多租户隔离基础** | 新增 `src/control/tenant.rs`：基于 `client_id` 的简单命名空间隔离（`<tenant_id, key>` → XXH64 时混入 tenant_id 作为 seed）；`KvEngine` trait 新增 `with_tenant(tenant_id)` 构造器；`HugePageRegion` 支持分区域分配（不同租户使用不同 MR，物理隔离）| `src/control/tenant.rs` (新), `src/api.rs`, `src/mem/region.rs` | ✅ | -- |
| **T11-D 运维手册** | 合并 T8-D 的部署文档，扩展为完整运维手册 `docs/operations.md`：监控指标（Prometheus exporter for QP 状态/CQ 深度/内存水位/GC 频率）、告警规则（QP ERROR > 0 / 内存水位 > 85% / GC 延迟 > 1s）、故障排查流程（QP 故障 → 检查步骤 → 恢复步骤）、性能调优指南（HugePages 调优/QP 数量调优/MTU 调优） | `docs/operations.md` (新) | ✅ | P9 |

**关卡门禁 Gate-11（最终质量门禁）：**
- [ ] T11-B：24h 压力测试通过——零 panic、零内存泄漏（valgrind / heaptrack）、零不可恢复错误
- [ ] T11-B：64 Client 高竞争 CAS 测试——72h 内零活锁（所有 Client 最终完成）、零死锁
- [ ] T11-B：网络故障注入——Server 宕机后 < 5s 恢复、QP ERROR 后 < 3s 恢复
- [ ] T11-A：安全方案选定并实现（至少控制面 mTLS）
- [ ] T11-C：多租户隔离验证（租户 A 读不到租户 B 的 key）
- [ ] T11-D：运维手册覆盖所有已知故障场景
- [ ] 全量回归：`cargo test --workspace` 全绿，bench 无退化 > 5%

---

## 波次依赖关系图

```
Wave 8 (工程化基础)  ─────────────────────────────────────┐
├── T8-A CI/CD ────────────── 全部波次前置 ────────────────┤
├── T8-B QP 恢复 ─────────── Wave 11 (压测依赖) ──────────┤
├── T8-C CQ 事件 ─────────── Wave 11 (监控依赖) ──────────┤
└── T8-D 部署文档 ────────── Wave 11 (运维手册合并) ───────┤
                                                           │
Wave 9 (分布式一致性) ─────── Gate-9 Oracle 审查 ─────────┤
├── T9-A 协议设计 ──→ T9-B 实现                            │
├── T9-C Slab 分配器 ───── Wave 10 (LRU 依赖 Chunk 大小) ─┤
└── T9-D 乐观读加固                                        │
                                                           │
Wave 10 (生产特性) ──────── Gate-10 实机验证 ──────────────┤
├── T10-A LRU ───────────── 依赖 T9-C (Chunk 大小) ───────┤
├── T10-B GPUDirect ─────── 独立                           │
└── T10-C 水位监控 ──────── 独立                           │
                                                           │
Wave 11 (加固) ──────────── Gate-11 最终质量门禁 ──────────┘
```

**可并行（跨波次）：** Wave 8 全部轨道可与 Wave 9 设计轨道（T9-A、T9-C）并行启动。

---

## 附录：问题编号对照表

| 编号 | 问题 | 解决波次 | 轨道 |
|------|------|---------|------|
| P1 | Extent 分配器分布式一致性 | Wave 9 | T9-A, T9-B |
| P2 | Slab 分配器 / vLLM Block Size 对齐 | Wave 9 | T9-C |
| P3 | GPUDirect RDMA 缺失 | Wave 10 | T10-B |
| P4 | PCIe 带宽竞争 / 水位监控 | Wave 10 | T10-C |
| P5 | QP 错误状态自动恢复 | Wave 8 | T8-B |
| P6 | 无 LRU 缓存淘汰策略 | Wave 10 | T10-A |
| P7 | 大对象数据撕裂风险 | Wave 9 | T9-D (缓解) |
| P8 | CQ 异步事件处理缺失 | Wave 8 | T8-C |
| P9 | PFC/ECN 部署文档缺失 | Wave 8 + 11 | T8-D, T11-D |
| P10 | 传输层加密 | Wave 11 | T11-A |
| CI/CD | CI/CD 流程全缺 | Wave 8 | T8-A |

---

> **参考文档：**
> - `docs/final_report.md` — 双报告交叉验证的综合分析
> - `docs/Rust-RDMA.md` — v3 完整技术设计方案
> - `docs/开发执行计划.md` — Waves 1–7 原始计划
> - `docs/进度报告.md` — 当前状态（211 tests, Waves 1–7 完成）
