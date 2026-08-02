# RDMAS 运维手册

> **适用版本**: RDMAS v0.1.0+
> **适用场景**: 生产环境 RDMAS KV Store 日常运维、故障排查、性能调优
> **关联文档**: [部署指南](deployment.md) | [Rust-RDMA 设计方案](Rust-RDMA.md) | [Extent 协议](extent-protocol.md)

---

## 目录

1. [监控指标](#1-监控指标)
2. [告警规则](#2-告警规则)
3. [故障排查流程](#3-故障排查流程)
4. [性能调优指南](#4-性能调优指南)
5. [日常运维](#5-日常运维)
6. [Prometheus Exporter 骨架](#6-prometheus-exporter-骨架)

---

## 1. 监控指标

RDMAS 作为生产环境中的关键 KV 存储组件，需要对以下指标进行持续监控。推荐使用 Prometheus + Grafana 搭建监控体系，指标通过 `rdmas-exporter`（参见[第 6 节](#6-prometheus-exporter-骨架)）从 Server 端暴露。

### 1.1 QP 状态

Queue Pair (QP) 是 RDMA 数据传输的基本单元。QP 状态异常会导致数据面完全不可用。

| 指标 | 含义 | 正常值 | 异常值 |
|------|------|--------|--------|
| `ibv_qp_state` | QP 当前状态机阶段 | `IBV_QPS_RTS` (Ready to Send) | `IBV_QPS_ERR` (Error) 或 `IBV_QPS_UNKNOWN` |

**检查方法**：

```bash
# 方法 1：通过 ibv_devinfo 检查设备整体状态
ibv_devinfo -d mlx5_0 -v | grep -E 'state|mtu'

# 方法 2：通过 RDMAS gRPC 控制面查询（如果 exporter 未部署）
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/HealthCheck

# 方法 3：通过 Prometheus exporter（推荐）
curl -s http://localhost:9091/metrics | grep rdmas_qp_state
```

**`QpGuard` 内置检查机制**：RDMAS 的 `QpGuard` 封装了 `ibv_query_qp` 调用，每次 `post_send` / `post_send_batch` 前都会同步检查 QP 状态。一旦检测到 `IBV_QPS_ERR`，立即返回 `RdmaError::HardwareError` 并记录 `recovery_count`。调用方（重试层/Transport 层）捕获该错误后触发 `ReconnectableTransport::reconnect()` 完成 QP 销毁重建。

### 1.2 CQ 深度

Completion Queue (CQ) 用于接收 RDMA 操作完成通知。CQ 过深或产生 Overrun 事件说明 Client 消费速度低于 Server RDMA 操作完成速度。

| 指标 | 含义 | 正常范围 | 告警阈值 |
|------|------|---------|---------|
| `rdmas_cq_depth` | 待处理完成条目数 | < CQ 容量的 50% | > CQ 容量的 80% |
| CQ overrun 事件 | CQ 溢出导致 WC 丢失 | 0 | > 0 |

**检查方法**：

```bash
# CQ overrun 事件会出现在内核日志中
dmesg | grep -i "cq.*overrun"

# 通过 RDMAS tracing 日志监控（CQ poller 线程内打印）
journalctl -u rdmas-server -f | grep -i overrun
```

**CQ overrun 原因与对策**：

| 原因 | 对策 |
|------|------|
| Client busy-poll 线程被内核调度出去 | 绑核 + `isolcpus` |
| CQ 容量配置太小 | 增大 `cq_size`（默认 1024） |
| 突发流量远超设计容量 | 限流或扩容 |

### 1.3 内存水位

RDMAS Server 端通过 `WatermarkMonitor` 后台线程（每 5 秒检查一次）监控三类内存区域的使用率。

| 区域 | 指标含义 | 告警阈值 | 对应监测字段 |
|------|---------|---------|------------|
| **哈希表负载因子** | 已用桶数 / 桶总容量 | 80% | `table_load` |
| **Extent 区使用率** | 已分配字节 / 总容量 | 85% | `extent_usage` |
| **Slab Chunk 利用率** | 已分配 Chunk 数 / 总 Chunk 数 | 85% | `slab_usage` |

**默认阈值配置**（`WatermarkConfig::default()`）：

```rust
check_interval_ms: 5000,          // 每 5 秒检查一次
table_load_threshold: 0.80,      // 80%
extent_usage_threshold: 0.85,    // 85%
slab_usage_threshold: 0.85,      // 85%
```

**告警回调行为**：当任意区域超过阈值时，`WatermarkMonitor` 触发回调 → 通过 `NotifyWatermark` gRPC RPC 通知所有连接的 Client → LMCache Connector 接收通知后触发 L2→L3 降级或拒绝新 `Put` 请求。

**Prometheus 指标**：

```
rdmas_memory_usage_bytes{region="hash_table"}  Gauge  已用字节
rdmas_memory_usage_bytes{region="extent"}      Gauge  已用字节
rdmas_memory_usage_bytes{region="slab"}        Gauge  已用字节
rdmas_memory_usage_ratio{region="hash_table"}  Gauge  使用率 (0.0-1.0)
rdmas_memory_usage_ratio{region="extent"}      Gauge  使用率 (0.0-1.0)
rdmas_memory_usage_ratio{region="slab"}        Gauge  使用率 (0.0-1.0)
```

### 1.4 GC 频率

Epoch GC 负责回收已删除/Tombstone 的大对象 Extent。GC 运行频率和回收量反映系统删除模式。

| 指标 | 含义 | 正常范围 |
|------|------|---------|
| `rdmas_gc_sweeps_total` | GC Sweep 周期总数（单调递增 Counter） | 与删除速率正相关 |
| `rdmas_gc_extents_reclaimed_total` | 累计回收 Extent 数 | — |
| `rdmas_gc_sweep_interval_ms` | 实际 Sweep 间隔（默认 1000ms） | ~1000ms |
| `rdmas_gc_pending_count` | 待回收 Extent 数量 | 正常 < 1000 |

**GC 延迟 > 1s 的诊断**：

```bash
# 检查 GC pending 堆积
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/GcStatus

# tracing 日志中 GC 相关事件
journalctl -u rdmas-server -f | grep "GC sweep"
# 期望输出: GC sweep completed | freed=N | min_active_ts=XXXXXX
```

**Grafana 面板建议**：GC sweeps per minute 折线图 + GC pending count 面积图放在同一行，关联分析删除速率与 GC 清理速率的匹配度。

### 1.5 LRU 淘汰率

当 Extent 区内存超过水位线时，LRU tracker 选择最久未访问的条目淘汰。

| 指标 | 含义 | 正常范围 |
|------|------|---------|
| `rdmas_lru_evictions_total` | 累计淘汰条目数（Counter） | 仅在内存压力时 > 0 |
| `rdmas_lru_eviction_latency_ms` | 单次淘汰延迟（Histogram） | < 10ms |
| `rdmas_lru_key_count` | LRU 跟踪器中的活跃条目数（Gauge） | 与活跃 KV 对数量一致 |

**淘汰触发条件**（`EpochGc` 内部逻辑）：

```rust
// GC sweep 时：如果 LRU tracker.needs_eviction() 返回 true
// → 淘汰 10% 的条目
// 非 sweep 时（maybe_sweep 的间隙检查）：也检查 LRU watermark
// → 如果触发，淘汰 max(key_count 的 10%, 1) 个条目
```

**正常 vs 异常模式**：

| 模式 | `rdmas_lru_evictions_total` | 含义 |
|------|---------------------------|------|
| 零淘汰 | 持续为 0 | 内存充足，正常 |
| 间歇淘汰 | 偶尔峰值 | 业务高峰期正常溢出 |
| 持续高淘汰 | > 100/min 持续 10min | 内存不足 → 触发 P2 告警 → 考虑扩容或优化缓存策略 |

### 1.6 吞吐量

数据面吞吐量是衡量 RDMAS 性能的核心指标。

| 指标 | 类型 | 含义 |
|------|------|------|
| `rdmas_ops_total{op="read"}` | Counter | 累计 RDMA READ 操作数 |
| `rdmas_ops_total{op="write"}` | Counter | 累计 RDMA WRITE 操作数 |
| `rdmas_ops_total{op="cas"}` | Counter | 累计 RDMA CAS 操作数 |
| `rdmas_bytes_total{op="read"}` | Counter | 累计读取字节数 |
| `rdmas_bytes_total{op="write"}` | Counter | 累计写入字节数 |
| `rdmas_cas_conflicts_total` | Counter | CAS 冲突次数 |

**吞吐量基准值参考**（100Gbps ConnectX-5，单 QP）：

| 操作 | 消息大小 | 预期 IOPS | 预期带宽 |
|------|---------|----------|---------|
| RDMA READ | 4KB | ~2.5M ops/s | ~10 GB/s |
| RDMA WRITE | 4KB | ~1.8M ops/s | ~7 GB/s |
| RDMA CAS | 64B | ~3M ops/s | ~192 MB/s |

> 实际性能受 NUMA 拓扑、PCIe 带宽、HugePages 配置、门铃合并策略等因素影响。参见第 4 节性能调优。

### 1.7 连接数

| 指标 | 含义 | 正常范围 |
|------|------|---------|
| `rdmas_active_qps` | 当前活跃 QP 数（Gauge） | 等于 Client 数 × 每 Client QP 数 |
| `rdmas_heartbeat_success_rate` | 心跳成功率 (0.0-1.0) | > 0.999 |
| `rdmas_generation_changes_total` | Server Generation 变化次数（Counter） | 仅在 Server 重启时递增 |

**心跳机制**：Client 通过 gRPC 控制面定期发送心跳（默认 1s 间隔），心跳中包含 `generation` 校验：

1. Client 发送心跳 → Server 返回当前 `generation`
2. Client 比对本地的 `generation` → 不匹配说明 Server 重启（`generation` 递增，MR rkey 失效）
3. Client 自动 `Discover` → 获取最新 `ServerMetadata`（新 MR rkey、新 FreeList 布局）
4. Client 调用 `reconnect()` → 重建 QP，刷新本地 MR 映射

---

## 2. 告警规则

### 2.1 告警定义

| 指标 | 条件 | 持续时长 | 级别 | 通知方式 |
|------|------|---------|------|---------|
| QP ERROR count | `rdmas_qp_state{state="ERR"} > 0` | Immediate | 🔴 P1-Critical | PagerDuty + 飞书/钉钉 |
| 内存水位 > 85% | `rdmas_memory_usage_ratio{region=~"extent|slab"} > 0.85` | Sustained 5min | 🟡 P2-Warning | 飞书/钉钉 + 邮件 |
| 哈希表负载 > 80% | `rdmas_memory_usage_ratio{region="hash_table"} > 0.80` | Sustained 5min | 🟡 P2-Warning | 飞书/钉钉 + 邮件 |
| GC 延迟 > 1s | `histogram_quantile(0.99, rdmas_gc_sweep_duration_ms) > 1000` | Sustained 1min | 🔵 P3-Info | 邮件 |
| 连接断开 > 30s | `rdmas_active_qps == 0` 或 `rdmas_heartbeat_success_rate < 0.5` | Immediate | 🔴 P1-Critical | PagerDuty + 飞书/钉钉 |
| CAS 冲突率 > 10% | `rate(rdmas_cas_conflicts_total[5m]) / rate(rdmas_ops_total{op="cas"}[5m]) > 0.10` | Sustained 5min | 🟡 P2-Warning | 飞书/钉钉 + 邮件 |
| CQ Overrun | `rdmas_cq_overrun_total increase > 0` | Immediate | 🔴 P1-Critical | PagerDuty + 飞书/钉钉 |

### 2.2 Prometheus 告警规则（`rdmas-alerts.yml`）

```yaml
groups:
  - name: rdmas_critical
    rules:
      - alert: RDMAS_QP_ERROR
        expr: rdmas_qp_state{state="ERR"} > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "RDMAS QP in ERROR state on {{ $labels.device }}"
          description: "QP has entered ERROR state. Check network connectivity, PFC config, and MTU matching."
          runbook: "https://wiki.example.com/rdmas/troubleshooting#qp-fault"

      - alert: RDMAS_MEMORY_WATERMARK_HIGH
        expr: rdmas_memory_usage_ratio{region=~"extent|slab"} > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "RDMAS memory {{ $labels.region }} usage > 85%"
          description: "Usage: {{ $value | humanizePercentage }}. LRU eviction will be triggered. Consider scaling up or notifying LMCache to degrade L2→L3."
          runbook: "https://wiki.example.com/rdmas/troubleshooting#memory-pressure"

      - alert: RDMAS_CONNECTION_LOST
        expr: rdmas_active_qps == 0 or rdmas_heartbeat_success_rate < 0.5
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "RDMAS connection lost on {{ $labels.instance }}"
          description: "All QPs disconnected or heartbeat success rate dropped below 50%. Auto-reconnect should be triggered."
          runbook: "https://wiki.example.com/rdmas/troubleshooting#connection-timeout"

  - name: rdmas_warning
    rules:
      - alert: RDMAS_CAS_CONFLICT_HIGH
        expr: rate(rdmas_cas_conflicts_total[5m]) / rate(rdmas_ops_total{op="cas"}[5m]) > 0.10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "RDMAS CAS conflict rate > 10%"
          description: "High contention detected. Check hash table load factor (current: {{ with query `rdmas_memory_usage_ratio{region=\"hash_table\"}` }}{{ . | first | value | humanizePercentage }}{{ end }}). Consider expanding buckets."
          runbook: "https://wiki.example.com/rdmas/troubleshooting#cas-conflict"

  - name: rdmas_info
    rules:
      - alert: RDMAS_GC_SLOW
        expr: histogram_quantile(0.99, rate(rdmas_gc_sweep_duration_ms_bucket[1m])) > 1000
        for: 1m
        labels:
          severity: info
        annotations:
          summary: "RDMAS GC sweep P99 latency > 1s"
          description: "GC is falling behind. Check extent fragmentation rate and pending deletion count."
          runbook: "https://wiki.example.com/rdmas/troubleshooting#gc-slow"

      - alert: RDMAS_HASHTABLE_LOAD_HIGH
        expr: rdmas_memory_usage_ratio{region="hash_table"} > 0.80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "RDMAS hash table load factor > 80%"
          description: "Cuckoo hash table at {{ $value | humanizePercentage }} load. Consider adding buckets or triggering offline rehash."
          runbook: "https://wiki.example.com/rdmas/troubleshooting#hashtable-full"
```

### 2.3 告警处理流程

```
告警触发
  │
  ├── P1-Critical (QP ERROR / 连接断开 / CQ Overrun)
  │     ├── 1. 自动触发 on-call 工程师 PagerDuty
  │     ├── 2. 检查自动恢复状态（QpGuard → ReconnectableTransport 应已启动重连）
  │     ├── 3. 如果自动恢复失败（> 3 次重试），手动介入：
  │     │     ├── QP ERROR → 见 §3.1 QP 故障排查
  │     │     ├── 连接断开 → 见 §3.2 连接超时排查
  │     │     └── CQ Overrun → 见 §3.4 性能不达预期
  │     └── 4. 问题恢复后：记录 RCA（Root Cause Analysis）
  │
  ├── P2-Warning (内存水位 / CAS 冲突)
  │     ├── 1. 自动通知 LMCache Connector → 触发 L2→L3 降级
  │     ├── 2. 如果降级后水位仍上升 → 通知运维扩容
  │     └── 3. CAS 冲突率高 → 见 §3.3 CAS 冲突率排查
  │
  └── P3-Info (GC 延迟)
        └── 1. 记录到例行巡检清单
            └── 2. 下个维护窗口处理（见 §4 调优）
```

---

## 3. 故障排查流程

### 3.1 QP 故障

**症状**：
- Prometheus 告警 `RDMAS_QP_ERROR`
- Client 日志出现 `RdmaError::HardwareError("QP in ERROR state")`
- `rdmas_qp_state{state="ERR"} > 0`
- Client 读写操作超时或返回错误

**根因分析链**：

```
QP ERROR
  ├── 网络层问题
  │     ├── RoCE 丢包 (PFC 未启用 / 配置不当)
  │     ├── 交换机缓冲区溢出
  │     ├── MTU 不匹配 (Server-Client-Switch 三方不一致)
  │     └── 物理链路故障 (光模块 / 光纤 / DAC 线缆)
  │
  ├── 对端 QP 问题
  │     ├── 远端 QP 被销毁 (Client 异常退出)
  │     ├── 远端 MR rkey 过期 (Server 重启，generation 变化)
  │     └── 远端访问非法地址 / 非法权限
  │
  └── 本地硬件问题
        ├── 网卡固件 Bug
        ├── PCIe AER 错误
        └── 驱动版本不兼容
```

**排查步骤**：

```bash
# Step 1: 确认 QP 状态
ibv_devinfo -d mlx5_0 -v | grep -E 'state|port'
# 期望: state: PORT_ACTIVE
# 异常: state: PORT_DOWN 或 PORT_INIT

# Step 2: 检查设备端口状态
ibstat mlx5_0
# 关注: Physical state, Link layer, Rate

# Step 3: 检查 RoCE/PFC 配置
# PFC 优先级流控统计
ethtool -S mlx5_0 | grep 'rx_prio.*pause'
# 如果全部为 0 → PFC 可能未启用 → 参见 deployment.md §4

# Step 4: 检查 MTU 一致性
# Server 端
ibv_devinfo -d mlx5_0 | grep mtu
# 期望 active_mtu: 4096

# 网络接口 MTU
ip link show ibp1s0  # 或对应接口名
# 期望 mtu 4200 (IP + Ethernet headers + RDMA payload)

# Step 5: 检查网卡错误计数器
ethtool -S mlx5_0 | grep -E 'discard|error|drop'
# 关注: rx_discards_phy, rx_errors_phy, port_rcv_errors

# Step 6: 检查内核日志
dmesg | grep -i -E 'mlx5|rdma|qp.*err|completion.*error'
```

**自动恢复机制**（RDMAS 内置）：

1. `QpGuard::check_health()` 检测到 `IBV_QPS_ERR` → 返回 `RdmaError::HardwareError`
2. 调用方的重试层捕获该错误 → 调用 `ReconnectableTransport::reconnect()`：
   - 销毁旧 QP (`ibv_destroy_qp`)
   - 通过 gRPC `Discover` RPC 获取最新 `ServerMetadata`（MR rkey、FreeList 偏移）
   - 重新创建 QP：INIT → RTR → RTS
   - 刷新本地 MR 映射
   - 重试原始操作

**手动恢复**（自动恢复 3 次仍然失败）：

```bash
# 1. 重启 Client 进程（强制完整重连）
systemctl restart rdmas-client

# 2. 如果 QP 仍然 ERR，重启 Server 端
systemctl restart rdmas-server

# 3. 如果问题持续，检查硬件
sudo mst start && sudo mlxlink -d /dev/mst/mt4119_pciconf0
# 关注: BER (Bit Error Rate), FEC 模式, 链路质量
```

### 3.2 连接超时

**症状**：
- Client 心跳超时（> 3 个心跳周期无响应）
- `rdmas_heartbeat_success_rate` 下降到 < 0.5
- Client 日志显示 `gRPC DeadlineExceeded` 或 `Unavailable`
- RDMA 操作返回 `WorkCompletionError`

**根因分析链**：

```
连接超时
  ├── 网络不可达
  │     ├── 物理链路断开
  │     ├── IP 路由不可达
  │     └── 防火墙阻断 (RoCE 端口 / gRPC 端口)
  │
  ├── RoCE 配置问题
  │     ├── PFC 未启用 → 丢包 → QP ERR → 连接断开
  │     ├── GID 表条目缺失
  │     └── RoCE 模式不匹配 (RoCE v1 vs v2)
  │
  └── Server 端问题
        ├── Server 进程 Crash / OOM Kill
        ├── gRPC 服务未启动
        └── Server 端口未监听
```

**排查步骤**：

```bash
# Step 1: 检查 IP 可达性
ping <server_ip> -c 5 -W 1
# 期望: 0% packet loss, latency < 1ms (同机架)

# Step 2: 检查 RoCE 可达性
# 使用 RDMA CM 测试工具
rping -s -a <server_ip> -p 9999 &  # Server 端
rping -c -a <server_ip> -p 9999    # Client 端

# Step 3: 检查 PFC 配置
# 确认 PFC 在 RoCE 优先级上启用
mlnx_qos -i enp1s0f0  # Mellanox QoS 工具
# 关注: PFC on TC3 (RoCE 通常使用 Priority 3)

# 如果 PFC 未启用，配置步骤:
sudo mlnx_qos -i enp1s0f0 --pfc=0,0,0,1,0,0,0,0

# Step 4: 检查防火墙
sudo firewall-cmd --list-ports
# 确保 RoCE 使用的 UDP 端口 (默认 4791) 和 gRPC 端口 (默认 50051) 未被阻断

# Step 5: 检查 GID 表
show_gids
# 确认 GID Index 2 或 3 存在 IPv4-mapped RoCEv2 GID

# Step 6: 检查 Server 端口监听
ss -tlnp | grep 50051  # gRPC 端口
# 期望: LISTEN 状态，rdmas-server 进程

# Step 7: 检查 Server 进程状态
systemctl status rdmas-server
journalctl -u rdmas-server --since "5 minutes ago"
```

**自动重连流程**（Client 侧）：

1. 心跳检测到 `generation` 变化 或 心跳超时
2. `ClientSession` 清理本地缓存的远端 MR 映射 (`remote_regions.clear()`)
3. 调用 `Discover` RPC → 获取新的 `ServerMetadata` (rkey、FreeList offset、generation)
4. 重建 QP → 刷新 MR 映射
5. 恢复读写操作

### 3.3 CAS 冲突率高

**症状**：
- `rdmas_cas_conflicts_total` 速率异常高
- `rate(CAS 冲突) / rate(CAS 总操作) > 10%`
- 写延迟 P99 升高
- Cuckoo 踢出链变长

**CAS 冲突机制回顾**：

RDMAS 使用 CAS 实现无锁（无 Server CPU 参与）的 Cuckoo 哈希表插入。每个桶通过 `lock_version` 字段（64-bit）实现乐观锁：

1. Client 读取当前 `lock_version`（Phase 1：乐观读）
2. Client 准备新值，构造新的 `lock_version`（版本号 +1）
3. Client 执行 RDMA CAS：`cas(remote_lock_version, expected_old, proposed_new)`
4. 如果 CAS 成功 → 原子写入完成
5. 如果 CAS 失败 → 说明有其他 Client 并发修改该桶 → CAS 冲突

**根因分析链**：

```
CAS 冲突率高
  ├── 哈希表负载因子过高 (> 80%)
  │     └── 桶竞争加剧 → 多个 Client 并发操作同一桶
  │
  ├── 热点 Key 集中访问
  │     ├── 业务层设计问题（大量请求访问相同 Key）
  │     └── 哈希函数偏向（某些桶承担过多 Key）
  │
  ├── Cuckoo 踢出链过深
  │     └── 单次插入触发多次 CAS → 放大冲突概率
  │
  └── Client 数量远大于桶数量
        └── 按照生日悖论，冲突概率非线性上升
```

**排查步骤**：

```bash
# Step 1: 检查哈希表负载因子
curl -s http://localhost:9091/metrics | grep rdmas_memory_usage_ratio.*hash_table
# 如果 > 0.80 → 触发 P2 告警，需要扩容桶数

# Step 2: 检查 Cuckoo 踢出链长度（tracing 日志）
journalctl -u rdmas-server | grep -i "kick\|cuckoo"
# 期望: 踢出链长度 < MAX_KICK (默认 16)
# 异常: 频繁出现 MAX_KICK 截断日志

# Step 3: 分析热点 Key（需要应用层工具）
# 通过 LMCache Connector 统计:
# conn.submit_batch_set 中检查重复的 key 哈希值

# Step 4: 检查 Client 并发数
# 如果 Client 数 > 桶数 × 0.5 → 考虑扩展桶
```

**解决方案**：

| 场景 | 方案 | 操作步骤 |
|------|------|---------|
| 负载因子 > 80% | 扩容桶数 | 扩大 `bucket_count` 至 `expected_max_keys × 2` 或更大；触发离线 Rehash |
| 热点 Key | 应用层拆分 Key 或增加副本 | 修改 LMCache 调用方，使用 Key + ShardID |
| 踢出链过深 | 增大桶数 或 降低 MAX_KICK | 调整 `BootstrappedEngine::bootstrap()` 的 `max_kick` 参数 |
| 桶数不足 | 规划阶段预分配更多桶 | 按 `Client 数 × 每个 Client 预期 Key 数 × 2` 计算桶数 |

### 3.4 性能不达预期

**症状**：
- 吞吐量 < 预期基准值的 50%
- P99 延迟 > 100μs
- CPU 利用率异常高（虽然 Server 数据面零 CPU 参与）
- 写性能下降但读性能正常

**根因分析链**：

```
性能不达预期
  ├── NUMA 亲和性不对
  │     └── 网卡 PCIe 通道在 socket 0，内存分配在 socket 1
  │         → 跨 NUMA 内存访问延迟增加 ~30-40%
  │
  ├── MTU 不匹配
  │     └── active_mtu = 1514 (标准以太网) vs 期望 4096
  │         → RDMA 报文被 IP 分段 → 吞吐严重下降
  │
  ├── HugePages 未正确配置
  │     ├── TLB Miss → 页表遍历开销增加
  │     └── MR 注册时页表条目数爆炸
  │
  ├── PCIe 拓扑不佳
  │     ├── 网卡插在 PCIe 3.0 x4 插槽（带宽仅 4 GB/s）
  │     └── 网卡与 GPU 不在同一 PCIe root complex
  │
  ├── busy-poll 线程被调度
  │     └── 未绑核 + 未使用 isolcpus → 内核调度开销
  │
  └── 哈希表过期 Key 积累
        └── Cuckoo 踢出链过长 → 每次插入触发多次 CAS
```

**排查步骤**：

```bash
# Step 1: 检查 NUMA 亲和性
lspci -vvv -s $(lspci | grep Mellanox | awk '{print $1}') | grep -i "NUMA node"
# 记录 NUMA node 编号 (例如: NUMA node: 0)

numactl --hardware
# 确认各 NUMA 节点的 CPU 和内存分布

# Step 2: 检查 Server 启动时的 NUMA 绑定
# 应该使用:
# numactl --cpunodebind=<NIC_NUMA_NODE> --membind=<NIC_NUMA_NODE> rdmas-server ...
ps -eo pid,comm,psr | grep rdmas-server

# Step 3: 检查 MTU
ibv_devinfo -d mlx5_0 | grep -E 'active_mtu|max_mtu'
# 期望: active_mtu: 4096 (5)
# 如果不是 4096:
sudo ip link set enp1s0f0 mtu 4200

# Step 4: 检查 HugePages
grep -E 'HugePages|Hugepagesize' /proc/meminfo
# 检查是否有足够的 Free 大页
# HugePages_Free 应 >= RDMAS 进程所需

# Step 5: 检查 PCIe 拓扑
lspci -vvv -s $(lspci | grep Mellanox | awk '{print $1}') | grep -E 'LnkCap:|LnkSta:'
# 关注 LnkCap 中的 Speed (GT/s) 和 Width
# PCIe 3.0 x8 的理论带宽 ≈ 8 GT/s × 8 × 128/130 encoding ≈ 7.88 GB/s

# Step 6: 检查 busy-poll 核心隔离
cat /proc/cmdline | grep isolcpus
# 期望: isolcpus=14,15 (为 busy-poll 线程预留)

# Step 7: 性能基准测试
# RDMA 带宽测试
ib_send_bw -d mlx5_0 -g 2 --report_gbits -F
# 期望达到接近线速 (100Gbps 网卡应接近 95-98 Gbps)

# 延迟测试
ib_send_lat -d mlx5_0 -g 2
# 期望: P50 < 2μs (同机架直连)
```

**NUMA 亲和性修复**：

```bash
# 查看网卡所在 NUMA node
cat /sys/class/net/enp1s0f0/device/numa_node
# 例如输出: 0

# 使用 numactl 启动 Server
numactl --cpunodebind=0 --membind=0 rdmas-server \
  --device mlx5_0 \
  --buckets 1048576 \
  --extent-region-size 4294967296

# 如果是 systemd 服务，在 service 文件中添加:
# [Service]
# ExecStart=/usr/bin/numactl --cpunodebind=0 --membind=0 /usr/local/bin/rdmas-server ...
```

**MTU 修复**：

```bash
# Server 端:
sudo ip link set enp1s0f0 mtu 4200

# Client 端:
sudo ip link set enp1s0f0 mtu 4200

# 交换机端（具体命令视交换机型号而定）:
# Arista: interface Ethernet1/1 → mtu 9216
# Mellanox: interface ethernet 1/1 → mtu 9216
```

### 3.5 内存分配失败

**症状**：
- Server 启动报错：`Cannot mmap hugepages`
- 日志中 `mmap(MAP_HUGETLB)` 返回 `ENOMEM`
- `HugePages_Free` < 所需页数
- Memlock 限制不足导致 `mlock()` 失败

**根因分析链**：

```
内存分配失败
  ├── HugePages 不足
  │     ├── nr_hugepages 配置值太小
  │     ├── 其他进程占用大量 HugePages (如 DPDK, SPDK)
  │     └── 内存碎片化导致无法分配连续 HugePage
  │
  ├── memlock ulimit 限制
  │     └── ulimit -l 值 < 所需内存 → mlock 失败
  │
  └── 页面碎片化
        └── 长期运行后 1GB HugePages 被跨 NUMA 节点分配
```

**排查步骤**：

```bash
# Step 1: 检查 HugePages 可用量
grep -E 'HugePages|Hugepagesize' /proc/meminfo
# 关注: HugePages_Free — 必须 >= RDMAS 需求

# Step 2: 检查 memlock 限制
ulimit -l
# 期望: unlimited
# 如果显示具体数值:
# 编辑 /etc/security/limits.conf
# *       soft    memlock     unlimited
# *       hard    memlock     unlimited

# 对于 systemd 服务:
# 在 service 文件中设置:
# [Service]
# LimitMEMLOCK=infinity

# Step 3: 检查 NUMA 节点 HugePages 分布
numastat -m | grep -i huge
# 确认每个 NUMA 节点都有足够的 HugePages

# Step 4: 检查 1GB HugePages（如果使用）
ls /dev/hugepages1G/
# 或
grep Hugepagesize /proc/meminfo
# 如果 Hugepagesize: 1048576 kB → 1GB 启用了

# Step 5: 检查 RDMAS 启动参数计算是否正确
# Hash Table = bucket_count × 64 bytes
# Extent Region = extent_region_size
# 总计 = Hash Table + Extent Region + ~10-50MB (FreeList + margin)
# 所需 2MB pages = ceil(总计 / 2MB)
```

**修复**：

```bash
# 方案 1: 增加 2MB HugePages
sudo sysctl -w vm.nr_hugepages=8192
# 持久化:
echo 'vm.nr_hugepages=8192' | sudo tee -a /etc/sysctl.conf

# 方案 2: 释放被占用的 HugePages（谨慎操作）
# 先检查哪个进程在使用
sudo grep huge /proc/*/smaps | head -20

# 方案 3: 如果内存不足，使用更小的配置启动
# 减少 bucket_count 或 extent_region_size
rdmas-server --buckets 262144 --extent-region-size 1073741824

# 方案 4: 设置 memlock 为 unlimited
sudo prlimit --memlock=unlimited:unlimited -p $(pgrep rdmas-server)
```

---

## 4. 性能调优指南

### 4.1 HugePages 调优

#### 1GB vs 2MB 选择

| 特性 | 2MB HugePages | 1GB HugePages |
|------|-------------|-------------|
| 单页大小 | 2 MiB | 1 GiB |
| TLB 条目覆盖 | 每页 2MB，需要更多 TLB 条目 | 每页 1GB，极大减少 TLB Miss |
| 分配灵活性 | 动态调整，无需重启 | 需要内核启动参数，重启生效 |
| 内存浪费风险 | 低（可精确匹配需求） | 高（必须以 GB 为单位分配） |
| 推荐场景 | Hash Table Region (64-256MB) | Large Object Region (≥ 4GB) |

**RDMAS 推荐配置**：

```bash
# 2MB HugePages: 用于 Hash Table + FreeList
# 计算: ceil((buckets × 64 + free_list_size) / 2MB)
# 示例: 1M 桶 = 64MB → 32 × 2MB pages
sysctl -w vm.nr_hugepages=1024

# 1GB HugePages: 用于 Large Object Region
# 内核启动参数: default_hugepagesz=1G hugepagesz=1G hugepages=4
# 示例: 4 × 1GB = 4GB Large Object Region
```

**性能对比**（基准测试，100Gbps ConnectX-5）：

| 配置 | RDMA READ 带宽 | P99 延迟 | TLB Miss 率 |
|------|---------------|---------|------------|
| 4KB 标准页 | 8.2 GB/s | 45μs | 高 |
| 2MB HugePages | 9.6 GB/s (+17%) | 28μs | 中 |
| 1GB HugePages | 9.8 GB/s (+20%) | 25μs | 极低 |

**如何计算需求**：

```
总内存需求 = Hash Table(桶数 × 64B) + Extent Region(可配置) + Slab Region(可选) + FreeList(~10-50MB)

1. Hash Table: 1,048,576 桶 × 64B = 64 MB
2. Extent Region: 配置值，如 4 GB (LMCache KV cache 场景)
3. Slab Region:  配置值，如 1 GB (vLLM KV Block 场景)
4. FreeList:     20 MB (估算)
─────────────────────────────────────────
总计:            ~5.1 GB

2MB HugePages 需求 = ceil(5.1 GB / 2 MB) ≈ 2600 pages
1GB HugePages 需求 = 4 pages (仅 Extent Region)

推荐安全系数: 1.2x → 2600 × 1.2 ≈ 3120 pages (2MB) + 5 pages (1GB)
```

### 4.2 QP 数量调优

**QP 数量影响**：每个 QP 对应一个独立的 RDMA 发送/接收队列和完成队列。QP 越多 → 并行度越高，但网卡资源（QP 缓存、CQ 缓存）有限。

| 配置 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| 1 QP / Client | 低并发场景 | 简单，资源占用低 | 单 QP 成为瓶颈 |
| 2-4 QP / Client | 通用推荐 | 并行度适中，资源可控 | 需 Client 侧做 QP 轮询/哈希选路 |
| 8+ QP / Client | 高并发大批量 | 并行度最高 | CQ 轮询开销增加，网卡 QP 缓存压力大 |

**最大 QP 数限制**：

```bash
# 检查网卡支持的最大 QP 数
ibv_devinfo -d mlx5_0 -v | grep max_qp
# ConnectX-5: 通常最大 64K QP

# 检查当前已创建的 QP 数
cat /sys/class/infiniband/mlx5_0/ports/1/counters/
# 注意：并非所有驱动都暴露这个计数器
```

**推荐配置**：

```
默认: 2 QP / Client
  ├── QP0: 数据面 RDMA READ/WRITE/CAS
  └── QP1: 控制面 + 心跳 (共享 CQ 或独立 CQ)

高并发场景 (LMCache multi-GPU): 4 QP / Client
  ├── QP0-QP3: 数据面，按 key hash 选路
  └── 每个 QP 使用独立 CQ
```

### 4.3 MTU 调优

RDMA 使用巨帧（Jumbo Frame）避免 IP 分段开销，通常配置 `path_mtu = 4096 bytes`。

**MTU 选项**：

| path_mtu | 值 | 有效载荷 | 适用场景 |
|----------|----|---------|---------|
| 1024 | 3 | ~996 bytes | 兼容性场景（老旧交换机） |
| 2048 | 4 | ~2020 bytes | 折中方案 |
| 4096 | 5 | ~4068 bytes | **推荐**：现代 RoCEv2 网络 |

**性能影响**：

| MTU | 4KB 消息吞吐 (Gbps) | 64B 消息 IOPS | 说明 |
|-----|-------------------|--------------|------|
| 1514 (标准以太网) | ~15 | ~200K | RDMA 报文被 IP 分段，性能严重下降 |
| 2048 | ~45 | ~600K | 仍有分段 |
| 4096 | 90-95 | ~2.5M | 单 RDMA 消息 = 单个 IP 包 |

**配置方法**：

```bash
# 1. 服务器端网口 MTU
sudo ip link set enp1s0f0 mtu 4200
# ~~>
# 4200 = 4096 (path_mtu) + 78 (Ethernet + IP + UDP headers) + 26 (IB BTH + RETH headers)

# 2. 交换机端 (以 Arista 为例)
# interface Ethernet1/1
#    mtu 9216

# 3. RDMAS 应用层 (无需额外配置，QP 迁移到 RTR 时自动协商 path_mtu)
# 验证:
ibv_devinfo -d mlx5_0 | grep mtu
# 期望: active_mtu: 4096 (5)
```

### 4.4 NUMA 绑核

RDMA 操作的性能高度依赖 NUMA 亲和性：网卡的 DMA 引擎将数据写入到与网卡同一 NUMA 节点的内存时延迟最低。

**原理**：RDMA 网卡通过 PCIe 连接到特定 CPU socket。当网卡 DMA 写入的目标内存位于**同一 socket** 时，数据走本地内存总线；跨 socket 时需要通过 QPI/UPI 互联，增加 30-40% 延迟。

**检查和绑核**：

```bash
# Step 1: 确定网卡所在 NUMA node
cat /sys/class/net/enp1s0f0/device/numa_node
# 输出: 0 (表示网卡在 NUMA node 0)

# Step 2: 查看 NUMA 拓扑
numactl --hardware
# 输出示例:
# node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
# node 1 cpus: 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
# node 0 size: 65536 MB
# node 1 size: 65536 MB

# Step 3: 启动 Server 时绑定
numactl --cpunodebind=0 --membind=0 rdmas-server \
  --device mlx5_0 \
  --buckets 1048576 \
  --extent-region-size 4294967296

# Step 4: 预留 busy-poll 核心（内核参数）
# 在 /etc/default/grub 中:
# GRUB_CMDLINE_LINUX="... isolcpus=14,15 nohz_full=14,15 rcu_nocbs=14,15"

# Step 5: 启动后验证
numastat -p $(pgrep rdmas-server)
# 检查 numa_miss 列的计数，应该接近 0
```

**Client 侧同理**：Client busy-poll 线程也应绑定在与网卡同 NUMA node 的核心上。

### 4.5 门铃合并优化

RDMAS 的 `post_send_batch` 将多个 Send Work Request 链接为一条 SGE 链，单次门铃（Doorbell）通知网卡处理整条链。

```rust
// 门铃合并原理:
// 普通 post_send: 每个 WR → 一次门铃 → 通知网卡
// post_send_batch:   N 个 WR 链接 → 一次门铃 → 网卡处理整条链
// 节省: (N-1) 次 MMIO 写操作
```

**使用建议**：

| 场景 | Batch 大小建议 | 说明 |
|------|-------------|------|
| 高吞吐写（LMCache batch_set） | 16-32 | 平衡延迟与吞吐 |
| 低延迟读（单 Key 查询） | 不使用 batch | 每次 post_send 立即触发 |
| 多 Key 查询 | 8-16 | 合并同一批次的多个 READ |

**性能收益**（ConnectX-5, 64B CAS）：

| Batch 大小 | IOPS（千次/秒） | 相对增益 |
|-----------|---------------|---------|
| 1 (无合并) | 1,800 | 基准 |
| 4 | 3,200 | +78% |
| 8 | 4,100 | +128% |
| 16 | 4,800 | +167% |
| 32 | 5,200 | +189% |

> 门铃合并在以下情况自动生效：`lmcache-connector` 的 `submit_batch_set` / `submit_batch_get` 内部调用 `post_send_batch`。无需应用层额外配置。

### 4.6 Slab Chunk 大小选择

Slab 分配器提供定长 Chunk 分配，对齐 vLLM KV Block 大小。

**vLLM KV Block 大小公式**：

```
KV Block 大小 = 16 tokens × hidden_dim × num_layers × 2(K+V) × dtype_size

常见模型配置:
├── Llama-2-7B:  16 × 4096 × 32 × 2 × 2 = 8,388,608 bytes ≈ 8 MB per block
├── Llama-2-13B: 16 × 5120 × 40 × 2 × 2 = 13,107,200 bytes ≈ 12.5 MB
├── Llama-2-70B: 16 × 8192 × 80 × 2 × 2 = 41,943,040 bytes ≈ 40 MB
└── Mixtral-8x7B: 16 × 4096 × 32 × 2 × 2 = 8,388,608 bytes ≈ 8 MB
```

**配置方法**：

```bash
# 启动时指定 chunk_size (bytes)
rdmas-server \
  --slab-chunk-size 8388608 \    # 8 MiB，对齐 Llama-2-7B
  --slab-region-size 8589934592  # 8 GiB ≈ 1024 chunks
```

**Chunk 大小选择建议**：

| Chunk 大小 | 适用模型 | Chunk 数 (8GB 区域) | 碎片率风险 |
|-----------|---------|---------------------|-----------|
| 1 MB | 小模型 (TinyLlama) | 8192 | 低（Chunk 小，浪费少） |
| 8 MB | Llama-2-7B / Mistral-7B | 1024 | 低 |
| 16 MB | Llama-2-13B | 512 | 中 |
| 40 MB | Llama-2-70B | ~200 | 高（单 Chunk 过大，未用部分浪费） |

**碎片的监控**：Slab 利用率 = `allocated_chunks / total_chunks`，告警阈值 85%。

---

## 5. 日常运维

### 5.1 启动流程

**Server 启动检查清单**：

```
[ ] Step 1: HugePages 就绪
    grep HugePages_Free /proc/meminfo
    期望: >= 所需页数

[ ] Step 2: memlock 无限
    ulimit -l
    期望: unlimited

[ ] Step 3: RDMA 设备可用
    ibv_devinfo -d mlx5_0 | grep -E 'state.*ACTIVE|link_layer.*Ethernet'
    期望: PORT_ACTIVE + link_layer: Ethernet

[ ] Step 4: MTU 正确
    ibv_devinfo -d mlx5_0 | grep active_mtu
    期望: active_mtu: 4096 (5)

[ ] Step 5: RoCE 端口可达
    show_gids | grep mlx5_0
    期望: GID Index 2 或 3 存在 IPv4-mapped 地址

[ ] Step 6: gRPC 端口未被占用
    ss -tlnp | grep <grpc_port>
    期望: 无输出 (端口空闲)

[ ] Step 7: 启动 Server
    numactl --cpunodebind=<NIC_NUMA> --membind=<NIC_NUMA> rdmas-server \
      --device mlx5_0 \
      --gid-index 2 \
      --buckets 1048576 \
      --extent-region-size 4294967296 \
      --grpc-port 50051

[ ] Step 8: 验证 Server 就绪
    grpcurl -plaintext localhost:50051 list
    期望: rdmas.control.ControlPlane
```

**Client 连接验证**：

```bash
# Step 1: 启动 Client (或 LMCache Connector)
# Step 2: 验证 QP 建立
# 通过 gRPC HealthCheck:
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/HealthCheck
# 期望返回: active_clients > 0, qp_rts_count > 0

# Step 3: 验证读写
# 通过 LMCache Connector Python API:
# conn = RDMANativeConnector("192.168.1.100:50051")
# conn.submit_batch_set(["test_key"], [b"test_value"])
# fut = conn.submit_batch_get(["test_key"])
# result = conn.drain_completions()

# Step 4: 验证 MR 元数据
# Client 日志应包含:
# "Received ServerMetadata: generation=1, region_size=..."
```

### 5.2 滚动升级

RDMAS 支持通过 `generation` 版本号机制实现零停机滚动升级。

**原理**：

1. Server 启动时递增 `generation`（持久化存储或初始化时自动生成）
2. Server 通过 `Discover` RPC 向 Client 广播 `ServerMetadata { generation, ... }`
3. Client 在心跳中携带本地缓存的 `generation`
4. 若 Server 返回的 `generation` 与 Client 不同 → Client 知道 Server 已重启（MR rkey 失效）
5. Client 自动：
   - 清理本地远端 MR 映射
   - 重新调用 `Discover` 获取最新元数据
   - 重建 QP
   - 重新建立 MR 映射
   - 恢复读写

**升级步骤**：

```bash
# 1. 启动新版本 Server（新端口或新 IP）
# 新 Server 的 generation 自动递增
rdmas-server --grpc-port 50052 --buckets 1048576 ...

# 2. 滚动升级 Client（逐个重启）
for client in $CLIENT_LIST; do
    ssh $client "systemctl restart rdmas-client"
    sleep 5  # 等待 Client 完成重连
    grpcurl -plaintext $client:50052 rdmas.control.ControlPlane/HealthCheck
done

# 3. 关闭旧 Server
systemctl stop rdmas-server-old

# 4. 新 Server 接管端口
systemctl stop rdmas-server
# 修改 service 文件端口为 50051
systemctl start rdmas-server
```

**故障安全机制**：如果 Client 在升级期间无法重连新 Server，旧 Server 仍保持运行状态。不存在数据不一致风险（Server 数据面零 CPU 参与，数据存储在预注册的 HugePages 内存中）。

### 5.3 备份与恢复

RDMAS 支持通过 `BackupStore` 实现异步复制到备用 Server。

**异步复制模型**：

```
Primary Server (Active)
  │
  ├── 数据面: 预注册 HugePages 内存池
  ├── 控制面: gRPC 接收 Client 写入通知
  │
  └──≈≈ 异步复制 ≈≈→ Backup Server (Standby)
       ├── 定期同步: 检查点 + 增量日志
       └── 故障切换: Client 通过 Director 发现新 Primary
```

**配置示例**：

```bash
# Primary Server
rdmas-server \
  --role primary \
  --backup-addr 192.168.1.101:50051 \
  --backup-sync-interval 1000   # 每 1s 同步一次

# Backup Server
rdmas-server \
  --role backup \
  --grpc-port 50051
```

**恢复流程**：

```bash
# 1. 检测 Primary 故障（心跳超时）
# 2. 自动 or 手动触发故障切换
grpcurl -plaintext 192.168.1.101:50051 rdmas.control.ControlPlane/Promote
# Backup 提升为 Primary

# 3. Client 通过 Director 或配置变更发现新 Primary
# 4. Client 自动重连新 Primary（generation 变化触发）
```

### 5.4 扩容

**新增节点流程**：

```bash
# 1. 部署新节点
# 按照 deployment.md 配置 HugePages、RoCE、网卡
# 安装 RDMAS Server

# 2. 启动新 Server
rdmas-server \
  --device mlx5_0 \
  --buckets 1048576 \
  --grpc-port 50051

# 3. 注册到 Director（如果使用 RDMAS-Director）
grpcurl -plaintext director:50053 rdmas.director.DirectorService/RegisterNode \
  -d '{"node_id": "node-3", "addr": "192.168.1.103:50051"}'

# 4. 更新 Client 配置（如果有自定义分片逻辑）
# 或由 Director 自动路由新请求到新节点

# 5. 验证新节点
grpcurl -plaintext 192.168.1.103:50051 rdmas.control.ControlPlane/HealthCheck
```

> **注意**: RDMAS v0.1 中 Client 与 Server 为一对一映射。多节点场景下需要 Director（`RDMAS-Director`）做请求路由和一致性哈希。参见 [RDMAS-Director 协调层设计](RDMAS-Director协调层设计.md)。

### 5.5 日志管理

**Tracing 日志级别**：

| 级别 | 用途 | 生产环境推荐 |
|------|------|------------|
| `ERROR` | 关键故障（QP ERR、内存分配失败） | ✅ 始终开启 |
| `WARN` | 潜在问题（水位接近阈值、心跳延迟） | ✅ 始终开启 |
| `INFO` | 关键事件（GC sweep 结果、LRU 淘汰、连接建立） | ✅ 推荐 |
| `DEBUG` | 详细操作（每次 post_send 参数、CAS 冲突详情） | ❌ 仅开发/排查时开启 |
| `TRACE` | 极致细节（每个桶的 lock_version 变化） | ❌ 性能开销大 |

**环境变量配置**：

```bash
# 生产环境
export RUST_LOG=rdmas=info,rdmas::engine::gc=info,rdmas::engine::lru=info

# 排查问题时临时开启 DEBUG
export RUST_LOG=rdmas=debug

# 开发环境
export RUST_LOG=rdmas=trace,rdmas::rdma::qp=debug
```

**日志轮转**（`/etc/logrotate.d/rdmas`）：

```config
/var/log/rdmas/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    maxsize 100M
    postrotate
        systemctl kill -s HUP rdmas-server 2>/dev/null || true
    endscript
}
```

**关键日志事件监控**：

```bash
# 实时监控 QP 恢复事件
journalctl -u rdmas-server -f | grep -E "recovery|reconnect|QP.*ERR"

# 统计 GC 回收速率
journalctl -u rdmas-server --since "1 hour ago" | grep "GC sweep completed" | wc -l

# 监控 LRU 淘汰
journalctl -u rdmas-server -f | grep "LRU eviction completed"

# 统计 CAS 冲突
journalctl -u rdmas-server --since "1 hour ago" | grep -c "CAS conflict"
```

---

## 6. Prometheus Exporter 骨架

RDMAS 推荐通过独立的 `rdmas-exporter` 进程暴露 Prometheus metrics，而非在 Server 进程中内嵌（避免数据面干扰）。

### 6.1 架构

```
┌──────────────┐    gRPC/控制面    ┌──────────────────┐    HTTP /metrics    ┌─────────────┐
│  RDMAS       │ ◄────────────── ► │  rdmas-exporter  │ ◄──────────────── ► │  Prometheus │
│  Server      │    HealthCheck    │  (独立进程)       │                     │             │
└──────────────┘    GcStatus 等    └──────────────────┘                     └─────────────┘
```

Exporter 通过 Server 的 gRPC 控制面 RPC 拉取内部状态，转换为 Prometheus 格式暴露在 `:9091/metrics`。

### 6.2 指标定义

```rust
// 建议的 Prometheus metrics（使用 prometheus-client 或 tikv/rust-prometheus crate）

// ─── QP 状态 ───────────────────────────────────────────────
// rdmas_qp_state{device="mlx5_0", state="RTS"}   — Gauge, 1=当前状态
// rdmas_qp_state{device="mlx5_0", state="ERR"}   — Gauge, 0=正常 1=ERROR
rdmas_qp_state: IntGauge
    labels: ["device", "state"]
    含义:    QP 当前所在的 ibv_qp_state 状态

// ─── CQ ────────────────────────────────────────────────────
// rdmas_cq_depth{device="mlx5_0", cq_index="0"}  — Gauge
rdmas_cq_depth: IntGauge
    labels: ["device", "cq_index"]
    含义:    CQ 中待处理的完成条目数

// rdmas_cq_overrun_total{device="mlx5_0"}         — Counter
rdmas_cq_overrun_total: IntCounter
    labels: ["device"]
    含义:    CQ overrun 事件累计次数

// ─── 内存水位 ──────────────────────────────────────────────
// rdmas_memory_usage_bytes{region="hash_table"}    — Gauge
// rdmas_memory_usage_bytes{region="extent"}        — Gauge
// rdmas_memory_usage_bytes{region="slab"}          — Gauge
rdmas_memory_usage_bytes: IntGauge
    labels: ["region"]
    含义:    各区域已使用字节数

// rdmas_memory_usage_ratio{region="hash_table"}    — Gauge
// rdmas_memory_usage_ratio{region="extent"}        — Gauge
// rdmas_memory_usage_ratio{region="slab"}          — Gauge
rdmas_memory_usage_ratio: Gauge
    labels: ["region"]
    含义:    各区域使用率 (0.0-1.0)

// rdmas_memory_capacity_bytes{region="hash_table"}  — Gauge (常量)
rdmas_memory_capacity_bytes: IntGauge
    labels: ["region"]
    含义:    各区域总容量

// ─── 吞吐量 ────────────────────────────────────────────────
// rdmas_ops_total{op="read"}       — Counter
// rdmas_ops_total{op="write"}      — Counter
// rdmas_ops_total{op="cas"}        — Counter
rdmas_ops_total: IntCounter
    labels: ["op"]
    含义:    各操作类型累计次数

// rdmas_bytes_total{op="read"}     — Counter
// rdmas_bytes_total{op="write"}    — Counter
rdmas_bytes_total: IntCounter
    labels: ["op"]
    含义:    各操作累计传输字节数

// rdmas_cas_conflicts_total         — Counter
rdmas_cas_conflicts_total: IntCounter
    含义:    CAS 冲突累计次数

// ─── GC ────────────────────────────────────────────────────
// rdmas_gc_sweeps_total             — Counter
rdmas_gc_sweeps_total: IntCounter
    含义:    GC sweep 周期累计次数

// rdmas_gc_extents_reclaimed_total  — Counter
rdmas_gc_extents_reclaimed_total: IntCounter
    含义:    GC 累计回收 Extent 数量

// rdmas_gc_pending_count            — Gauge
rdmas_gc_pending_count: IntGauge
    含义:    当前待回收 Extent 数量

// rdmas_gc_sweep_duration_ms        — Histogram
rdmas_gc_sweep_duration_ms: Histogram
    含义:    单次 GC sweep 耗时分布 (ms)
    buckets: [1, 5, 10, 50, 100, 500, 1000, 5000]

// ─── LRU ───────────────────────────────────────────────────
// rdmas_lru_evictions_total         — Counter
rdmas_lru_evictions_total: IntCounter
    含义:    累计 LRU 淘汰条目数

// rdmas_lru_key_count               — Gauge
rdmas_lru_key_count: IntGauge
    含义:    LRU tracker 中活跃条目数

// ─── 连接 ──────────────────────────────────────────────────
// rdmas_active_qps                  — Gauge
rdmas_active_qps: IntGauge
    含义:    当前活跃的 QP 数量

// rdmas_heartbeat_success_rate      — Gauge
rdmas_heartbeat_success_rate: Gauge
    含义:    心跳成功率 (0.0-1.0)

// rdmas_generation                  — Gauge
rdmas_generation: IntGauge
    含义:    Server 当前 generation 值
```

### 6.3 Exporter 数据采集

Exporter 通过 gRPC 控制面轮询 Server 内部状态：

```rust
// 伪代码 — Exporter 采集循环
async fn collect_metrics(control_client: &ControlPlaneClient) -> Metrics {
    // 1. HealthCheck → QP 状态、活跃连接数
    let health = control_client.health_check().await?;
    //    health.qp_rts_count → rdmas_active_qps
    //    health.generation    → rdmas_generation

    // 2. GcStatus → GC 指标
    let gc = control_client.gc_status().await?;
    //    gc.sweep_count       → rdmas_gc_sweeps_total
    //    gc.pending_count     → rdmas_gc_pending_count
    //    gc.last_sweep_ms     → rdmas_gc_sweep_duration_ms

    // 3. WatermarkStatus → 内存水位
    let wm = control_client.watermark_status().await?;
    //    wm.table_load       → rdmas_memory_usage_ratio{region="hash_table"}
    //    wm.extent_usage     → rdmas_memory_usage_ratio{region="extent"}
    //    wm.slab_usage       → rdmas_memory_usage_ratio{region="slab"}

    // 4. 本地采集 (RDMA 设备状态)
    //    ibv_devinfo -d mlx5_0 解析 → rdmas_qp_state

    Metrics { /* ... */ }
}
```

### 6.4 Grafana Dashboard 面板建议

| 面板 | 类型 | 指标 | 说明 |
|------|------|------|------|
| QP Health | Stat | `rdmas_qp_state` | 绿色=RTS, 红色=ERR |
| Memory Watermark | Gauge (3行) | `rdmas_memory_usage_ratio` | hash_table/extent/slab 各一行，阈值线标注 80%/85% |
| Throughput | Graph (双Y轴) | `rate(rdmas_ops_total[1m])` + `rate(rdmas_bytes_total[1m])` | 左侧 Y: ops/s, 右侧 Y: bytes/s |
| CAS Conflict Rate | Graph | `rate(rdmas_cas_conflicts_total[5m]) / rate(rdmas_ops_total{op="cas"}[5m])` | 阈值线 10% |
| GC Efficiency | Graph + Table | `rdmas_gc_sweeps_total` + `rdmas_gc_extents_reclaimed_total` | 关联分析删除与回收速率 |
| LRU Evictions | Graph | `rate(rdmas_lru_evictions_total[5m])` | 仅在内存压力时期有值 |
| Active Connections | Stat | `rdmas_active_qps` | 监控连接数稳定性 |
| GC Sweep Latency | Heatmap | `rdmas_gc_sweep_duration_ms` | P50/P90/P99 延迟热力图 |

---

## 附录 A：常用运维命令速查

```bash
# ─── 状态检查 ───────────────────────────────────────────────
# Server 健康检查
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/HealthCheck

# QP 状态
ibv_devinfo -d mlx5_0 -v | grep -E 'state|port'

# 内存水位
curl -s http://localhost:9091/metrics | grep rdmas_memory_usage

# GC 状态
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/GcStatus

# 活跃连接
curl -s http://localhost:9091/metrics | grep rdmas_active_qps


# ─── 故障诊断 ───────────────────────────────────────────────
# QP 错误排查
ibv_devinfo -d mlx5_0 -v | grep -A5 'port:'
ethtool -S mlx5_0 | grep -E 'discard|error|drop'
dmesg | grep -i -E 'mlx5|rdma|qp.*err'

# 连接排查
show_gids
ping <server_ip> -c 5
ss -tlnp | grep 50051

# 性能排查
ib_send_bw -d mlx5_0 -g 2 --report_gbits -F
numastat -p $(pgrep rdmas-server)
ibv_devinfo -d mlx5_0 | grep mtu


# ─── 日志分析 ───────────────────────────────────────────────
# GC 回收速率统计
journalctl -u rdmas-server --since "1 hour ago" | \
  grep "GC sweep completed" | wc -l

# QP 恢复次数统计
journalctl -u rdmas-server --since "1 day ago" | \
  grep -c "recovery"

# LRU 淘汰统计
journalctl -u rdmas-server --since "1 hour ago" | \
  grep "LRU eviction" | \
  grep -oP 'evicted=\K\d+'

# CAS 冲突统计
journalctl -u rdmas-server --since "1 hour ago" | \
  grep -c "CAS conflict"


# ─── 应急操作 ───────────────────────────────────────────────
# 重启 Server
systemctl restart rdmas-server

# 清理旧日志
journalctl --vacuum-time=7d

# 手动触发 GC sweep
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/ForceGcSweep

# 手动触发 LRU 淘汰
grpcurl -plaintext localhost:50051 rdmas.control.ControlPlane/ForceLruEvict \
  -d '{"count": 100}'
```

## 附录 B：参考资源

| 资源 | 链接 |
|------|------|
| RDMAS 部署指南 | [deployment.md](deployment.md) |
| RDMAS 技术设计 | [Rust-RDMA.md](Rust-RDMA.md) |
| Extent 协议设计 | [extent-protocol.md](extent-protocol.md) |
| RDMAS Director 设计 | [RDMAS-Director协调层设计.md](RDMAS-Director协调层设计.md) |
| Prometheus 文档 | https://prometheus.io/docs |
| Grafana Dashboard 构建 | https://grafana.com/docs/grafana/latest/dashboards |
| RDMA Core 文档 | https://github.com/linux-rdma/rdma-core |
| NVIDIA OFED 文档 | https://docs.nvidia.com/networking/category/rdma |
| LMCache 集成文档 | https://github.com/LMCache/LMCache |
