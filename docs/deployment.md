# RDMAS 生产部署指南

> **适用版本**: RDMAS v0.1.0+  
> **适用场景**: 生产环境 RDMA KV Store 部署、开发测试环境搭建  
> **关联文档**: [README.md](../README.md) | [Rust-RDMA 设计方案](Rust-RDMA.md)

---

## 目录

1. [硬件要求](#1-硬件要求)
2. [操作系统要求](#2-操作系统要求)
3. [HugePages 配置](#3-hugepages-配置)
4. [RoCE 流控配置（PFC / ECN）](#4-roce-流控配置pfc--ecn)
5. [RDMA 网卡配置](#5-rdma-网卡配置)
6. [SoftRoCE 开发环境](#6-softroce-开发环境)
7. [Docker 容器化部署](#7-docker-容器化部署)
8. [快速部署检查清单](#8-快速部署检查清单)
9. [故障排查](#9-故障排查)

---

## 1. 硬件要求

### 1.1 RDMA 网卡

| 项目 | 最低要求 | 推荐配置 |
|------|---------|---------|
| **网卡型号** | Mellanox ConnectX-4 Lx (MCX4121A) | ConnectX-5/6/7 或 BlueField-2/3 DPU |
| **链路速度** | 25 Gbps | 100 Gbps（Extent 大对象场景推荐） |
| **端口数量** | 单端口 | 双端口（用于冗余绑定或控制/数据面分离） |
| **PCIe 版本** | PCIe 3.0 x8 | PCIe 4.0 x16（匹配 100/200 Gbps 线速） |

> **兼容性说明**: RDMAS 依赖 `RDMA_CAS` (Compare-and-Swap) 原子操作。部分低端 RoCEv2 网卡（如 ConnectX-3 Pro）对 CAS 支持受限或性能较差。**部署前务必验证 CAS 支持**（参见第 8 节检查清单的 Step 6）。

### 1.2 服务器要求

| 项目 | 最低要求 | 推荐配置 |
|------|---------|---------|
| **CPU 架构** | x86_64 | x86_64（支持 PCIe ATS/RO 特性以优化 RDMA 性能） |
| **CPU 核心数** | 8 核 | 16+ 核（为 busy-poll CQ 线程保留 1-2 个独占核心） |
| **内存** | 16 GB | 64 GB+（取决于 Hash 表 + Large Object Region 规划） |
| **NUMA 拓扑** | 单 socket | 双 socket（注意 NUMA 亲和性：网卡 PCIe 通道与内存绑定同一 socket） |

### 1.3 网络拓扑选择

#### 直连拓扑（推荐，适用于 2 节点部署）

```
┌──────────────┐                    ┌──────────────┐
│   Server A   │   RoCEv2 (25/100G) │   Server B   │
│  mlx5_0──────│────────────────────│──────mlx5_0  │
└──────────────┘                    └──────────────┘
```

- **优点**: 零交换机延迟（< 1μs 额外延迟），无 PFC 配置复杂度
- **缺点**: 仅限 2 节点；无交换机级流控保护
- **适用**: 小规模部署、开发/测试环境

#### 交换机拓扑（推荐，适用于 ≥3 节点或生产环境）

```
┌──────┐  ┌──────┐  ┌──────┐
│Node A│  │Node B│  │Node C│
└──┬───┘  └──┬───┘  └──┬───┘
   │         │         │
   └─────────┼─────────┘
      ┌──────┴──────┐
      │ RoCE Switch │  ← 须支持 DCB/PFC/ECN
      └─────────────┘
```

- **必须配置 PFC 和 ECN**（见第 4 节），否则丢包会导致 RDMA 性能崩塌
- 推荐交换机：NVIDIA Spectrum 系列、Cisco Nexus 9000 系列、Arista 7000 系列

---

## 2. 操作系统要求

### 2.1 支持的发行版

| 发行版 | 最低版本 | 内核版本 | 说明 |
|--------|---------|---------|------|
| **RHEL / CentOS Stream** | 8.7+ | ≥ 4.18 (需确认 RDMA 支持) | `rdma-core-devel` 由 EPEL 提供 |
| **Ubuntu** | 22.04 LTS+ | ≥ 5.15 | 官方仓库包含 `rdma-core` |
| **Fedora** | 40+ | ≥ 6.8 | `rdma-core-devel` 在默认仓库 |
| **Debian** | 12 (Bookworm)+ | ≥ 6.1 | 官方仓库包含 `rdma-core` |

> **内核版本要求**: 最低 5.15。RHEL 8.x 的 4.18 内核包含 RDMA 子系统的 backport，但强烈建议升级到 kernel-rt（实时内核）以降低 busy-poll 线程的调度抖动。

### 2.2 必需软件包

#### Fedora

```bash
sudo dnf install -y \
    rdma-core-devel \
    libibverbs-utils \
    librdmacm-utils \
    perftest \
    clang \
    glibc-headers \
    pkg-config \
    mstflint \          # Mellanox 固件工具
    numactl             # NUMA 亲和性控制
```

#### Ubuntu / Debian

```bash
sudo apt install -y \
    rdma-core \
    libibverbs-dev \
    librdmacm-dev \
    ibverbs-utils \
    rdmacm-utils \
    perftest \
    clang \
    libclang-dev \
    pkg-config \
    mstflint \
    numactl
```

#### RHEL / CentOS Stream

```bash
sudo dnf config-manager --set-enabled crb    # CentOS Stream 9/10
sudo dnf install -y epel-release
sudo dnf install -y \
    rdma-core-devel \
    libibverbs-utils \
    librdmacm-utils \
    perftest \
    clang \
    glibc-headers \
    pkg-config \
    numactl
```

### 2.3 Rust 工具链

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
# 确认版本 >= 1.75
rustc --version
# 为 release 构建添加 musl 或 gnu 目标（如需要）
rustup target add x86_64-unknown-linux-gnu
```

---

## 3. HugePages 配置

RDMAS Server 端在启动时通过 `mmap` 分配大块连续物理内存并注册到 RDMA 网卡（Memory Region, MR）。使用 HugePages 可以：

1. **减少页表条数**：2MB 页替代 4KB 页，TLB 命中率大幅提升
2. **锁定物理内存**：防止内核换出 RDMA 注册页面（换出会导致 RDMA 操作失败）
3. **提高 MR 注册速度**：减少 `ibv_reg_mr` 的页表遍历开销

### 3.1 1GB vs 2MB HugePages 选择指南

| 页面大小 | 单页大小 | 优点 | 缺点 | 适用场景 |
|---------|---------|------|------|---------|
| **2MB** | 2 MiB | 灵活分配，热添加无需重启 | TLB 覆盖范围较小 | 通用部署，Hash 表 < 1GB |
| **1GB** | 1 GiB | 单页覆盖极大内存区域，TLB 效率最高 | 需在内核启动参数中预留；无法灵活回收 | Large Object Region ≥ 4GB |

**RDMAS 推荐策略**:
- **Hash Table Region**（通常 64MB-256MB）：使用 2MB HugePages，通过 `/proc/sys/vm/nr_hugepages` 动态分配
- **Large Object Region**（≥ 1GB，如 LMCache KV cache 场景）：使用 1GB HugePages，通过内核启动参数 `default_hugepagesz=1G hugepagesz=1G hugepages=4` 预留

### 3.2 计算所需 HugePages 数量

RDMAS 内存区域规划公式：

```
总内存需求 = Hash Table Region + Large Object Region + Free List Region
           = (桶数 × 64B) + (大对象池大小) + (空闲链表区 ~10-50MB)

2MB 页数 = ⌈总内存需求 / 2MB⌉
1GB 页数 = ⌈Large Object Region / 1GB⌉（如有）
```

**示例计算**（1M 桶哈希表 + 4GB Large Object Region）：

```
Hash Table Region  = 1,048,576 × 64B = 64 MB
Large Object Region = 4 GB
Free List Region    = 20 MB（估算）
─────────────────────────────────────
总计                ≈ 4.1 GB

2MB HugePages = ⌈4.1 GB / 2 MB⌉ = ⌈2100⌉ = 2100 页
```

**推荐安全系数**: 在计算结果基础上额外增加 10-20%，为运行时碎片化和未来扩展留余量。

### 3.3 持久化配置

#### 2MB HugePages — `/etc/sysctl.conf`

```bash
# 编辑 /etc/sysctl.conf
sudo tee -a /etc/sysctl.conf <<'EOF'
# RDMAS HugePages 配置
vm.nr_hugepages = 4096
vm.hugetlb_shm_group = 0
EOF

# 立即生效
sudo sysctl -p

# 创建挂载点（多数发行版已自动挂载）
sudo mkdir -p /dev/hugepages
sudo mount -t hugetlbfs -o pagesize=2M none /dev/hugepages
```

#### 1GB HugePages — `/etc/default/grub`

```bash
# 编辑 /etc/default/grub，在 GRUB_CMDLINE_LINUX 中添加
GRUB_CMDLINE_LINUX="... default_hugepagesz=1G hugepagesz=1G hugepages=4"

# 重新生成 GRUB 配置
# RHEL/Fedora:
sudo grub2-mkconfig -o /boot/grub2/grub.cfg
# Ubuntu/Debian:
sudo update-grub

# 重启生效
sudo reboot
```

#### 挂载 1GB HugePages

```bash
sudo mkdir -p /dev/hugepages1G
sudo mount -t hugetlbfs -o pagesize=1G none /dev/hugepages1G
# 持久化写入 /etc/fstab
echo 'none /dev/hugepages1G hugetlbfs pagesize=1G 0 0' | sudo tee -a /etc/fstab
```

### 3.4 `memlock` ulimit 配置

RDMA 内存注册需要锁定物理内存，必须提高进程的 `memlock` 限制。

```bash
# /etc/security/limits.conf
sudo tee -a /etc/security/limits.conf <<'EOF'
# RDMAS — 允许锁定 RDMA 注册内存
*       soft    memlock     unlimited
*       hard    memlock     unlimited
EOF

# 对于 systemd 管理的服务，还需在 service 文件中设置
# /etc/systemd/system/rdmas-server.service:
# [Service]
# LimitMEMLOCK=infinity

# 验证（重新登录后）
ulimit -l
# 预期输出: unlimited
```

### 3.5 验证命令和预期输出

```bash
# 1. 检查 HugePages 总览
grep -E 'HugePages|Hugepagesize' /proc/meminfo

# 预期输出示例 (4096 页 × 2MB):
# HugePages_Total:    4096
# HugePages_Free:     4096
# HugePages_Rsvd:        0
# HugePages_Surp:        0
# Hugepagesize:       2048 kB

# 2. 检查 1GB 大页（如已配置）
ls /dev/hugepages1G/
grep Hugepagesize /proc/meminfo

# 3. 验证挂载点
mount | grep huge
# 预期输出:
# hugetlbfs on /dev/hugepages type hugetlbfs (rw,relatime,pagesize=2M)
# hugetlbfs on /dev/hugepages1G type hugetlbfs (rw,relatime,pagesize=1G)

# 4. 测试 mmap 大页
cat > /tmp/test_hugepage.c <<'EOF'
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
int main() {
    void *p = mmap(NULL, 2*1024*1024, PROT_READ|PROT_WRITE,
                   MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB, -1, 0);
    if (p == MAP_FAILED) { perror("mmap hugepage failed"); return 1; }
    printf("HugePage mmap OK: %p\n", p);
    munmap(p, 2*1024*1024);
    return 0;
}
EOF
gcc -o /tmp/test_hugepage /tmp/test_hugepage.c && /tmp/test_hugepage && rm /tmp/test_hugepage*
```

---

## 4. RoCE 流控配置（PFC / ECN）

### 4.1 为什么 RDMA 需要无损网络

RDMA over Converged Ethernet (RoCEv2) 使用 UDP 封装 InfiniBand 传输层。在 RDMA 中：

1. **不支持丢包重传**：RDMA 的 `RDMA_READ`/`WRITE`/`CAS` 操作依赖硬件可靠性。丢包导致 QP 进入错误状态，恢复代价极高（需要销毁并重建 QP）。
2. **RoCEv2 无 InfiniBand 信用流控**：InfiniBand 在链路层通过信用机制（Credit-Based Flow Control）保证零丢包；RoCEv2 替代为以太网拥塞控制协议。
3. **PFC 是 RoCEv2 的基石**：Priority Flow Control (PFC, IEEE 802.1Qbb) 在以太网层实现逐优先级的暂停帧，是构建无损网络的核心。

### 4.2 无损网络三层架构

```
┌──────────────────────────────────────────────┐
│  第三层：ECN (Explicit Congestion Notification) │  ← 端到端拥塞通知
│          触发发送端降速（DCQCN 算法）              │
├──────────────────────────────────────────────┤
│  第二层：PFC (Priority Flow Control)           │  ← 逐跳暂停帧，防止丢包
│          IEEE 802.1Qbb, 每优先级独立暂停        │
├──────────────────────────────────────────────┤
│  第一层：Wred/ECN 阈值 + Buffer 管理            │  ← 交换机缓冲区提前预警
└──────────────────────────────────────────────┘
```

**工作流程**:
1. 交换机 buffer 使用率达到 WRED ECN 阈值 → 交换机标记 ECN 位
2. 接收端收到 ECN 标记 → 发送 CNP (Congestion Notification Packet) 给发送端
3. 发送端收到 CNP → DCQCN 算法降低发送速率
4. 若 buffer 继续增长 → 达到 PFC 阈值 → 交换机发 PFC 暂停帧 → **不丢包停下游**

### 4.3 交换机 PFC 配置

#### PFC 基本原理

- PFC 在 802.1p CoS（Class of Service）级别工作，共 8 个优先级（0-7）
- RDMA 流量通常映射到 CoS 3 或 CoS 5
- 只有 RDMA 流量的优先级启用 PFC；其他流量（如管理网、存储网）不应启用，避免"暂停风暴"扩散
- 必须为 PFC 优先级分配**独立的无损缓冲区**（Headroom Buffer）

#### Cisco Nexus 9000 系列配置示例

```cisco
! 启用 QoS
feature qos

! 定义 RDMA 流量的 class-map
class-map type qos match-all RDMA-CLASS
  match dscp 26                    ! AF31, 对应 RDMA 流量
  ! 或 match cos 3

! 定义网络 QoS 策略
policy-map type qos RDMA-POLICY
  class RDMA-CLASS
    set qos-group 3

! 定义网络 QoS 策略映射
system qos
  service-policy type qos input RDMA-POLICY

! 启用 PFC（CoS 3）
priority-flow-control mode on
! 或针对特定接口:
interface Ethernet1/1-1/32
  priority-flow-control mode on

! 配置无损缓冲区和 PFC 阈值（示例值）
! Headroom buffer: 容纳一个 BDP (Bandwidth-Delay Product) 的数据量
! BDP = 100Gbps × 传输延迟 / 8 ≈ 100Gbps × 2μs / 8 ≈ 25KB
! 建议 Headroom: BDP × 安全系数 × MTU
pfc-priority 3
  pause-threshold 31250       ! 约 25KB，超过此值发 PFC 暂停
  resume-threshold 15625      ! 约 12.5KB，低于此值恢复
  headroom-buffer-size 50000  ! 约 40KB Headroom
```

#### NVIDIA (Mellanox) Spectrum 交换机配置示例

```bash
# 启用 PFC
mlxconfig -d /dev/mst/<device> set PFC_ENABLE=1

# 或通过 Cumulus Linux / SONiC CLI
# 在接口上启用 PFC 并设置阈值
sudo pfc config set --priority 3 --enable eth0
sudo pfc config set --priority 3 --xoff-threshold 31250 eth0
sudo pfc config set --priority 3 --xon-threshold 15625 eth0

# 设置 buffer 池大小
sudo mmu config set --ingress-lossy-buffer 0 --priority 3
sudo mmu config set --ingress-lossless-buffer 20000000  # 20MB, 依交换机总 buffer 而定

# 为 RoCEv2 配置 ECN
sudo ecn config set --enable --min 65536 --max 262144 --probability 10 eth0
```

#### 关键参数说明

| 参数 | 含义 | 推荐值 | 计算依据 |
|------|------|--------|---------|
| **PFC XOFF Threshold** | Buffer 使用量超过此值 → 发 PFC 暂停帧 | 25-50 KB | 需能容纳暂停帧到达之前仍在飞行中的数据 (Headroom) |
| **PFC XON Threshold** | Buffer 使用量降到低于此值 → 发恢复帧 | XOFF 的 50% | 避免频繁振荡（PFC 风暴） |
| **Headroom Buffer** | 为 PFC 优先级保留的专用缓冲 | BDP × 2 | `BDP = 链路带宽 × 最大一跳延迟` |
| **PFC 暂停时间** | 暂停帧中的暂停时间 (quanta) | 65535 (最大) | 每 quantum = 512 bit 时间；65535 ≈ 335μs @ 100Gbps |
| **ECN min threshold** | 开始标记 ECN 的 buffer 阈值 | 64 KB | 低于 PFC XOFF，提前预警 |
| **ECN max threshold** | 开始丢弃的 buffer 阈值 | 256 KB | 接近但不触发丢包的 Buffer 上限 |

### 4.4 WRED / ECN 阈值建议

ECN (Explicit Congestion Notification, RFC 3168) 是 PFC 的前序保障：在 PFC 暂停触发之前，通过拥塞通知让发送端主动降速，避免不必要的 PFC 暂停。

```
Buffer 使用量
─────────────────────────────────────────────
  0%  ──────────────────────────────────── 正常转发
       │
       ▼
  ECN Min (≈40% PFC XOFF)
       ├──────────────────────────────── 标记 ECN → 发送端降速
       │
       ▼
  ECN Max (≈90% PFC XOFF)
       ├──────────────────────────────── 开始尾丢弃（保底）
       │
       ▼
  PFC XOFF Threshold
       └──────────────────────────────── 发 PFC 暂停帧（最后防线）
─────────────────────────────────────────────
 100%
```

**推荐阈值**（基于交换机总 buffer 百分比）：

| 阈值 | 推荐占比 | 100Gbps 典型值 |
|------|---------|---------------|
| ECN min | 5-10% 总 buffer | 65,536 字节 |
| ECN max | 15-25% 总 buffer | 262,144 字节 |
| ECN 标记概率 | — | 10%（初始），最大 100% |
| PFC XOFF | 20-30% 总 buffer | 350,000 字节 |
| PFC XON | XOFF 的 50% | 175,000 字节 |

### 4.5 验证无损网络

#### 使用 `ibv_rc_pingpong` 测试

```bash
# 服务端（Node A）
ibv_rc_pingpong -d mlx5_0 -g 0 -s 65536 -r 1000000

# 客户端（Node B）
ibv_rc_pingpong -d mlx5_0 -g 0 -s 65536 -r 1000000 <Server_IP>

# 预期输出:
#  local address:  LID 0x0000, QPN 0x00004a, PSN 0xabcdef
#  remote address: LID 0x0000, QPN 0x00004b, PSN 0xfedcba
#  65536000000 bytes in 1.23 seconds = 426.34 Gb/s
#  1000000 iterations, 0 failures
#
# 关键指标: failures 必须为 0
```

#### 排查 PFC 触发次数

```bash
# NVIDIA (Mellanox) 网卡
ethtool -S mlx5_0 | grep -i pfc
# rx_prio3_pause: 0           ← PFC 暂停帧接收数（应为 0 或极低）
# tx_prio3_pause: 0           ← 发送的暂停帧数
# rx_prio3_pause_duration: 0  ← 累计暂停时间

# 交换机侧（NVIDIA Spectrum）
# 查看接口 PFC 统计
show interface eth0 counters | grep pfc
```

### 4.6 常见问题排查

| 现象 | 可能原因 | 排查步骤 |
|------|---------|---------|
| `ibv_rc_pingpong` 报 `failure` | 丢包导致 QP 错误 | 1. 确认 PFC 已启用；2. 确认交换机所有跳都配置了 PFC；3. 检查 buffer 大小是否足够 |
| PFC 暂停帧计数持续增长 | 网络拥塞严重 | 1. 降低发送速率；2. 增大 ECN 标记激进程度；3. 检查是否有流量冲突 |
| PFC 风暴（暂停帧在整个网络扩散） | PFC 配置不当或交换机级联 | 1. 限制 PFC 仅对 RDMA CoS 启用；2. 检查下游交换机 buffer |
| 吞吐远低于线速 | ECN 配置过于激进 | 1. 增大 ECN min threshold；2. 降低 ECN 标记概率；3. 检查 DCQCN 参数 |

---

## 5. RDMA 网卡配置

### 5.1 固件升级（Mellanox MFT 工具）

```bash
# 安装 MFT（Mellanox Firmware Tools）
# 从 NVIDIA 官网下载: https://network.nvidia.com/products/adapter-software/firmware-tools/

# 或通过包管理器安装
# Fedora:
sudo dnf install -y mstflint
# Ubuntu:
sudo apt install -y mstflint

# 检查当前固件版本
sudo mst start
sudo mst status -v
mlxfwmanager --query

# 查看可用固件
mlxfwmanager --online-query

# 升级固件（示例：ConnectX-5）
# 下载 .bin 固件文件后:
sudo mlxfwmanager -i <firmware_file>.bin -d /dev/mst/mt4119_pciconf0 -f

# 重启网卡（软重置，不重启服务器）
sudo mlxfwreset -d /dev/mst/mt4119_pciconf0 reset

# 验证
mlxfwmanager --query
```

### 5.2 `ibv_devinfo` 输出解读

```bash
ibv_devinfo -d mlx5_0 -v
```

**关键输出段解读**：

```
hca_id: mlx5_0                          # HCA 标识符（应用层引用此名称）
    transport:                      InfiniBand (0)   # 传输层: IB=0, iWARP=2, RoCE=未标识
    fw_ver:                         16.35.3006      # 固件版本
    node_guid:                      506b:4b03:00a7:3c8e  # 全局唯一节点标识
    sys_image_guid:                 506b:4b03:00a7:3c8e
    vendor_id:                      0x02c9           # 0x02c9 = Mellanox
    vendor_part_id:                 4121              # 4121 = ConnectX-4 Lx
    hw_ver:                         0x0
    board_id:                       MCX4121A-ACAT
    phys_port_cnt:                  1                # 物理端口数
        port:   1
            state:                  PORT_ACTIVE (4)  # 端口状态: ACTIVE = 链路已建立
            max_mtu:                4096 (5)         # 最大 MTU
            active_mtu:             4096 (5)         # 当前 MTU（4096 = RoCE "巨帧"）
            sm_lid:                 0                # Subnet Manager LID (RoCE = 0)
            port_lid:               0                # Local ID (RoCE = 0)
            lid_mask_count:         0
            max_msg_sz:             0x40000000       # 最大消息大小 (1GB)
            max_mr_size:            0xFFFFFFFFFFFF   # 最大 MR 大小
            link_layer:             Ethernet         # 链路层: Ethernet = RoCEv2 模式
```

**重点检查项**：
- `state: PORT_ACTIVE` — 链路已建立且正常
- `link_layer: Ethernet` — 确认为 RoCEv2 模式（不是 InfiniBand）
- `active_mtu: 4096` — RDMA 需要使用巨帧；若为 1514 则有性能损失
- `max_mr_size: 0xFFFFFFFFFFFF` — MR 大小上限足够

### 5.3 RoCEv2 模式确认

```bash
# 方法 1: 查看链路层类型
ibv_devinfo -d mlx5_0 | grep link_layer
# 期望: link_layer: Ethernet

# 方法 2: 检查 RoCE 模式
cat /sys/class/infiniband/mlx5_0/ports/1/gid_attrs/types/0
# 输出: RoCE v2   (如果是 "IB/RoCE v1"，需要配置为 RoCEv2)

# 方法 3: 配置 RoCEv2（如需要）
# 设置 DCB (Data Center Bridging) 优先级
sudo cma_roce_mode -d mlx5_0 -p 1 -m 2   # -m 2 = RoCEv2
```

### 5.4 GID 表检查和选择

RoCEv2 使用 GID (Global Identifier) 进行寻址，GID 表包含网卡的所有可用地址。

```bash
# 查看所有 GID
ibv_devinfo -d mlx5_0 -v | grep -A2 GID

# 或使用 show_gids
show_gids
# 输出示例:
# DEV     PORT    INDEX   GID                                      IPv4            VER     DEV
# mlx5_0  1       0       fe80:0000:0000:0000:506b:4b03:00a7:3c8e                 v1      enp1s0f0
# mlx5_0  1       1       fe80:0000:0000:0000:506b:4b03:00a7:3c8e                 v2      enp1s0f0
# mlx5_0  1       2       0000:0000:0000:0000:0000:ffff:0a00:0001 10.0.0.1        v1      enp1s0f0
# mlx5_0  1       3       0000:0000:0000:0000:0000:ffff:0a00:0001 10.0.0.1        v2      enp1s0f0

# GID 选择规则:
#   Index 0/1 = 链路本地地址（不可跨子网路由）
#   Index 2/3 = IPv4-mapped RoCEv2 GID（可跨子网路由）
#
# 推荐使用 IPv4-mapped GID (Index 2 或 3)
# 在 RDMAS 配置中指定: --device mlx5_0 --gid-index 2
```

### 5.5 多端口绑定建议

对于多端口网卡，有两种使用策略：

| 策略 | 实现 | 优点 | 缺点 |
|------|------|------|------|
| **独立端口** | 每个端口分配独立 QP + 不同 GID | 隔离控制面和数据面流量；单个端口故障不影响全部 | 带宽不聚合 |
| **端口绑定 (LAG)** | 交换机 LACP + 网卡 RoCE LAG | 带宽聚合（2 × 100Gbps） | 需要交换机支持；配置复杂度高 |

**推荐方案**（双端口 ConnectX-5）：
```
端口 1 (mlx5_0) → 数据面 RoCEv2 流量（RDMA READ/WRITE/CAS）
端口 2 (mlx5_1) → 控制面 TCP/gRPC 流量（心跳、MR 元数据分发）
```

---

## 6. SoftRoCE 开发环境

SoftRoCE (RXE) 是 Linux 内核提供的软件 RDMA 实现。**仅用于开发和功能验证，严禁用于性能评估。**

### 6.1 内核模块加载

```bash
# 加载 SoftRoCE 内核模块
sudo modprobe rdma_rxe

# 确认模块已加载
lsmod | grep rdma_rxe
# 预期: rdma_rxe              <size>  0

# 设置开机自动加载
echo 'rdma_rxe' | sudo tee /etc/modules-load.d/rdma_rxe.conf
```

### 6.2 创建 SoftRoCE 设备

```bash
# 列出可用网口
ip link show

# 在 eth0 上创建 SoftRoCE 设备
sudo rdma link add rxe0 type rxe netdev eth0

# 验证设备
ibv_devices
# 预期输出:
#     device                 node GUID
#     ------              ----------------
#     rxe0                0a0027fffe123456

rdma link show
# 预期输出:
# link rxe0/1 state ACTIVE physical_state LINK_UP netdev eth0
```

**多设备创建**（多节点测试）：

```bash
# Node A
sudo rdma link add rxe0 type rxe netdev eth0

# Node B
sudo rdma link add rxe0 type rxe netdev eth0
# 或使用不同名称以避免混淆
sudo rdma link add rxe1 type rxe netdev eth1
```

### 6.3 限制和注意事项

| 限制 | 说明 |
|------|------|
| **性能** | SoftRoCE 延迟为硬件 RDMA 的 10-50 倍；吞吐受限于 CPU 和内核调度。**不能用于性能基准测试** |
| **CAS 支持** | SoftRoCE 支持 RDMA_CAS，但语义可能与硬件略有差异。务必在真实硬件上验证 |
| **并发能力** | 内核 RXE 驱动使用全局锁，高并发下吞吐下降严重（< 1M OPS） |
| **HugePages** | SoftRoCE 仍需要 HugePages 进行 MR 注册 |
| **PFC / ECN** | SoftRoCE 不需要交换机配置（走内核 TCP/IP 栈），但真实 RoCE 必须配置 |
| **适用场景** | 功能测试、CI/CD 流水线、API 集成测试、demo 展示 |

---

## 7. Docker 容器化部署

### 7.1 构建镜像

项目根目录提供了 `Dockerfile`：

```bash
# 构建镜像
docker build -t rdmas:latest .

# 查看构建产物
docker images rdmas
```

### 7.2 最小可运行命令

```bash
# Server 端运行
docker run --rm \
    --privileged \
    --network=host \
    --device=/dev/infiniband/uverbs0 \
    --device=/dev/infiniband/rdma_cm \
    -v /dev/hugepages:/dev/hugepages \
    -e RDMA_DEVICE_NAME=mlx5_0 \
    rdmas:latest \
    rdmas-server --device mlx5_0 --gid-index 2 --port 9400
```

**必需参数详解**：

| 参数 | 必需 | 说明 |
|------|------|------|
| `--privileged` | ✅ | 允许容器内执行 `mmap(MAP_HUGETLB)` 和 RDMA verbs 操作 |
| `--network=host` | ✅ | RDMA 是内核旁路（kernel bypass），绕过容器网络栈；必须使用 host 网络模式 |
| `--device=/dev/infiniband/uverbs0` | ✅ | 暴露 RDMA 用户态设备（verbs 设备） |
| `--device=/dev/infiniband/rdma_cm` | ✅ (连接管理) | 暴露 RDMA CM (Connection Manager) 设备，用于 QP 建立 |
| `-v /dev/hugepages:/dev/hugepages` | ✅ | 挂载 HugePages 文件系统，用于 MR 注册 |

### 7.3 GPU 设备传递（GPUDirect RDMA）

当 RDMAS 作为 LMCache L2 后端时，可能需要 GPUDirect RDMA（GPU 显存 <-> RDMA 网卡直通，绕过 CPU 内存拷贝）。

```bash
docker run --rm \
    --privileged \
    --network=host \
    --device=/dev/infiniband/uverbs0 \
    --device=/dev/infiniband/rdma_cm \
    --gpus all \                               # 传递所有 GPU
    -v /dev/hugepages:/dev/hugepages \
    --ulimit memlock=-1:-1 \                   # memlock unlimited
    --ipc=host \                               # GPU 共享内存
    rdmas:latest \
    rdmas-server --device mlx5_0 --gid-index 2 --gpudirect
```

**GPUDirect RDMA 前置条件**：
1. GPU 架构 ≥ Kepler (NVIDIA) 或 MI200+ (AMD)
2. GPU 和 RDMA 网卡在同一 PCIe root complex 下（NUMA 亲和）
3. `nvidia-peermem` 内核模块已加载：`sudo modprobe nvidia-peermem`
4. 启用 `--gpudirect` 标志

### 7.4 `docker-compose.yml` 多节点示例

```yaml
# docker-compose.yml — RDMAS 多节点 SoftRoCE 集成测试
# 用于 CI/CD 或本地功能验证（非性能测试）

version: "3.8"

services:
  rdmas-server:
    image: rdmas:latest
    container_name: rdmas-server
    privileged: true
    network_mode: host
    devices:
      - /dev/infiniband/uverbs0
      - /dev/infiniband/rdma_cm
    volumes:
      - /dev/hugepages:/dev/hugepages
      - ./config/server.toml:/app/config/server.toml:ro
    environment:
      - RDMA_DEVICE_NAME=rxe0
      - RUST_LOG=info,rdmas=debug
    command:
      - rdmas-server
      - --config
      - /app/config/server.toml
    ulimits:
      memlock: -1
    restart: unless-stopped

  rdmas-client:
    image: rdmas:latest
    container_name: rdmas-client
    privileged: true
    network_mode: host
    devices:
      - /dev/infiniband/uverbs0
      - /dev/infiniband/rdma_cm
    volumes:
      - /dev/hugepages:/dev/hugepages
      - ./config/client.toml:/app/config/client.toml:ro
    environment:
      - RDMA_DEVICE_NAME=rxe0
      - RUST_LOG=info,rdmas=debug
    command:
      - rdmas-bench
      - --server
      - "10.0.0.1:9400"
      - --operations
      - "1000000"
    ulimits:
      memlock: -1
    depends_on:
      - rdmas-server
```

> **注意**：以上示例使用 SoftRoCE 设备名称 `rxe0`，适用于本地测试。生产环境应替换为 `mlx5_0`（Mellanox 网卡）。多物理节点部署时，在每个节点分别运行对应的 service。

### 7.5 Kubernetes RDMA 设备插件简介

对于 Kubernetes 集群部署，需要使用 RDMA 设备插件将 HCA 资源暴露给 Pod。

**NVIDIA Network Operator**（推荐）：

```bash
# 安装 Network Operator (Helm)
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia
helm install network-operator nvidia/network-operator \
    --namespace nvidia-network-operator \
    --create-namespace \
    --set rdmaSharedDevicePlugin.enabled=true \
    --set secondaryNetwork.enabled=true
```

**Pod 请求 RDMA 资源**：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: rdmas-server
spec:
  containers:
  - name: rdmas
    image: rdmas:latest
    securityContext:
      privileged: true
    resources:
      limits:
        rdma/rdma_shared_device_mlx5: 1   # 请求 1 个 RDMA HCA
    volumeMounts:
    - name: hugepages
      mountPath: /dev/hugepages
  volumes:
  - name: hugepages
    hostPath:
      path: /dev/hugepages
      type: Directory
```

**K8s 部署注意事项**：
1. Pod 必须使用 `hostNetwork: true`（RDMA 内核旁路）
2. `securityContext.privileged: true` 是必需的
3. HugePages 必须通过 `hostPath` 挂载
4. 推荐使用 NodeSelector 或 NodeAffinity 将 RDMA Pod 调度到有 RDMA 网卡的节点

---

## 8. 快速部署检查清单

从裸机到首次成功 RDMA 读写，按顺序执行以下步骤。每步完成后打勾确认。

---

### Step 1: 硬件验证

- [ ] 确认服务器已安装 Mellanox ConnectX-4 Lx/5/6 或更高型号网卡
- [ ] 确认网卡物理连接到交换机或对端服务器，端口指示灯正常
- [ ] 运行 `lspci | grep Mellanox` 确认网卡被系统识别

### Step 2: 操作系统准备

```bash
# [ ] 确认内核版本 >= 5.15
uname -r

# [ ] 安装 RDMA 依赖
# Fedora:
sudo dnf install -y rdma-core-devel libibverbs-utils perftest clang glibc-headers numactl
# Ubuntu:
sudo apt install -y rdma-core libibverbs-dev ibverbs-utils perftest clang libclang-dev pkg-config numactl

# [ ] 安装 Rust 工具链 (>= 1.75)
rustc --version
# 若无: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Step 3: 网卡固件检查

```bash
# [ ] 启动 MST 并查询固件
sudo mst start
mlxfwmanager --query

# [ ] 确认固件版本 >= 16.35.xxxx (ConnectX-5) 或 >= 14.32.xxxx (ConnectX-4 Lx)
# 如有必要，升级固件（见第 5.1 节）
```

### Step 4: HugePages 配置

```bash
# [ ] 计算所需 HugePages（见第 3.2 节）
# [ ] 配置 2MB HugePages
echo 4096 | sudo tee /proc/sys/vm/nr_hugepages

# [ ] 持久化写入 /etc/sysctl.conf
echo 'vm.nr_hugepages = 4096' | sudo tee -a /etc/sysctl.conf

# [ ] 验证
grep HugePages_Total /proc/meminfo
# 期望: HugePages_Total:    4096
```

### Step 5: memlock 配置

```bash
# [ ] 配置 limits.conf
echo '* soft memlock unlimited' | sudo tee -a /etc/security/limits.conf
echo '* hard memlock unlimited' | sudo tee -a /etc/security/limits.conf

# [ ] 重新登录后验证
ulimit -l
# 期望: unlimited
```

### Step 6: RDMA 连通性验证

```bash
# [ ] 确认 RDMA 设备可见
ibv_devices
# 期望: 列出 mlx5_0（或 rxe0 SoftRoCE）

# [ ] 确认设备详情
ibv_devinfo -d mlx5_0
# [ ] 检查: state=PORT_ACTIVE, link_layer=Ethernet, active_mtu=4096

# [ ] 双机 pingpong 测试（服务端 + 客户端分别执行）
# Server:
ibv_rc_pingpong -d mlx5_0 -g 2 -s 65536
# Client:
ibv_rc_pingpong -d mlx5_0 -g 2 -s 65536 <Server_IP>

# [ ] 验证: 0 failures，吞吐接近线速

# [ ] CAS 原子操作验证
ibv_atomic_bw -d mlx5_0 -g 2
# Client:
ibv_atomic_bw -d mlx5_0 -g 2 <Server_IP> --atomic_type=extended_cmp_swp
# [ ] 验证 CAS 操作成功完成
```

### Step 7: PFC / ECN 配置（交换机环境）

> 如果是直连拓扑，跳过此步骤。

```bash
# [ ] 在交换机上启用 PFC（CoS 3 或 CoS 5）
# [ ] 配置无损 Buffer 和 PFC 阈值
# [ ] 配置 ECN 阈值（min=64KB, max=256KB）
# [ ] 两侧网卡确认 PFC 计数器初始为 0
ethtool -S mlx5_0 | grep -i 'rx_prio.*pause'

# [ ] 再次运行 pingpong 测试，确认 failures=0
```

### Step 8: 编译 RDMAS

```bash
# [ ] 克隆仓库
git clone https://github.com/ipconfiger/rdmas.git
cd rdmas

# [ ] 编译
cargo build --release

# [ ] 运行测试（SoftRoCE 环境下或跳过 RDMA 相关测试）
cargo test --release
# 期望: 全部通过（或 RDMA 相关测试标记为 ignored）
```

### Step 9: 启动 Server

```bash
# [ ] Server 节点执行
./target/release/rdmas-server \
    --device mlx5_0 \
    --gid-index 2 \
    --port 9400 \
    --buckets 1048576 \
    --large-obj-size 4294967296

# [ ] 检查日志确认:
#     - HugePages mmap 成功
#     - MR 注册成功 (rkey + vaddr)
#     - gRPC 控制面监听端口 9400
```

### Step 10: 首次读写验证

```bash
# [ ] Client 节点执行（或同节点另一个终端）
./target/release/rdmas-client \
    --server <Server_IP>:9400 \
    --device mlx5_0 \
    --gid-index 2 \
    put --key "hello" --value "rdmas-works"

# [ ] 读回验证
./target/release/rdmas-client \
    --server <Server_IP>:9400 \
    --device mlx5_0 \
    --gid-index 2 \
    get --key "hello"
# 期望输出: "rdmas-works"
```

---

**全部 10 步通过后，RDMAS 部署完成！** 🎉

---

## 9. 故障排查

### 9.1 `ibv_devinfo` 看不到设备

| 症状 | 诊断命令 | 解决方案 |
|------|---------|---------|
| 无 RDMA 设备列出 | `ibv_devices` 空输出 | 确认网卡驱动已加载：`lsmod \| grep mlx5`；若无，`sudo modprobe mlx5_ib` |
| 设备存在但状态为 `PORT_DOWN` | `ibv_devinfo -d mlx5_0 \| grep state` | 1. 检查物理线缆；2. 确认对端端口已启用；3. `ip link set <iface> up` |
| SoftRoCE 设备不存在 | `rdma link show` 空 | `sudo modprobe rdma_rxe` → `sudo rdma link add rxe0 type rxe netdev eth0` |

### 9.2 `mmap` HugePages 失败

| 症状 | 诊断命令 | 解决方案 |
|------|---------|---------|
| Server 启动报错 "Cannot mmap hugepages" | `grep HugePages_Free /proc/meminfo` | 1. 确认 `nr_hugepages` >= 所需数量；2. 确认 `/dev/hugepages` 已挂载 |
| `HugePages_Free = 0` | `cat /proc/sys/vm/nr_hugepages` | 增大 `nr_hugepages` 值，可能需要重启释放被占用的页 |
| `mmap` 返回 `ENOMEM` | `grep Hugepagesize /proc/meminfo` | 确认页面大小与 `mmap` 请求的 `length` 参数对齐 |
| `ulimit -l` 显示非 `unlimited` | `ulimit -l` | 检查 `/etc/security/limits.conf` 的 `memlock` 条目；重新登录生效 |
| systemd 服务中 memlock 受限 | `systemctl show rdmas-server \| grep LimitMEMLOCK` | 在 `.service` 文件中添加 `LimitMEMLOCK=infinity`，然后 `sudo systemctl daemon-reload` |

### 9.3 RDMA 连接超时

| 症状 | 诊断命令 | 解决方案 |
|------|---------|---------|
| `rdma_connect` 超时 | 检查防火墙规则：`sudo firewall-cmd --list-all` | RDMA CM 使用 TCP 端口同步（默认随机端口）。放行控制面 gRPC 端口（如 9400）并确保 RDMA_CM 端口可达 |
| GID 不匹配 | `show_gids` 对比两端 GID | 确认两端使用相同 GID Index；确认 IP 在同一子网（跨子网需配置路由） |
| QP 状态卡在 INIT | `ibv_rc_pingpong` 一端无输出 | 1. 确认服务端先启动；2. 检查 IP 地址是否正确；3. 检查子网掩码 |
| ARP 表缺失 | `ip neigh show dev <iface>` | 手动添加：`sudo ip neigh add <remote_IP> lladdr <remote_MAC> dev <iface>` |

### 9.4 QP 状态异常

RDMA QP 状态机：`RESET → INIT → RTR → RTS → (SQE/Error) → (RESET)`

```bash
# 查看 QP 状态
ibv_rc_pingpong -d mlx5_0 -g 2 2>&1 | grep "QP state"
```

| 状态异常 | 含义 | 原因 | 解决方案 |
|---------|------|------|---------|
| QP stuck at INIT | 未完成到 RTR 的迁移 | 远端 QP 参数未就绪 | 确认 `rdma_connect` 的 `qp_num` 和 `qkey` 正确传递 |
| QP stuck at RTR | 未完成到 RTS 的迁移 | 远端 QP 未进入 RTR | 确认双方 `modify_qp` 调用顺序正确：**被动方先进入 RTR，主动方才进入 RTS** |
| QP in ERR state | QP 进入错误状态 | 丢包、MTU 不匹配、远端 QP 销毁 | 1. 检查 PFC 配置；2. 确认 MTU 匹配；3. 销毁并重建 QP |
| Completion with error | CQ 轮询到错误完成 | 远端访问非法地址/非法权限 | 1. 确认 MR rkey 未过期（Server 重启后 rkey 失效）；2. 确认 generation_id 匹配；3. 检查访问偏移量未超出 MR 范围 |

### 9.5 性能不达预期

| 症状 | 诊断命令 | 可能原因 | 解决方案 |
|------|---------|---------|---------|
| 吞吐 < 50% 线速 | `ethtool -S mlx5_0 \| grep -i discard` | 丢包导致 QP 重建 | 排查 PFC/ECN；检查交换机 CRC 错误计数 |
| P99 延迟抖动 > 100μs | `perf sched record -a sleep 5` | busy-poll 线程被内核调度出去 | 1. 绑核：`taskset -c <core_id>`；2. 隔离核心：`isolcpus=<core_range>` 内核参数 |
| CPU 利用率异常高 | `top -H -p $(pgrep rdmas-server)` | Server 触碰到数据面内存（缓存一致性开销） | 确认 Server 代码路径不读写数据面内存 |
| 写性能下降，读正常 | 检查 Cuckoo 踢出链长度 (日志) | 哈希表负载因子过高 | 增大桶数量；降低 `MAX_KICK` 阈值 |
| NUMA 不均衡 | `numastat -p $(pgrep rdmas-server)` | 网卡 PCIe 通道在 socket 0，内存分配在 socket 1 | 使用 `numactl --membind=0 --cpunodebind=0` 启动 Server |
| MTU 不匹配 | `ibv_devinfo -d mlx5_0 \| grep mtu` | 巨帧未启用 | 设置 MTU：`sudo ip link set <iface> mtu 4200`；交换机侧同步修改 |

---

## 附录 A：常用诊断命令速查

```bash
# RDMA 设备信息
ibv_devices                          # 列出所有 RDMA 设备
ibv_devinfo -d mlx5_0 -v             # 详细设备信息
ibstat mlx5_0                        # 设备状态（含链路速率）
show_gids                            # GID 表

# 网络统计
ethtool -S mlx5_0 | grep -E 'pause|drop|error|discard'  # 网卡统计
ethtool mlx5_0                       # 链路协商速率
perftest/ib_send_bw -d mlx5_0 -g 2  # 带宽测试

# 内存与 CPU
grep -E 'HugePages|Hugepagesize' /proc/meminfo  # HugePages 状态
numactl --hardware                    # NUMA 拓扑
lspci -vvv -s $(lspci | grep Mellanox | awk '{print $1}')  # PCIe 详情

# PFC / ECN
ethtool -S mlx5_0 | grep 'rx_prio.*pause'  # PFC 暂停帧统计
tc -s qdisc show dev <iface>               # 流量控制队列统计

# 固件与驱动
mlxfwmanager --query                # 固件版本
modinfo mlx5_core                   # 驱动版本和参数
```

## 附录 B：参考资源

| 资源 | 链接 |
|------|------|
| RDMA Core 文档 | https://github.com/linux-rdma/rdma-core |
| NVIDIA OFED 文档 | https://docs.nvidia.com/networking/category/rdma |
| RoCEv2 配置指南 | https://community.mellanox.com/s/article/understanding-rocev2-configuration |
| DCQCN 拥塞控制 | https://conferences.sigcomm.org/sigcomm/2015/pdf/papers/p523.pdf |
| LMCache 集成文档 | https://github.com/LMCache/LMCache |
| RDMAS 技术设计 | [Rust-RDMA.md](Rust-RDMA.md) |
