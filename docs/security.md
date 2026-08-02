# RDMAS RDMA KV 存储安全设计文档

## 1. 威胁模型

### 1.1 范围内威胁

| 威胁类型 | 描述 | 影响面 |
|----------|------|--------|
| **窃听 (Eavesdropping)** | 攻击者旁路抓取控制面gRPC流量，窃取 MR rkey、vaddr 等元数据 | 控制面 |
| **中间人攻击 (MITM)** | 攻击者冒充合法节点，篡改 RegionMetadata 路由信息，诱导客户端写入恶意地址 | 控制面 |
| **未授权访问 (Unauthorized Access)** | 未认证节点接入集群，注册为客户端并访问哈希表 / extent 数据 | 控制面 + 数据面 |
| **资源耗尽 (Resource Exhaustion)** | 恶意客户端无限注册 / 发送心跳，消耗服务器 QP、内存等资源 | 控制面 |

### 1.2 范围外威胁

- **数据面 (RDMA) 窃听**：RDMA 数据面运行在可信数据中心 Fabric（InfiniBand / RoCE）内，物理安全由机架 / 交换机隔离保障。这是 RDMA 行业的通用实践（参见 NVMe-oF、TensorRT-LLM 等部署模式）。
- **物理攻击**：服务器物理访问，内存 dump。
- **固件攻击**：NIC 固件后门，PCIe 总线嗅探。
- **DoS via RDMA**：恶意对端直接发送 RDMA WRITE/CAS（由 GPUDirect 安全层防护，见第 6 节）。

---

## 2. 方案选择

### 2.1 候选方案对比

| 方案 | 保护层 | 数据面延迟影响 | 部署复杂度 | 适用场景 |
|------|--------|--------------|-----------|----------|
| **Option 1: gRPC 控制面 mTLS** | 控制面 | **0μs** (完全不触及数据面) | 低 (Cargo.toml + 证书) | ✅ 本方案 |
| **Option 2: IPsec 传输模式** | 网络层 | ~5-15μs (内核加解密) | 高 (需交换机支持, 运维复杂) | 跨DC / 广域网 |
| **Option 3: AES-GCM 应用层加密** | 数据面 | ~2-5μs/op (每操作加解密) | 中 (CR 代码) | 零信任环境 |

### 2.2 选择 Option 1 的理由

1. **零数据面延迟影响**：mTLS 仅在连接建立和周期性心跳时工作，完全不触及 RDMA 写路径。数据面维持 ~1.5μs 的单边 RDMA 读延迟。
2. **行业标准实践**：gRPC + mTLS 是云原生控制面的标配（Kubernetes API Server、etcd、Linkerd 均采用此模式）。
3. **最小化攻击面**：控制面是唯一的外露接口（gRPC 端口 9400）。数据面（RDMA QP）绑定到特定设备，无 TCP socket 暴露。
4. **实施成本低**：仅需创建自签名 CA + 节点证书，无需修改 RDMA 传输层代码。

### 2.3 为什么不选择 IPsec / AES-GCM

- **IPsec**：需要网络管理员配置 ESP 策略和 IKE 协商，在与 InfiniBand 交换机配合时常有兼容性问题，且显著增加运维复杂度。
- **AES-GCM 应用层加密**：每个 `rdma_write` / `rdma_read` 操作需要 CPU 执行 AES-GCM (2-5μs)，在批量写入 1024 entries 的场景下会导致 **2-5ms 累积延迟**，对于 KV 存储是致命的。此外，加密后的数据不再支持 RDMA CAS 原子操作。

---

## 3. mTLS 实现方案

### 3.1 架构概览

```
┌──────────────┐                    ┌──────────────┐
│  Client      │                    │  Server      │
│  ┌──────────┐│   gRPC (mTLS)     │┌──────────┐  │
│  │Control   ││◄──────────────────►││Control   │  │
│  │Client    ││  ┌──────────────┐  ││Server    │  │
│  └──────────┘│  │  TLS 1.3     │  │└──────────┘  │
│              │  │  - Server cert│  │              │
│  ┌──────────┐│  │  - Client cert│  │┌──────────┐  │
│  │RDMA      ││  │  - CA verify  │  ││Data      │  │
│  │Transport ││  └──────────────┘  ││Regions   │  │
│  └──────────┘│                    │└──────────┘  │
│              │  RDMA (明文)         │              │
│  RDMA WRITE  │◄═══════════════════►│  RDMA READ   │
│  (DC Fabric) │  受信任数据中心网络    │  (DC Fabric) │
└──────────────┘                    └──────────────┘
```

### 3.2 服务端 TLS 配置

`ControlServer` 提供静态工厂方法 `tls_config()` 创建 `ServerTlsConfig`：

```rust
use tonic::transport::{Identity, ServerTlsConfig, Certificate};

impl ControlServer {
    /// 创建 mTLS 服务端配置
    pub fn tls_config(
        cert_pem: &str,
        key_pem: &str,
        ca_cert_pem: Option<&str>,
    ) -> Result<ServerTlsConfig, Box<dyn std::error::Error>> {
        let identity = Identity::from_pem(cert_pem, key_pem);
        let mut config = ServerTlsConfig::new().identity(identity);

        // 如果提供了 CA 证书，启用客户端证书认证 (mTLS)
        if let Some(ca_pem) = ca_cert_pem {
            let ca = Certificate::from_pem(ca_pem);
            config = config.client_ca_root(ca);
        }

        Ok(config)
    }
}
```

镜像逻辑也反映在 `ControlServer` 的 `with_tls()` builder 模式中：
- `use_tls` 字段控制是否将 TLS config 应用到 `tonic::transport::Server::builder()`。
- 典型使用：

```rust
let server = ControlServer::new(engine).with_tls();
let tls_cfg = ControlServer::tls_config(cert, key, Some(ca))?;

Server::builder()
    .tls_config(tls_cfg)?
    .add_service(server.into_service())
    .serve(addr)
    .await?;
```

### 3.3 客户端 TLS 配置

`ClientSession` 通过 `connect_tls()` 方法建立 mTLS 连接：

```rust
pub struct TlsConfig {
    pub ca_cert_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
}

impl ClientSession {
    pub async fn connect_tls(
        server_addr: &str,
        client_id: u64,
        tls_config: &TlsConfig,
    ) -> Result<Self, ClientSessionError> {
        // ...
    }
}
```

`ControlClient` 相应支持 TLS channel：

```rust
impl ControlClient {
    pub async fn connect_tls(
        addr: &str,
        tls_config: &ClientTlsConfig, // tonic::transport::channel::ClientTlsConfig
    ) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(format!("https://{}", addr))?
            .tls_config(tls_config.clone())?;
        let channel = endpoint.connect().await?;
        let inner = ControlPlaneClient::new(channel);
        Ok(Self { inner })
    }
}
```

### 3.4 证书管理策略

生产部署推荐使用自签名 CA 管理集群内部证书：

1. **CA 证书** (ca.pem)：由运维或 K8s cert-manager 生成，分发给所有节点
2. **节点证书** (server.pem / key.pem 和 client.pem / key.pem)：每个节点拥有一对
3. **SAN 约束**：证书 Subject Alternative Name 设置为节点 hostname
4. **轮换**：证书有效期 365 天，通过重启应用自动重新加载

开发 / 测试环境可直接使用 mkcert 生成 localhost 证书。

---

## 4. rkey 安全

### 4.1 现状分析

目前 rkey 通过 gRPC `DiscoverResponse` 的 `RegionMetadata` 字段分发给客户端。对于封闭集群（节点数 < 100），这**暂时足够**，原因如下：

1. **rkey 是 RDMA 访问的门槛**：没有有效 rkey 的攻击者无法发起 RDMA 操作。rkey 由内核生成，每次 PD 注册时随机分配。
2. **mTLS 保证了传输层安全**：rkey 在 gRPC 通道中传输时已由 TLS 1.3 加密，不会被中间人窃取。
3. **内存区域边界保护**：`RegionMetadata.size` 和 `region.type` 告诉客户端每个区域的边界。即使拥有 rkey，恶意客户端也只能在注册区域内读写，无法越界。

### 4.2 局限性与改进方向

- **rkey 分发后无撤销**：一旦客户端收到 rkey，它可以在 gRPC 通道关闭后仍然发起 RDMA 操作，直到服务器取消注册 MR。
- **改进方向**（Wave 12+）：服务器周期性地重新注册 MR（生成新 rkey），通过 heartbeat 或 generation bump 推送给合法客户端，旧的 rkey 自然失效。

---

## 5. 未来改进

### 5.1 GPUDirect 安全 (Wave 12+)

- **威胁**：GPU 显存映射到 BAR 空间后，任何拥有 vaddr + rkey 的客户端都可以通过 GPUDirect RDMA 直接读写 GPU 内存。
- **防护措施**：
  - 使用 CUDA MPS 限制 GPU 上下文访问
  - GPUDirect 专用的独立 rkey namespace（每个 client 分配不同的 rkey）
  - PCIe ACS (Access Control Services) 在 IOMMU 层做设备隔离

### 5.2 多租户数据隔离 (Wave 13+)

- **密钥分区**：每个租户分配独立的哈希表分片，通过不同的 rkey 访问
- **配额控制**：gRPC 服务端增加 per-tenant 的 QPS / 内存使用配额检查
- **审计日志**：每次 `Discover` / `Deregister` 操作记录租户 ID 和时间戳

### 5.3 速率限制 (Wave 11)

- 在 gRPC 层实现 token bucket 限流，防止恶意客户端高频发送 heartbeat / deregister 导致服务器资源耗尽
- 统计数据面 RDMA 操作频率，对异常高频的客户端自动解除注册

---

## 6. 延迟预算分析

### 6.1 各方案延迟影响量化

| 方案 | 额外延迟来源 | 单次操作开销 | 批量1024 ops 累积 | 影响面 |
|------|-------------|-------------|-------------------|--------|
| **Option 1 (mTLS)** | 握手 (仅连接建立时) | 0μs (数据面) | 0μs | 控制面 |
| **Option 2 (IPsec)** | 内核 ESP 加解密 + 封装 | 5-15μs/op | 5-15ms | 数据面 |
| **Option 3 (AES-GCM)** | 用户态 AES-NI 加解密 | 2-5μs/op | 2-5ms | 数据面 |

### 6.2 延迟预算约束

当前系统延迟预算：

```
基线 RDMA Read (1 条目): ~1.5μs
目标 P99 延迟:         <100μs
```

- Option 1 在预算内完全不影响，因为 TLS 握手在连接建立阶段完成，数据面零开销。
- Option 2 / 3 均会导致**数据面延迟增加 2-15μs/op**，在批量操作场景下 (1024 ops × 5μs = 5ms) 完全超出预算。

### 6.3 结论

**Option 1 (gRPC 控制面 mTLS) 是当前最优解**。它在不触及数据面热路径的前提下，提供了对控制面元数据（rkey、vaddr、generation）的端到端加密和双向认证，满足封闭集群的安全需求。数据面明文方案在可信数据中心 Fabric 环境下符合行业惯例。
