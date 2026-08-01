# TCP 回退传输层设计

> 当无 RDMA 网卡时，自动回退到 TCP 模式运行。不修改现有 RDMA 代码的任何一行。

---

## 〇、设计目标

| 要求 | 实现方式 |
|------|---------|
| **RDMA 代码零改动** | 在现有 `src/rdma/` 之上插入 `Transport` trait 抽象层 |
| **引擎层零感知** | `src/engine/` 不关心底层是 RDMA 还是 TCP |
| **自动检测** | ClientSession::connect 时探测 RDMA 设备 → 有就用 RDMA，没有就 TCP |
| **API 兼容** | `ClientReader::get` / `ClientWriter::insert` 签名不变 |
| **TCP 效率** | 单连接复用，无协议解析开销（二进制帧协议，非 RESP/HTTP） |

---

## 一、架构变更：插入 Transport 抽象层

### 现有架构

```
ClientSession → QueuePair (ibverbs QP, One-Sided RDMA)
              → CompletionQueue
              → RdmaRuntime (async poller)
```

### 新架构

```
ClientSession → Transport (trait)
                    ├── RdmaTransport  (One-Sided RDMA, 现有代码)
                    └── TcpTransport   (TCP Send/Recv, 新增)
```

**RDMA 代码变化：零。** `QueuePair`、`CompletionQueue`、`RdmaRuntime` 保持不动。只是 `ClientSession` 现在使用 `Transport` trait 而非直接持有 `QueuePair`。

---

## 二、Transport Trait 定义

```rust
// src/transport/mod.rs (新文件)
use async_trait::async_trait;
use crate::error::RdmaError;

/// 抽象传输层：对上层暴露统一的读/写/CAS 接口
#[async_trait]
pub trait Transport: Send + Sync {
    /// 连接到远程节点
    async fn connect(addr: &str) -> Result<Self, RdmaError> where Self: Sized;

    /// 从远程地址读取数据到本地 buffer
    async fn read(
        &self,
        local_buf: &mut [u8],
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<(), RdmaError>;

    /// 将本地 buffer 写入远程地址
    async fn write(
        &self,
        local_buf: &[u8],
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<(), RdmaError>;

    /// 远程 CAS 操作
    async fn cas(
        &self,
        compare: u64,
        swap: u64,
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<bool, RdmaError>;

    /// 是否为 RDMA 传输（用于性能统计和优化决策）
    fn is_rdma(&self) -> bool;

    /// 获取传输类型名称
    fn name(&self) -> &'static str;
}
```

---

## 三、RdmaTransport：现有 RDMA 代码的薄包装

```rust
// src/transport/rdma.rs (新文件)
use async_trait::async_trait;
use std::sync::Arc;
use crate::rdma::{QueuePair, CompletionQueue};
use crate::runtime::{RdmaRuntime, Poller};
use crate::error::RdmaError;

pub struct RdmaTransport {
    runtime: Arc<RdmaRuntime>,
    _poller: Poller,          // 保持 poller 线程存活
    cq: Arc<CompletionQueue>,
    qp: Arc<QueuePair>,
}

#[async_trait]
impl Transport for RdmaTransport {
    async fn connect(server_addr: &str) -> Result<Self, RdmaError> {
        // 1. 打开 RDMA 设备
        // 2. 创建 PD、CQ、QP
        // 3. gRPC Discover → 获取 server MR 元数据
        // 4. QP: INIT → RTR → RTS
        // 5. 启动 Poller + RdmaRuntime
        // → 现有 ClientSession::connect 逻辑，无改动
    }

    async fn read(&self, buf: &mut [u8], lkey: u32, remote_addr: u64, rkey: u32) -> Result<(), RdmaError> {
        self.runtime.rdma_read(buf, lkey, remote_addr, rkey).await
    }

    async fn write(&self, buf: &[u8], lkey: u32, remote_addr: u64, rkey: u32) -> Result<(), RdmaError> {
        self.runtime.rdma_write(buf, lkey, remote_addr, rkey).await
    }

    async fn cas(&self, compare: u64, swap: u64, lkey: u32, remote_addr: u64, rkey: u32) -> Result<bool, RdmaError> {
        self.runtime.rdma_cas(compare, swap, lkey, remote_addr, rkey).await?;
        Ok(true) // CAS 成功; 失败会返回 Err
    }

    fn is_rdma(&self) -> bool { true }
    fn name(&self) -> &'static str { "RDMA" }
}
```

**关键：`RdmaTransport::connect` 内部复用现有的 `ClientSession::connect` 全部逻辑。** 只是把 QP/CQ/Runtime 包装进 Transport trait。

---

## 四、TcpTransport：纯 TCP 实现

### 4.1 协议设计

使用二进制帧协议（非文本、非 RESP），最小化解析开销：

```
Client → Server:
  [u8: opcode] [u64: request_id] [u64: remote_addr] [u32: rkey] [u64: compare/swap OR u32:length] [bytes: payload]

  opcode: 0x01 = READ, 0x02 = WRITE, 0x03 = CAS

Server → Client:
  [u64: request_id] [u8: status] [u32: data_length] [bytes: data]

  status: 0x00 = SUCCESS, 0x01 = ERROR
```

### 4.2 TcpTransport 实现

```rust
// src/transport/tcp.rs (新文件)
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::error::RdmaError;

pub struct TcpTransport {
    stream: Arc<Mutex<TcpStream>>,
    request_id: AtomicU64,
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(addr: &str) -> Result<Self, RdmaError> {
        let stream = TcpStream::connect(addr).await
            .map_err(|e| RdmaError::Internal(format!("TCP connect: {}", e)))?;
        stream.set_nodelay(true).ok(); // 禁用 Nagle 算法，降低延迟
        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            request_id: AtomicU64::new(1),
        })
    }

    async fn read(&self, buf: &mut [u8], _lkey: u32, remote_addr: u64, rkey: u32) -> Result<(), RdmaError> {
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().await;

        // 发送 READ 请求
        let header = [0x01u8]; // READ opcode
        stream.write_all(&header).await?;
        stream.write_all(&req_id.to_le_bytes()).await?;
        stream.write_all(&remote_addr.to_le_bytes()).await?;
        stream.write_all(&rkey.to_le_bytes()).await?;
        stream.write_all(&(buf.len() as u32).to_le_bytes()).await?;

        // 读取响应
        let mut resp_header = [0u8; 13]; // req_id(8) + status(1) + len(4)
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[8];
        let data_len = u32::from_le_bytes(resp_header[9..13].try_into().unwrap());

        if status != 0 {
            return Err(RdmaError::Internal("TCP READ failed".into()));
        }

        let mut data = vec![0u8; data_len as usize];
        stream.read_exact(&mut data).await?;
        let copy_len = data_len.min(buf.len() as u32) as usize;
        buf[..copy_len].copy_from_slice(&data[..copy_len]);
        Ok(())
    }

    async fn write(&self, buf: &[u8], _lkey: u32, remote_addr: u64, rkey: u32) -> Result<(), RdmaError> {
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().await;

        let header = [0x02u8]; // WRITE opcode
        stream.write_all(&header).await?;
        stream.write_all(&req_id.to_le_bytes()).await?;
        stream.write_all(&remote_addr.to_le_bytes()).await?;
        stream.write_all(&rkey.to_le_bytes()).await?;
        stream.write_all(&(buf.len() as u32).to_le_bytes()).await?;
        stream.write_all(buf).await?;

        let mut resp = [0u8; 9]; // req_id(8) + status(1)
        stream.read_exact(&mut resp).await?;
        if resp[8] != 0 {
            return Err(RdmaError::Internal("TCP WRITE failed".into()));
        }
        Ok(())
    }

    async fn cas(&self, compare: u64, swap: u64, _lkey: u32, remote_addr: u64, rkey: u32) -> Result<bool, RdmaError> {
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().await;

        let header = [0x03u8]; // CAS opcode
        stream.write_all(&header).await?;
        stream.write_all(&req_id.to_le_bytes()).await?;
        stream.write_all(&remote_addr.to_le_bytes()).await?;
        stream.write_all(&rkey.to_le_bytes()).await?;
        stream.write_all(&compare.to_le_bytes()).await?;
        stream.write_all(&swap.to_le_bytes()).await?;

        let mut resp = [0u8; 10]; // req_id(8) + status(1) + swapped(1)
        stream.read_exact(&mut resp).await?;
        Ok(resp[9] == 1) // true = CAS succeeded
    }

    fn is_rdma(&self) -> bool { false }
    fn name(&self) -> &'static str { "TCP" }
}
```

### 4.3 TCP Server 端

Server 需要监听 TCP 端口，处理 READ/WRITE/CAS 请求：

```rust
// src/transport/tcp_server.rs (新文件)
use tokio::net::TcpListener;

pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    pub async fn bind(addr: &str) -> Result<Self, RdmaError> {
        let listener = TcpListener::bind(addr).await
            .map_err(|e| RdmaError::Internal(format!("TCP bind: {}", e)))?;
        Ok(Self { listener })
    }

    pub async fn serve(&self, engine: Arc<BootstrappedEngine>) {
        loop {
            let (mut stream, _) = self.listener.accept().await.unwrap();
            let engine = engine.clone();
            tokio::spawn(async move {
                handle_tcp_client(&mut stream, engine).await;
            });
        }
    }
}

async fn handle_tcp_client(stream: &mut TcpStream, engine: Arc<BootstrappedEngine>) {
    let mut buf = [0u8; 4096];
    loop {
        // 读 opcode
        if stream.read_exact(&mut buf[..1]).await.is_err() { return; }
        let opcode = buf[0];
        // 读 request_id
        if stream.read_exact(&mut buf[..8]).await.is_err() { return; }
        let req_id = u64::from_le_bytes(buf[..8].try_into().unwrap());

        match opcode {
            0x01 => handle_read(stream, req_id, engine).await,
            0x02 => handle_write(stream, req_id, engine).await,
            0x03 => handle_cas(stream, req_id, engine).await,
            _ => { return; }
        }
    }
}

async fn handle_read(stream: &mut TcpStream, req_id: u64, engine: Arc<BootstrappedEngine>) {
    let mut buf = [0u8; 16]; // addr(8) + rkey(4) + len(4)
    if stream.read_exact(&mut buf).await.is_err() { return; }
    let remote_addr = u64::from_le_bytes(buf[..8].try_into().unwrap());
    let data_len = u32::from_le_bytes(buf[12..16].try_into().unwrap());

    // 从 HugePage 直接读数据（Server CPU 参与——TCP 路径不可避免）
    let data = unsafe {
        std::slice::from_raw_parts(remote_addr as *const u8, data_len as usize)
    };

    let mut resp = [0u8; 13]; // req_id(8) + status(1) + len(4)
    resp[..8].copy_from_slice(&req_id.to_le_bytes());
    resp[8] = 0x00; // SUCCESS
    resp[9..13].copy_from_slice(&data_len.to_le_bytes());

    let _ = stream.write_all(&resp).await;
    let _ = stream.write_all(data).await;
}

async fn handle_write(stream: &mut TcpStream, req_id: u64, engine: Arc<BootstrappedEngine>) {
    // 类似：读 addr + rkey + len + data → 写入 HugePage → 返回 OK
}

async fn handle_cas(stream: &mut TcpStream, req_id: u64, engine: Arc<BootstrappedEngine>) {
    // 类似：读 addr + rkey + compare + swap → CAS 操作 → 返回结果
}
```

---

## 五、ClientSession 适配

### 现有 connect 逻辑

```rust
// src/client/session.rs (现有)
impl ClientSession {
    pub async fn connect(server_addr: &str) -> Result<Self> {
        // 1. gRPC discover
        // 2. Open RDMA device
        // 3. Create QP...
    }
}
```

### 新 connect 逻辑（增加 TCP 回退）

```rust
impl ClientSession {
    pub async fn connect(server_addr: &str) -> Result<Self> {
        let control = ControlClient::connect(server_addr).await?;
        let metadata = control.discover().await?;

        // 尝试 RDMA，失败则回退 TCP
        let transport: Box<dyn Transport> = match RdmaTransport::connect(server_addr).await {
            Ok(rdma) => {
                tracing::info!("Using RDMA transport");
                Box::new(rdma)
            }
            Err(e) => {
                tracing::warn!("RDMA unavailable ({}), falling back to TCP", e);
                let tcp = TcpTransport::connect(server_addr).await
                    .map_err(|e| RdmaError::Internal(format!("TCP fallback failed: {}", e)))?;
                Box::new(tcp)
            }
        };

        Ok(Self { control, metadata, transport, ... })
    }
}
```

---

## 六、目录结构

```
src/
├── transport/              # 新增: 传输抽象层
│   ├── mod.rs              # Transport trait 定义
│   ├── rdma.rs             # RdmaTransport (现有 RDMA 代码的薄包装)
│   ├── tcp.rs              # TcpTransport (TCP 客户端)
│   └── tcp_server.rs       # TCP Server (服务端监听)
├── rdma/                   # 现有: 不改动
├── runtime/                # 现有: 不改动
├── engine/                 # 现有: 不改动
├── client/
│   └── session.rs          # 修改: 使用 Transport trait 替代直接持有 QP
```

---

## 七、对现有代码的影响

| 文件 | 是否修改 | 修改内容 |
|------|---------|---------|
| `src/rdma/**` | ❌ 不修改 | — |
| `src/runtime/**` | ❌ 不修改 | — |
| `src/engine/**` | ❌ 不修改 | — |
| `src/client/session.rs` | ✅ 修改 | 用 `Transport` trait 替代直接持有 `QueuePair` |
| `src/client/read.rs` | ✅ 修改 | 用 `transport.read()` 替代 `runtime.rdma_read()` |
| `src/client/write.rs` | ✅ 修改 | 用 `transport.write()/cas()` 替代直接 QP 操作 |
| `src/transport/` | 🆕 新增 | 4 个文件 |
| `src/control/` | ✅ 修改 | 新增 TCP Server 启动逻辑 |

---

## 八、关键设计决策

### 8.1 为什么 TCP 路径用 Server CPU？

**无法避免。** TCP 被设计为两方协议——Server 必须解析请求、读写内存、返回响应。但这是可接受的降级：有 RDMA 时 Server CPU = 0，无 RDMA 时 Server CPU > 0。

### 8.2 为什么用二进制协议而非 HTTP/gRPC？

HTTP/gRPC 对 64 字节的 KV 操作来说开销太大（头部 > 数据）。自定义二进制帧协议：
- 请求头 = 1 + 8 + 8 + 4 = 21 字节（vs HTTP ~200+ 字节）
- 单一 TCP 连接复用所有请求（vs HTTP 每请求可能建立新连接）

### 8.3 为什么 TCP Server 内嵌而非独立进程？

减少部署复杂度。ControlServer 启动时同时启动 TCP listener，同一个进程同时处理 gRPC 控制面和 TCP 数据面。

### 8.4 Transport trait 用 async_trait 还是手动 Future？

`async_trait` 更简单，且 `async fn` 在 trait 中已稳定（Rust 1.75+）。额外一次 Box future 分配的开销在 TCP 路径中可忽略（TCP 延迟远大于分配开销）。

---

## 九、验证方法

```bash
# 无 RDMA 环境（当前机器）
cargo test --lib

# 启动 TCP Server
cargo run --bin rdmas-server -- --transport tcp --port 9999

# 启动 Client（自动检测无 RDMA，回退 TCP）
cargo run --bin rdmas-client -- --server localhost:9999 --transport auto
```

**预期**：所有 205 测试通过，TCP 模式下能完成基本的读/写/CAS 操作。`is_rdma()` 返回 `false`，日志显示 "Falling back to TCP"。
