//! TCP Transport: binary frame protocol over tokio TcpStream.
//!
//! Protocol (minimal overhead, non-text):
//!   Request:  [u8: opcode][u64: req_id][u64: addr][u32: rkey][u32: len][bytes: payload]
//!   Response: [u64: req_id][u8: status][optional: data]
//!   opcodes:  0x01=READ, 0x02=WRITE, 0x03=CAS

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::RdmaError;

use super::{ReconnectableTransport, Transport};

const OP_READ: u8 = 0x01;
const OP_WRITE: u8 = 0x02;
const OP_CAS: u8 = 0x03;

pub struct TcpTransport {
    stream: Arc<Mutex<TcpStream>>,
    request_id: AtomicU64,
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(addr: &str) -> Result<Self, RdmaError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RdmaError::Internal(format!("TCP connect to {}: {}", addr, e)))?;
        stream
            .set_nodelay(true)
            .map_err(|e| RdmaError::Internal(format!("set_nodelay: {}", e)))?;
        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            request_id: AtomicU64::new(1),
        })
    }

    async fn read(
        &self,
        buf: &mut [u8],
        _lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<(), RdmaError> {
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().await;

        // Send READ request
        stream
            .write_all(&[OP_READ])
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&req_id.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&remote_addr.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&rkey.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&(buf.len() as u32).to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;

        // Read response: req_id(8) + status(1) + len(4)
        let mut header = [0u8; 13];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        if header[8] != 0 {
            return Err(RdmaError::Internal("TCP READ failed".into()));
        }
        let data_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
        let copy_len = data_len.min(buf.len());

        let mut data = vec![0u8; data_len];
        stream
            .read_exact(&mut data)
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        buf[..copy_len].copy_from_slice(&data[..copy_len]);
        Ok(())
    }

    async fn write(
        &self,
        buf: &[u8],
        _lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<(), RdmaError> {
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().await;

        stream
            .write_all(&[OP_WRITE])
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&req_id.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&remote_addr.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&rkey.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&(buf.len() as u32).to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(buf)
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;

        let mut resp = [0u8; 9]; // req_id(8) + status(1)
        stream
            .read_exact(&mut resp)
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        if resp[8] != 0 {
            return Err(RdmaError::Internal("TCP WRITE failed".into()));
        }
        Ok(())
    }

    async fn cas(
        &self,
        compare: u64,
        swap: u64,
        _lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> Result<bool, RdmaError> {
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().await;

        stream
            .write_all(&[OP_CAS])
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&req_id.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&remote_addr.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&rkey.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&compare.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        stream
            .write_all(&swap.to_le_bytes())
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;

        let mut resp = [0u8; 10]; // req_id(8) + status(1) + swapped(1)
        stream
            .read_exact(&mut resp)
            .await
            .map_err(|e| RdmaError::Internal(e.to_string()))?;
        Ok(resp[8] == 0 && resp[9] == 1)
    }

    fn is_rdma(&self) -> bool {
        false
    }
    fn name(&self) -> &'static str {
        "TCP"
    }
}

#[async_trait]
impl ReconnectableTransport for TcpTransport {
    async fn reconnect(&self, server_addr: &str) -> Result<Box<dyn Transport>, RdmaError> {
        // Create a fresh TCP connection from scratch.
        let transport = TcpTransport::connect(server_addr).await?;
        Ok(Box::new(transport))
    }
}
