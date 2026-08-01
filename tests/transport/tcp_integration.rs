//! TCP transport integration tests: end-to-end READ / WRITE / CAS
//! with a mock TCP server implementing the RDMAS binary frame protocol.
//!
//! Protocol:
//!   Request:  [u8: opcode][u64: req_id][u64: addr][u32: rkey][u32: len][payload]
//!   Response: [u64: req_id][u8: status][data...]
//!   opcodes:  0x01=READ, 0x02=WRITE, 0x03=CAS

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use rdmas::transport::TcpTransport;
use rdmas::transport::Transport;

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

const OP_READ: u8 = 0x01;
const OP_WRITE: u8 = 0x02;
const OP_CAS: u8 = 0x03;

// ---------------------------------------------------------------------------
// Mock TCP server
// ---------------------------------------------------------------------------

/// A minimal TCP server that implements the RDMAS binary frame protocol.
///
/// Maintains an in-memory buffer that acts as "remote memory" for read/write/cas
/// operations.  Runs in a background thread and shuts down cleanly on drop.
struct MockTcpServer {
    addr: String,
    handle: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicU64>,
}

impl MockTcpServer {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        let mut buffer = vec![0xAAu8; 4096]; // 4 KB test buffer, all 0xAA
        let shutdown = Arc::new(AtomicU64::new(0));
        let shutdown_clone = shutdown.clone();

        let handle = thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("set_nonblocking failed");

            loop {
                // Check shutdown before blocking on accept
                if shutdown_clone.load(Ordering::Relaxed) > 0 {
                    break;
                }

                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nodelay(true).ok();
                        handle_client(&mut stream, &mut buffer);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No pending connection — briefly yield then retry
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            handle: Some(handle),
            shutdown,
        }
    }

    fn addr(&self) -> &str {
        &self.addr
    }
}

impl Drop for MockTcpServer {
    fn drop(&mut self) {
        self.shutdown.store(1, Ordering::Relaxed);
        // We switched to non-blocking mode so no need to connect-to-unblock,
        // but keep the fallback in case the thread is waiting.
        let _ = std::net::TcpStream::connect(&self.addr);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Client protocol handler
// ---------------------------------------------------------------------------

/// Handle one client connection in a loop until the stream errors (client
/// disconnects) or a malformed / unknown opcode arrives.
fn handle_client<S: Read + Write>(stream: &mut S, buffer: &mut [u8]) {
    loop {
        // ── Read opcode ──
        let mut opcode = [0u8; 1];
        if stream.read_exact(&mut opcode).is_err() {
            return;
        }

        // ── Read request id ──
        let mut req_id_buf = [0u8; 8];
        if stream.read_exact(&mut req_id_buf).is_err() {
            return;
        }
        let req_id = u64::from_le_bytes(req_id_buf);

        match opcode[0] {
            OP_READ => {
                // params: addr(8) + rkey(4) + len(4) = 16 bytes
                let mut params = [0u8; 16];
                if stream.read_exact(&mut params).is_err() {
                    return;
                }
                let offset = u64::from_le_bytes(params[..8].try_into().unwrap()) as usize;
                let data_len =
                    u32::from_le_bytes(params[12..16].try_into().unwrap()) as usize;

                let end = (offset + data_len).min(buffer.len());
                let actual_len = end.saturating_sub(offset);

                // Response: req_id(8) + status(1) + actual_len(4)
                let mut resp = [0u8; 13];
                resp[..8].copy_from_slice(&req_id.to_le_bytes());
                resp[8] = 0x00; // OK
                resp[9..13].copy_from_slice(&(actual_len as u32).to_le_bytes());
                stream.write_all(&resp).unwrap();

                if actual_len > 0 {
                    stream.write_all(&buffer[offset..end]).unwrap();
                }
            }

            OP_WRITE => {
                // params: addr(8) + rkey(4) + len(4) = 16 bytes
                let mut params = [0u8; 16];
                if stream.read_exact(&mut params).is_err() {
                    return;
                }
                let offset = u64::from_le_bytes(params[..8].try_into().unwrap()) as usize;
                let data_len =
                    u32::from_le_bytes(params[12..16].try_into().unwrap()) as usize;

                let mut data = vec![0u8; data_len];
                if stream.read_exact(&mut data).is_err() {
                    return;
                }

                let end = (offset + data_len).min(buffer.len());
                let copy_len = end - offset;
                buffer[offset..end].copy_from_slice(&data[..copy_len]);

                // Response: req_id(8) + status(1) = 9 bytes
                let mut resp = [0u8; 9];
                resp[..8].copy_from_slice(&req_id.to_le_bytes());
                resp[8] = 0x00;
                stream.write_all(&resp).unwrap();
            }

            OP_CAS => {
                // CAS params: addr(8) + rkey(4) + compare(8) + swap(8) = 28 bytes
                let mut params = [0u8; 28];
                if stream.read_exact(&mut params).is_err() {
                    return;
                }
                let offset = u64::from_le_bytes(params[..8].try_into().unwrap()) as usize;
                let compare = u64::from_le_bytes(params[12..20].try_into().unwrap());
                let swap = u64::from_le_bytes(params[20..28].try_into().unwrap());

                if offset + 8 <= buffer.len() {
                    let current =
                        u64::from_le_bytes(buffer[offset..offset + 8].try_into().unwrap());
                    let swapped = current == compare;
                    if swapped {
                        buffer[offset..offset + 8].copy_from_slice(&swap.to_le_bytes());
                    }
                    // Response: req_id(8) + status(1) + swapped_flag(1) = 10 bytes
                    let mut resp = [0u8; 10];
                    resp[..8].copy_from_slice(&req_id.to_le_bytes());
                    resp[8] = 0x00;
                    resp[9] = swapped as u8;
                    stream.write_all(&resp).unwrap();
                } else {
                    // Out of bounds
                    let mut resp = [0u8; 10];
                    resp[..8].copy_from_slice(&req_id.to_le_bytes());
                    resp[8] = 0x01; // error status
                    resp[9] = 0x00;
                    stream.write_all(&resp).unwrap();
                }
            }

            _ => {
                // Unknown opcode → disconnect
                return;
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_tcp_read() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();
        assert!(!transport.is_rdma());
        assert_eq!(transport.name(), "TCP");

        let mut buf = vec![0u8; 64];
        transport.read(&mut buf, 0, 0, 0).await.unwrap();
        assert_eq!(&buf[..], &[0xAAu8; 64]);
    });
}

#[test]
fn test_tcp_read_partial_offset() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    // Pre-populate known values at server offsets [100..164]
    // We connect, write known data, then read it back.
    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        let data = b"Hello, RDMAS TCP!";
        transport.write(data, 0, 100, 0).await.unwrap();

        let mut buf = vec![0u8; data.len()];
        transport.read(&mut buf, 0, 100, 0).await.unwrap();
        assert_eq!(&buf[..], &data[..]);
    });
}

#[test]
fn test_tcp_write() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        let data = [0x42u8; 128];
        transport.write(&data, 0, 64, 0).await.unwrap();

        // Read back to verify
        let mut buf = vec![0u8; 128];
        transport.read(&mut buf, 0, 64, 0).await.unwrap();
        assert_eq!(&buf[..], &data[..]);

        // Bytes before offset 64 should still be 0xAA
        let mut before = vec![0u8; 64];
        transport.read(&mut before, 0, 0, 0).await.unwrap();
        assert_eq!(&before[..], &[0xAAu8; 64]);
    });
}

#[test]
fn test_tcp_write_large_payload() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        // Write 2 KB at offset 256
        let data = vec![0xDEu8; 2048];
        transport.write(&data, 0, 256, 0).await.unwrap();

        // Read back
        let mut buf = vec![0u8; 2048];
        transport.read(&mut buf, 0, 256, 0).await.unwrap();
        assert_eq!(buf, data);
    });
}

#[test]
fn test_tcp_cas_success() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        // Buffer starts as all 0xAA, so offset 0 is 0xAAAAAAAA_AAAAAAAA
        let result = transport
            .cas(0xAAAAAAAA_AAAAAAAA, 0xDEADBEEF_DEADBEEF, 0, 0, 0)
            .await
            .unwrap();
        assert!(result, "CAS should succeed when compare matches initial 0xAA");
    });
}

#[test]
fn test_tcp_cas_failure() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        // CAS to set a value first
        transport
            .cas(0xAAAAAAAA_AAAAAAAA, 0xDEADBEEF_DEADBEEF, 0, 0, 0)
            .await
            .unwrap();

        // Now CAS with wrong compare — must fail
        let result = transport
            .cas(0xAAAAAAAA_AAAAAAAA, 0xCAFECAFE_CAFECAFE, 0, 0, 0)
            .await
            .unwrap();
        assert!(!result, "CAS should fail when compare does not match current value");
    });
}

#[test]
fn test_tcp_cas_chain() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        let values: [(u64, u64); 4] = [
            (0xAAAAAAAA_AAAAAAAA, 0x11111111_11111111),
            (0x11111111_11111111, 0x22222222_22222222),
            (0x22222222_22222222, 0x33333333_33333333),
            (0x33333333_33333333, 0x44444444_44444444),
        ];

        for (compare, swap) in &values {
            let result = transport.cas(*compare, *swap, 0, 0, 0).await.unwrap();
            assert!(result, "CAS chain step failed: {:#x} -> {:#x}", compare, swap);
        }
    });
}

#[test]
fn test_tcp_concurrent_reads() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = Arc::new(TcpTransport::connect(server.addr()).await.unwrap());

        let mut handles = vec![];
        for i in 0..8u64 {
            let t = transport.clone();
            handles.push(tokio::spawn(async move {
                let mut buf = vec![0u8; 16];
                t.read(&mut buf, 0, i * 16, 0).await.unwrap();
                assert_eq!(&buf[..], &[0xAAu8; 16]);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
}

#[test]
fn test_tcp_concurrent_writes() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = Arc::new(TcpTransport::connect(server.addr()).await.unwrap());

        let mut handles = vec![];
        for i in 0..4u64 {
            let t = transport.clone();
            let data = vec![i as u8; 16];
            handles.push(tokio::spawn(async move {
                t.write(&data, 0, i * 16, 0).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Verify each write landed
        for i in 0..4u64 {
            let mut buf = vec![0u8; 16];
            transport.read(&mut buf, 0, i * 16, 0).await.unwrap();
            assert_eq!(&buf[..], &[i as u8; 16]);
        }
    });
}

#[test]
fn test_tcp_connect_invalid_address() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
    let result = rt.block_on(TcpTransport::connect("127.0.0.1:19999"));
    assert!(result.is_err(), "Connect to closed port should fail");
}

#[test]
fn test_tcp_connect_bad_hostname() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
    let result = rt.block_on(TcpTransport::connect("invalid.host.name:12345"));
    assert!(result.is_err(), "Connect to invalid hostname should fail");
}

#[test]
fn test_tcp_is_rdma_false() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();
        assert!(!transport.is_rdma());
    });
}

#[test]
fn test_tcp_name() {
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();
        assert_eq!(transport.name(), "TCP");
    });
}

#[test]
fn test_tcp_multiple_operations_single_connection() {
    // Verify the transport can do many ops on one connection without
    // protocol desynchronization.
    let server = MockTcpServer::new();
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();

    rt.block_on(async {
        let transport = TcpTransport::connect(server.addr()).await.unwrap();

        for i in 0..100u64 {
            // Write
            let data = i.to_le_bytes();
            transport.write(&data, 0, i * 8, 0).await.unwrap();

            // Read back
            let mut buf = vec![0u8; 8];
            transport.read(&mut buf, 0, i * 8, 0).await.unwrap();
            assert_eq!(buf, data, "Round-trip failed at iteration {}", i);
        }
    });
}
