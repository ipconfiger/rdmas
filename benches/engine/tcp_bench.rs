//! TCP transport performance benchmarks.
//!
//! Measures TCP transport READ/WRITE latency across different payload sizes
//! using a local mock server that implements the RDMAS binary frame protocol.
//!
//! ## Usage
//! ```bash
//! cargo bench --bench tcp
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rdmas::transport::{TcpTransport, Transport};

// ---------------------------------------------------------------------------
// Mock TCP server (benchmark edition)
// ---------------------------------------------------------------------------

/// Start a mock TCP server on a random port and return its address together
/// with a shutdown flag.  The server handles multiple requests per connection
/// (needed because the benchmark reuses one `TcpTransport` across iterations).
fn start_mock_server() -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();

    thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("set_nonblocking failed");
        let mut buffer = vec![0u8; 65536]; // 64 KB server memory

        loop {
            if !running_clone.load(Ordering::Relaxed) {
                break;
            }

            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nodelay(true).ok();
                    // Handle all requests on this connection until disconnect
                    handle_bench_client(&mut stream, &mut buffer);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Err(_) => break,
            }
        }
    });

    (addr, running)
}

/// Handle one client connection, processing requests in a loop until the
/// stream errors (client disconnects) or an unknown opcode arrives.
fn handle_bench_client(stream: &mut TcpStream, _buf: &mut [u8]) {
    loop {
        let mut op = [0u8; 1];
        if stream.read_exact(&mut op).is_err() {
            return;
        }

        let mut rid = [0u8; 8];
        if stream.read_exact(&mut rid).is_err() {
            return;
        }

        match op[0] {
            0x01 => {
                // READ: params = addr(8) + rkey(4) + len(4) = 16
                let mut params = [0u8; 16];
                if stream.read_exact(&mut params).is_err() {
                    return;
                }
                let len = u32::from_le_bytes(params[12..16].try_into().unwrap()) as usize;

                // Response: req_id(8) + status(1) + data_len(4) + data
                let mut resp = vec![0u8; 13 + len];
                resp[..8].copy_from_slice(&rid);
                resp[9..13].copy_from_slice(&(len as u32).to_le_bytes());
                // Data is filled with zeros (buffer is zeroed)

                if stream.write_all(&resp).is_err() {
                    return;
                }
                // Flush to push data out immediately
                let _ = stream.flush();
            }

            0x02 => {
                // WRITE: params = addr(8) + rkey(4) + len(4) = 16 + payload
                let mut params = [0u8; 16];
                if stream.read_exact(&mut params).is_err() {
                    return;
                }
                let len = u32::from_le_bytes(params[12..16].try_into().unwrap()) as usize;

                let mut data = vec![0u8; len];
                if stream.read_exact(&mut data).is_err() {
                    return;
                }

                // Response: req_id(8) + status(1) = 9
                let mut resp = [0u8; 9];
                resp[..8].copy_from_slice(&rid);
                resp[8] = 0x00;
                if stream.write_all(&resp).is_err() {
                    return;
                }
                let _ = stream.flush();
            }

            _ => return,
        }
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_tcp_ops(c: &mut Criterion) {
    let (addr, _running) = start_mock_server();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    let transport = rt.block_on(async { TcpTransport::connect(&addr).await.unwrap() });

    let mut group = c.benchmark_group("tcp_transport");
    group.measurement_time(Duration::from_secs(3));

    // ── READ benchmarks ──
    let sizes = [64usize, 256, 1024, 4096];
    for &size in &sizes {
        group.bench_with_input(BenchmarkId::new("read", size), &size, |b, &size| {
            let mut buf = vec![0u8; size];
            b.iter(|| {
                rt.block_on(async {
                    let _ = transport.read(&mut buf, 0, 0, 0).await;
                });
                black_box(&buf);
            });
        });
    }

    // ── WRITE benchmarks ──
    for &size in &sizes {
        group.bench_with_input(BenchmarkId::new("write", size), &size, |b, &size| {
            let data = vec![0x42u8; size];
            b.iter(|| {
                rt.block_on(async {
                    let _ = transport.write(&data, 0, 0, 0).await;
                });
                black_box(&data);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_tcp_ops);
criterion_main!(benches);
