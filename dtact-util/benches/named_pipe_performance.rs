//! Criterion bench for `dtact_util::io`'s Windows named pipes vs
//! `tokio::net::windows::named_pipe`: throughput of small-write/read
//! roundtrips over an already-connected client/server pair, run side by
//! side — the same shape `stream_performance.rs` uses for the in-process
//! duplex pipe.
//!
//! Windows-only, matching the native named-pipe backend itself
//! (`dtact-util/src/io/named_pipe_windows.rs`). Every item below is
//! individually `#[cfg(windows)]` rather than the whole file being gated
//! by an inner `#![cfg(windows)]`: `criterion_main!` expands to a `fn
//! main()`, and this `harness = false` bench target (see this crate's
//! `Cargo.toml`) must supply exactly one on every platform — cfg'ing out
//! the entire file would leave it with none at all on non-Windows, a
//! hard build error rather than just a skipped bench. The trivial
//! `#[cfg(not(windows))] fn main() {}` at the bottom is what fills that
//! gap there.
//!
//! The read and write run on separate threads, synchronized by a pair of
//! rendezvous channels, rather than a single thread awaiting the full
//! write before starting the read. A *named* pipe (unlike this crate's
//! in-process `stream::pair`, which is a big enough ring buffer to just
//! hold the whole payload) has a small, fixed-size *kernel* buffer
//! (`named_pipe_windows.rs` creates one with 4096-byte in/out buffers);
//! writing a payload bigger than that buffer genuinely cannot complete
//! until a reader drains it concurrently — awaiting the full write first
//! and only starting the read afterward deadlocks for any payload past
//! the buffer size, which is exactly what an earlier version of this
//! bench did for its 4KB/64KB cases.
//!
//! Run:  cargo bench --bench named_pipe_performance
//! Test: cargo bench --bench named_pipe_performance -- --test

#[cfg(windows)]
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(windows)]
use std::sync::Arc;
#[cfg(windows)]
use std::sync::mpsc as std_mpsc;

#[cfg(windows)]
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, Wake};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Arc::new(NoopWaker).into();
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[cfg(windows)]
fn dtact_pipe_pair(
    name: &str,
) -> (
    dtact_util::io::DtactNamedPipeHandle,
    dtact_util::io::DtactNamedPipeHandle,
) {
    let server = dtact_util::io::DtactNamedPipeServer::create(name).unwrap();
    let accept = std::thread::spawn(move || block_on(server.connect()).unwrap());
    std::thread::sleep(std::time::Duration::from_millis(20));
    let client = block_on(dtact_util::io::DtactNamedPipeClient::connect(name)).unwrap();
    let server_handle = accept.join().unwrap();
    (server_handle, client)
}

#[cfg(windows)]
fn tokio_pipe_pair(
    rt: &tokio::runtime::Runtime,
    name: &str,
) -> (
    tokio::net::windows::named_pipe::NamedPipeServer,
    tokio::net::windows::named_pipe::NamedPipeClient,
) {
    rt.block_on(async {
        let server = tokio::net::windows::named_pipe::ServerOptions::new()
            .create(name)
            .unwrap();
        let accept = tokio::spawn(async move {
            server.connect().await.unwrap();
            server
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let client = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(name)
            .unwrap();
        let server = accept.await.unwrap();
        (server, client)
    })
}

/// Spawn a persistent background reader thread for `dtact-io`'s named
/// pipe: on each `go` signal it reads exactly `size` bytes from `server`,
/// then signals `done`. Kept alive for the whole benchmark (rather than
/// spawned per-iteration) so thread-creation cost doesn't pollute the
/// measured throughput.
#[cfg(windows)]
fn spawn_dtact_reader(
    server: dtact_util::io::DtactNamedPipeHandle,
    size: usize,
) -> (std_mpsc::Sender<()>, std_mpsc::Receiver<()>) {
    let (go_tx, go_rx) = std_mpsc::channel::<()>();
    let (done_tx, done_rx) = std_mpsc::channel::<()>();
    std::thread::spawn(move || {
        let mut buf = vec![0u8; size];
        while go_rx.recv().is_ok() {
            let mut got = 0;
            while got < size {
                got += block_on(server.read(&mut buf[got..])).unwrap();
            }
            if done_tx.send(()).is_err() {
                break;
            }
        }
    });
    (go_tx, done_rx)
}

/// Same rendezvous shape as [`spawn_dtact_reader`], for the `tokio` arm.
#[cfg(windows)]
fn spawn_tokio_reader(
    rt: tokio::runtime::Handle,
    mut server: tokio::net::windows::named_pipe::NamedPipeServer,
    size: usize,
) -> (std_mpsc::Sender<()>, std_mpsc::Receiver<()>) {
    use tokio::io::AsyncReadExt;
    let (go_tx, go_rx) = std_mpsc::channel::<()>();
    let (done_tx, done_rx) = std_mpsc::channel::<()>();
    std::thread::spawn(move || {
        let mut buf = vec![0u8; size];
        while go_rx.recv().is_ok() {
            rt.block_on(server.read_exact(&mut buf)).unwrap();
            if done_tx.send(()).is_err() {
                break;
            }
        }
    });
    (go_tx, done_rx)
}

#[cfg(windows)]
fn bench_named_pipe_roundtrip(c: &mut Criterion) {
    use tokio::io::AsyncWriteExt;

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();

    for size in [64usize, 4096, 65536] {
        let label = if size < 1024 {
            format!("{size}B")
        } else {
            format!("{}KB", size / 1024)
        };
        let mut group = c.benchmark_group(format!("named_pipe_roundtrip ({label})"));
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("dtact-io", &label), &size, |b, &size| {
            let name = format!(r"\\.\pipe\dtact-bench-{}-{}", std::process::id(), size);
            let (server, client) = dtact_pipe_pair(&name);
            let (go, done) = spawn_dtact_reader(server, size);
            let data = vec![0xABu8; size];
            b.iter(|| {
                go.send(()).unwrap();
                block_on(client.write(&data)).unwrap();
                done.recv().unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("tokio", &label), &size, |b, &size| {
            let name = format!(
                r"\\.\pipe\dtact-bench-tokio-{}-{}",
                std::process::id(),
                size
            );
            let (server, mut client) = tokio_pipe_pair(&tokio_rt, &name);
            let (go, done) = spawn_tokio_reader(tokio_rt.handle().clone(), server, size);
            let data = vec![0xABu8; size];
            b.iter(|| {
                go.send(()).unwrap();
                tokio_rt.block_on(client.write_all(&data)).unwrap();
                done.recv().unwrap();
            });
        });

        group.finish();
    }
}

#[cfg(windows)]
criterion_group!(benches, bench_named_pipe_roundtrip);
#[cfg(windows)]
criterion_main!(benches);

#[cfg(not(windows))]
fn main() {}
