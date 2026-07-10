//! Comprehensive `dtact-util` example, run as fibers under the `dtact`
//! coroutine scheduler (the `native` backend — a `tokio`-backed build of
//! `dtact-util` would instead be driven from a `#[tokio::main]`, not this
//! macro; see `dtact-util`'s crate doc for the two-backend split).
//!
//! Combines four of `dtact-util`'s six primitive modules in one run:
//! - `timer`: a bounded sleep
//! - `fs`: write-then-read-back a temp file
//! - `stream`: an in-process duplex pipe between two fibers
//! - `io`: a loopback TCP echo between a server and client fiber
//!
//! Run with `cargo run --example rust_dtact_util` (needs `dtact-util`'s
//! default `native` feature, already on via the workspace dev-dependency).

use dtact::{dtact_await, dtact_init};
use dtact_util::io::{DtactTcpListener, DtactTcpStream, init as io_init};
use dtact_util::stream;
use dtact_util::{fs, timer};
use std::time::Duration;

#[dtact_init(workers = 4, stack = "256K", capacity = "1024")]
fn main() {
    println!("--- dtact-util comprehensive example (native backend) ---");

    // `io`/`fs` each own a small dedicated worker-thread pool, independent
    // of the dta_scheduler fiber workers above; start both once up front.
    io_init(2);
    fs::init(1);

    let timer_handle = dtact::spawn(async move {
        println!("[timer] sleeping 20ms...");
        timer::sleep(Duration::from_millis(20)).await;
        println!("[timer] awake.");
    });

    let fs_handle = dtact::spawn(async move {
        let dir = std::env::temp_dir().join(format!("dtact-util-example-{}", std::process::id()));
        fs::create_dir_all(&dir)
            .await
            .expect("create example temp dir");
        let path = dir.join("hello.txt");

        let file = fs::DtactFile::create(&path)
            .await
            .expect("create temp file");
        let (n, _buf) = file
            .write(b"hello from dtact-util fs".to_vec())
            .await
            .expect("write temp file");
        println!("[fs] wrote {n} bytes to {}", path.display());
        file.sync_all().await.expect("fsync temp file");
        drop(file);

        let file = fs::DtactFile::open(&path).await.expect("reopen temp file");
        let (n, buf) = file.read(vec![0u8; 64]).await.expect("read temp file");
        println!("[fs] read back: {:?}", String::from_utf8_lossy(&buf[..n]));

        let _ = fs::remove_file(&path).await;
    });

    let stream_handle = dtact::spawn(async move {
        let (a, b) = stream::pair(64);
        let msg = b"ping over dtact-util stream";
        let written = a.write(msg).await.expect("write to stream pair");
        println!("[stream] wrote {written} bytes into the pipe");

        let mut buf = vec![0u8; msg.len()];
        let read = b.read(&mut buf).await.expect("read from stream pair");
        println!(
            "[stream] read back {read} bytes: {:?}",
            String::from_utf8_lossy(&buf[..read])
        );
    });

    let io_handle = dtact::spawn(async move {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind std listener");
        let addr = std_listener.local_addr().expect("read local addr");
        let listener = DtactTcpListener::from_std(std_listener).expect("adopt std listener");

        // Server side: accept once, echo whatever it receives.
        let server = dtact::spawn(async move {
            let (stream, peer) = listener.accept().await.expect("accept connection");
            println!("[io] server accepted connection from {peer}");
            let mut buf = [0u8; 32];
            let n = stream.read(&mut buf).await.expect("server read");
            let n2 = stream.write(&buf[..n]).await.expect("server echo write");
            println!("[io] server echoed {n2} bytes back");
        });

        // Client side: connect, send a message, read the echo.
        let client = DtactTcpStream::connect(addr).await.expect("client connect");
        let msg = b"ping over dtact-util io";
        client.write(msg).await.expect("client write");
        let mut buf = [0u8; 32];
        let n = client.read(&mut buf).await.expect("client read echo");
        println!(
            "[io] client received echo: {:?}",
            String::from_utf8_lossy(&buf[..n])
        );

        dtact_await(server);
    });

    for (name, handle) in [
        ("timer", timer_handle),
        ("fs", fs_handle),
        ("stream", stream_handle),
        ("io", io_handle),
    ] {
        dtact_await(handle);
        println!("[master] {name} fiber joined.");
    }

    println!("--- all dtact-util primitives exercised successfully ---");
}
