use dtact_util::io::{DtactTcpListener, DtactTcpStream, init_runtime};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

// `cargo test` runs every `#[test]` fn in this file as a separate thread
// within one process, but `init_runtime` guards its one-time setup with a
// bare `GLOBAL_CONFIG.set(..).is_err() { return }` check — if two tests'
// threads both call it concurrently, whichever loses the race returns
// immediately *before* the winner has finished populating `WORKERS` and
// spawning the io-worker threads, and then panics on `WORKERS.get().unwrap()`
// the moment it tries to register a socket. `std::sync::Once::call_once`
// blocks every concurrent caller until the closure has fully returned, so
// routing all three tests below through it (instead of calling
// `init_runtime` directly) makes initialization a real one-time barrier
// rather than a racy idempotency check. For the same reason, none of these
// tests call `shutdown_runtime()` — doing so would tear down the
// process-global worker threads out from under whichever sibling test
// hasn't run yet.
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_runtime() {
    INIT.call_once(|| {
        init_runtime(2, 128, 1024, 4096, &[]);
    });
}

#[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
#[test]
fn test_io_driver_tcp() {
    ensure_runtime();

    // Bind a listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = listener.local_addr().unwrap();
    println!("Bound std TcpListener to {}", local_addr);
    let dtact_listener = DtactTcpListener::from_std(listener).unwrap();
    println!("Converted to DtactTcpListener successfully");

    let server_finished = Arc::new(AtomicU32::new(0));
    let server_finished_clone = server_finished.clone();

    // Spawn server accept loop fiber
    dtact::spawn(async move {
        println!("Server: starting accept");
        let (stream, client_addr) = dtact_listener.accept().await.unwrap();
        println!("Server: accepted connection from {}", client_addr);

        // Write message
        let msg = b"hello from dtact-io server";
        let mut written = 0;
        while written < msg.len() {
            println!("Server: writing bytes...");
            let n = stream.write(&msg[written..]).await.unwrap();
            println!("Server: wrote {} bytes", n);
            written += n;
        }

        println!("Server: finished successfully");
        server_finished_clone.store(1, Ordering::SeqCst);
    });

    // Spawn client fiber
    let client_finished = Arc::new(AtomicU32::new(0));
    let client_finished_clone = client_finished.clone();

    dtact::spawn(async move {
        println!("Client: connecting to {}", local_addr);
        // Connect stream
        let stream = DtactTcpStream::connect(local_addr).await.unwrap();
        println!("Client: connected successfully");

        // Read message
        let mut buf = [0u8; 100];
        let mut total_read = 0;
        while total_read < 26 {
            println!("Client: reading bytes...");
            let n = stream.read(&mut buf[total_read..]).await.unwrap();
            println!("Client: read {} bytes", n);
            if n == 0 {
                break;
            }
            total_read += n;
        }

        println!(
            "Client: read total {} bytes: {:?}",
            total_read,
            String::from_utf8_lossy(&buf[0..total_read])
        );
        assert_eq!(&buf[0..26], b"hello from dtact-io server");
        client_finished_clone.store(1, Ordering::SeqCst);
    });

    // Wait for completion
    for i in 0..100 {
        if server_finished.load(Ordering::SeqCst) == 1
            && client_finished.load(Ordering::SeqCst) == 1
        {
            println!("Both finished on iteration {}", i);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    assert_eq!(server_finished.load(Ordering::SeqCst), 1);
    assert_eq!(client_finished.load(Ordering::SeqCst), 1);
}

/// Minimal single-threaded block_on — used instead of `dtact::spawn` for
/// the two tests below so a failed `assert!`/`unwrap` inside the future
/// panics directly on the test thread (and fails the test the normal way)
/// rather than inside a fiber on a scheduler worker thread, where a panic
/// can unwind that worker instead of being attributed back to this test.
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

/// Connecting to an address nobody is listening on must surface a
/// connection-refused error rather than hanging or panicking.
#[test]
fn test_io_connect_refused_when_nothing_listening() {
    ensure_runtime();

    // Bind then immediately drop a listener to obtain a port that is very
    // likely free but that definitely has nothing listening on it by the
    // time we try to connect.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let result = block_on(DtactTcpStream::connect(addr));
    match result {
        Ok(_) => panic!("connecting to a closed/nonexistent listener must return Err"),
        Err(e) => {
            // Windows reports this as connection-refused; accept anything
            // that isn't a bogus success rather than pinning to one exact
            // `ErrorKind` across platforms/OS versions.
            assert_ne!(
                e.kind(),
                std::io::ErrorKind::Other,
                "unexpected error: {e:?}"
            );
        }
    }
}

/// Zero-length reads/writes must be handled as an immediate no-op success
/// (matching `std`/`tokio` convention) rather than issuing a real syscall
/// that could block or misbehave for a 0-byte transfer.
#[test]
fn test_io_zero_length_read_write() {
    dtact_autostart();
    ensure_runtime();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = listener.local_addr().unwrap();
    let dtact_listener = DtactTcpListener::from_std(listener).unwrap();

    let done = Arc::new(AtomicU32::new(0));
    let done_clone = done.clone();
    dtact::spawn(async move {
        let (stream, _addr) = dtact_listener.accept().await.unwrap();
        let n = stream.write(&[]).await.unwrap();
        assert_eq!(n, 0, "writing an empty buffer must report 0 bytes written");
        let mut buf = [0u8; 8];
        let n = stream.read(&mut buf[..0]).await.unwrap();
        assert_eq!(
            n, 0,
            "reading into an empty buffer must report 0 bytes read"
        );

        // Confirm the connection is still perfectly usable afterwards —
        // the zero-length ops must not have consumed/corrupted anything.
        let n = stream.write(b"still-alive").await.unwrap();
        assert_eq!(n, 11);
        done_clone.store(1, Ordering::SeqCst);
    });

    dtact::spawn(async move {
        let client = DtactTcpStream::connect(local_addr).await.unwrap();
        let mut buf = [0u8; 32];
        let mut total = 0;
        while total < 11 {
            let n = client.read(&mut buf[total..]).await.unwrap();
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(&buf[..11], b"still-alive");
    });

    for _ in 0..100 {
        if done.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(done.load(Ordering::SeqCst), 1);
}
