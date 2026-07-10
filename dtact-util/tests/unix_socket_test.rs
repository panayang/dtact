//! Exercises `DtactUnixStream`/`DtactUnixListener` on both backends — the
//! Unix-domain-socket counterpart to `driver_test.rs`'s (native) and
//! `udp_test.rs`'s (tokio `mod tokio_tests` shape) TCP/UDP tests. Unix
//! only, on both backends (no Windows Unix-domain-socket analogue).
#![cfg(unix)]

#[cfg(all(feature = "native", unix))]
mod native_tests {
    use dtact_util::io::{DtactUnixDatagram, DtactUnixListener, DtactUnixStream, init_runtime};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    // See `driver_test.rs`'s identical comment: `Once` makes
    // `init_runtime`'s one-time setup a real barrier across this file's
    // concurrently-run tests.
    static INIT: std::sync::Once = std::sync::Once::new();
    fn ensure_runtime() {
        INIT.call_once(|| {
            init_runtime(2, 128, 1024, 4096, &[]);
        });
    }

    /// A fresh, guaranteed-not-yet-existing socket path for one test —
    /// `bind` fails with `AddrInUse` if the path already exists, so every
    /// test needs its own.
    fn fresh_socket_path(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dtact-uds-test-{tag}-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
    #[test]
    fn test_unix_socket_driver_roundtrip() {
        ensure_runtime();

        let path = fresh_socket_path("roundtrip");
        let listener = UnixListener::bind(&path).unwrap();
        let dtact_listener = DtactUnixListener::from_std(listener).unwrap();

        let server_finished = Arc::new(AtomicU32::new(0));
        let server_finished_clone = server_finished.clone();
        let path_for_server = path.clone();

        dtact::spawn(async move {
            let (stream, _peer) = dtact_listener.accept().await.unwrap();
            let msg = b"hello from dtact-uds server";
            let mut written = 0;
            while written < msg.len() {
                let n = stream.write(&msg[written..]).await.unwrap();
                written += n;
            }
            server_finished_clone.store(1, Ordering::SeqCst);
            let _ = std::fs::remove_file(&path_for_server);
        });

        let client_finished = Arc::new(AtomicU32::new(0));
        let client_finished_clone = client_finished.clone();
        let path_for_client = path.clone();

        dtact::spawn(async move {
            let stream = DtactUnixStream::connect(&path_for_client).await.unwrap();
            let mut buf = [0u8; 100];
            let mut total_read = 0;
            while total_read < 27 {
                let n = stream.read(&mut buf[total_read..]).await.unwrap();
                if n == 0 {
                    break;
                }
                total_read += n;
            }
            assert_eq!(&buf[..27], b"hello from dtact-uds server");
            client_finished_clone.store(1, Ordering::SeqCst);
        });

        for _ in 0..100 {
            if server_finished.load(Ordering::SeqCst) == 1
                && client_finished.load(Ordering::SeqCst) == 1
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        assert_eq!(server_finished.load(Ordering::SeqCst), 1);
        assert_eq!(client_finished.load(Ordering::SeqCst), 1);
    }

    /// Peer credentials must report the current process's own uid/gid —
    /// both ends of a `socketpair`-style local connection are this same
    /// test process.
    #[test]
    fn test_unix_socket_peer_cred() {
        dtact_autostart();
        ensure_runtime();

        let path = fresh_socket_path("peer-cred");
        let listener = UnixListener::bind(&path).unwrap();
        let dtact_listener = DtactUnixListener::from_std(listener).unwrap();

        let done = Arc::new(AtomicU32::new(0));
        let done2 = done.clone();
        let path_for_server = path.clone();
        dtact::spawn(async move {
            let (stream, _peer) = dtact_listener.accept().await.unwrap();
            let cred = stream.peer_cred().unwrap();
            // SAFETY: libc::getuid/getgid take no arguments and never fail.
            assert_eq!(cred.uid(), unsafe { libc::getuid() });
            assert_eq!(cred.gid(), unsafe { libc::getgid() });
            if let Some(pid) = cred.pid() {
                assert_eq!(pid, std::process::id() as i32);
            }
            done2.store(1, Ordering::SeqCst);
            let _ = std::fs::remove_file(&path_for_server);
        });

        dtact::spawn(async move {
            let _client = DtactUnixStream::connect(&path).await.unwrap();
        });

        for _ in 0..100 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_unix_datagram_roundtrip() {
        dtact_autostart();
        ensure_runtime();

        let a_path = fresh_socket_path("dgram-a");
        let b_path = fresh_socket_path("dgram-b");
        let a = DtactUnixDatagram::bind(&a_path).unwrap();
        let b = DtactUnixDatagram::bind(&b_path).unwrap();

        let done = Arc::new(AtomicU32::new(0));
        let done2 = done.clone();
        let b_path_for_task = b_path.clone();
        dtact::spawn(async move {
            let msg = b"hello dtact unix datagram";
            let sent = a.send_to(msg, &b_path_for_task).await.unwrap();
            assert_eq!(sent, msg.len());

            let mut buf = [0u8; 64];
            let (n, from) = b.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], msg);
            assert_eq!(from.as_pathname(), Some(a_path.as_path()));

            let _ = std::fs::remove_file(&a_path);
            let _ = std::fs::remove_file(&b_path_for_task);
            done2.store(1, Ordering::SeqCst);
        });

        for _ in 0..100 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_unix_datagram_connected_send_recv() {
        dtact_autostart();
        ensure_runtime();

        let a_path = fresh_socket_path("dgram-conn-a");
        let b_path = fresh_socket_path("dgram-conn-b");
        let a = DtactUnixDatagram::bind(&a_path).unwrap();
        let b = DtactUnixDatagram::bind(&b_path).unwrap();

        let done = Arc::new(AtomicU32::new(0));
        let done2 = done.clone();
        dtact::spawn(async move {
            a.connect(&b_path).await.unwrap();
            b.connect(&a_path).await.unwrap();

            let msg = b"connected dgram";
            a.send(msg).await.unwrap();
            let mut buf = [0u8; 64];
            let n = b.recv(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], msg);

            let _ = std::fs::remove_file(&a_path);
            let _ = std::fs::remove_file(&b_path);
            done2.store(1, Ordering::SeqCst);
        });

        for _ in 0..100 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    /// Minimal single-threaded block_on — see `driver_test.rs`'s identical
    /// helper for why (panics attribute to the test thread, not a fiber).
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

    /// Connecting to a path nobody is listening on (never bound) must
    /// surface an error rather than hanging or panicking.
    #[test]
    fn test_unix_socket_connect_fails_when_nothing_listening() {
        ensure_runtime();

        let path = fresh_socket_path("connect-refused");
        // Never bound — `path` doesn't exist on the filesystem at all.

        let result = block_on(DtactUnixStream::connect(&path));
        match result {
            Ok(_) => panic!("connecting to a nonexistent socket path must return Err"),
            Err(e) => {
                assert_ne!(
                    e.kind(),
                    std::io::ErrorKind::Other,
                    "unexpected error: {e:?}"
                );
            }
        }
    }

    /// A path that's too long for `sockaddr_un::sun_path` must be
    /// rejected with a clean error, not a truncated/corrupted connect
    /// attempt.
    #[test]
    fn test_unix_socket_path_too_long_is_rejected() {
        ensure_runtime();

        let long_name = "a".repeat(200);
        let path = std::env::temp_dir().join(format!("{long_name}.sock"));

        match block_on(DtactUnixStream::connect(&path)) {
            Ok(_) => {
                panic!("an over-length socket path must be rejected, not silently truncated")
            }
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        }
    }

    /// Zero-length reads/writes must be an immediate no-op success,
    /// matching `DtactTcpStream`'s behavior (`driver_test.rs`'s identical
    /// test) and `std`/`tokio` convention.
    #[test]
    fn test_unix_socket_zero_length_read_write() {
        dtact_autostart();
        ensure_runtime();

        let path = fresh_socket_path("zero-length");
        let listener = UnixListener::bind(&path).unwrap();
        let dtact_listener = DtactUnixListener::from_std(listener).unwrap();

        let done = Arc::new(AtomicU32::new(0));
        let done_clone = done.clone();
        let path_for_server = path.clone();
        dtact::spawn(async move {
            let (stream, _peer) = dtact_listener.accept().await.unwrap();
            let n = stream.write(&[]).await.unwrap();
            assert_eq!(n, 0, "writing an empty buffer must report 0 bytes written");
            let mut buf = [0u8; 8];
            let n = stream.read(&mut buf[..0]).await.unwrap();
            assert_eq!(
                n, 0,
                "reading into an empty buffer must report 0 bytes read"
            );

            let n = stream.write(b"still-alive").await.unwrap();
            assert_eq!(n, 11);
            done_clone.store(1, Ordering::SeqCst);
            let _ = std::fs::remove_file(&path_for_server);
        });

        dtact::spawn(async move {
            let client = DtactUnixStream::connect(&path).await.unwrap();
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
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_tests {
    use dtact_util::io::{
        DtactUnixDatagram, DtactUnixListener, DtactUnixStream, get_runtime_handle, init_runtime,
    };
    use std::path::PathBuf;

    fn fresh_socket_path(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dtact-uds-tokio-test-{tag}-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn unix_socket_roundtrip() {
        init_runtime(2, 0, 0, 0, &[]);
        let path = fresh_socket_path("roundtrip");
        get_runtime_handle().block_on(async {
            let listener = DtactUnixListener::bind(&path).unwrap();

            let server_path = path.clone();
            tokio::spawn(async move {
                let (stream, _peer) = listener.accept().await.unwrap();
                let msg = b"hello from tokio-uds server";
                let mut written = 0;
                while written < msg.len() {
                    written += stream.write(&msg[written..]).await.unwrap();
                }
                let _ = std::fs::remove_file(&server_path);
            });

            // Give the accept loop a moment to be ready.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;

            let client = DtactUnixStream::connect(&path).await.unwrap();
            let mut buf = [0u8; 64];
            let mut total = 0;
            while total < 27 {
                let n = client.read(&mut buf[total..]).await.unwrap();
                if n == 0 {
                    break;
                }
                total += n;
            }
            assert_eq!(&buf[..27], b"hello from tokio-uds server");
        });
    }

    #[test]
    fn unix_datagram_roundtrip() {
        init_runtime(2, 0, 0, 0, &[]);
        let a_path = fresh_socket_path("dgram-a");
        let b_path = fresh_socket_path("dgram-b");
        get_runtime_handle().block_on(async {
            let a = DtactUnixDatagram::bind(&a_path).unwrap();
            let b = DtactUnixDatagram::bind(&b_path).unwrap();

            let msg = b"hello tokio unix datagram";
            let sent = a.send_to(msg, &b_path).await.unwrap();
            assert_eq!(sent, msg.len());

            let mut buf = [0u8; 64];
            let (n, from) = b.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], msg);
            assert_eq!(from.as_pathname(), Some(a_path.as_path()));

            let _ = std::fs::remove_file(&a_path);
            let _ = std::fs::remove_file(&b_path);
        });
    }

    /// Connecting to a path nobody is listening on must surface an error
    /// rather than hanging or panicking.
    #[test]
    fn unix_socket_connect_fails_when_nothing_listening() {
        init_runtime(2, 0, 0, 0, &[]);
        let path = fresh_socket_path("connect-refused");
        get_runtime_handle().block_on(async {
            let result = DtactUnixStream::connect(&path).await;
            assert!(
                result.is_err(),
                "connecting to a nonexistent socket path must return Err"
            );
        });
    }
}
