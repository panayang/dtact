//! Exercises `DtactNamedPipeServer`/`DtactNamedPipeClient` on both
//! backends. Windows-only, matching the types themselves.
#![cfg(windows)]

fn pipe_name(tag: &str) -> String {
    format!(
        r"\\.\pipe\dtact-util-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[cfg(feature = "native")]
mod native_tests {
    use super::pipe_name;
    use dtact_util::io::{DtactNamedPipeClient, DtactNamedPipeServer};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
    #[test]
    fn named_pipe_roundtrip() {
        let name = pipe_name("roundtrip");
        let server = DtactNamedPipeServer::create(&name).expect("create server pipe instance");

        let server_done = Arc::new(AtomicU32::new(0));
        let server_done2 = server_done.clone();
        dtact::spawn(async move {
            let stream = server.connect().await.expect("accept client connection");
            let msg = b"hello from dtact-util named pipe server";
            let mut written = 0;
            while written < msg.len() {
                written += stream.write(&msg[written..]).await.unwrap();
            }
            server_done2.store(1, Ordering::SeqCst);
        });

        let client_done = Arc::new(AtomicU32::new(0));
        let client_done2 = client_done.clone();
        let name_for_client = name.clone();
        dtact::spawn(async move {
            let stream = DtactNamedPipeClient::connect(&name_for_client)
                .await
                .expect("client connect");
            let expected = b"hello from dtact-util named pipe server";
            let mut buf = [0u8; 128];
            let mut total = 0;
            while total < expected.len() {
                let n = stream.read(&mut buf[total..]).await.unwrap();
                assert_ne!(n, 0, "server closed before sending everything");
                total += n;
            }
            assert_eq!(&buf[..expected.len()], expected);
            client_done2.store(1, Ordering::SeqCst);
        });

        for _ in 0..100 {
            if server_done.load(Ordering::SeqCst) == 1 && client_done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(server_done.load(Ordering::SeqCst), 1);
        assert_eq!(client_done.load(Ordering::SeqCst), 1);
    }

    /// Connecting to a name nobody created a server instance for must
    /// surface an error, not hang.
    #[test]
    fn connect_fails_when_no_server() {
        dtact_autostart();
        let name = pipe_name("no-server");

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

        let result = block_on(DtactNamedPipeClient::connect(&name));
        assert!(
            result.is_err(),
            "connecting to a pipe name with no server instance must return Err"
        );
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_tests {
    use super::pipe_name;
    use dtact_util::io::{
        DtactNamedPipeClient, DtactNamedPipeServer, get_runtime_handle, init_runtime,
    };

    #[test]
    fn named_pipe_roundtrip() {
        init_runtime(2, 0, 0, 0, &[]);
        get_runtime_handle().block_on(async {
            let name = pipe_name("roundtrip");
            let server = DtactNamedPipeServer::create(&name).expect("create server pipe instance");

            let name_for_client = name.clone();
            let client_task = tokio::spawn(async move {
                let stream = DtactNamedPipeClient::connect(&name_for_client)
                    .await
                    .expect("client connect");
                let expected = b"hello from dtact-util named pipe server";
                let mut buf = [0u8; 128];
                let mut total = 0;
                while total < expected.len() {
                    let n = stream.read(&mut buf[total..]).await.unwrap();
                    assert_ne!(n, 0, "server closed before sending everything");
                    total += n;
                }
                assert_eq!(&buf[..expected.len()], expected);
            });

            let stream = server.connect().await.expect("accept client connection");
            let msg = b"hello from dtact-util named pipe server";
            let mut written = 0;
            while written < msg.len() {
                written += stream.write(&msg[written..]).await.unwrap();
            }

            client_task.await.unwrap();
        });
    }

    #[test]
    fn connect_fails_when_no_server() {
        init_runtime(2, 0, 0, 0, &[]);
        get_runtime_handle().block_on(async {
            let name = pipe_name("no-server");
            let result = DtactNamedPipeClient::connect(&name).await;
            assert!(
                result.is_err(),
                "connecting to a pipe name with no server instance must return Err"
            );
        });
    }
}
