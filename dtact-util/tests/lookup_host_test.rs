//! Exercises `io::lookup_host` on both backends.

#[cfg(feature = "native")]
#[test]
fn native_resolves_loopback() {
    dtact_util::io::init_runtime(1, 128, 1024, 4096, &[]);

    // Minimal single-threaded block_on — this test has no need for
    // `dtact`'s own fiber runtime, only the DNS-resolver thread
    // `lookup_host` itself spawns.
    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        use std::pin::pin;
        use std::sync::Arc;
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

    let addrs: Vec<_> = block_on(dtact_util::io::lookup_host("localhost:80"))
        .expect("localhost:80 must resolve")
        .collect();
    assert!(
        !addrs.is_empty(),
        "localhost must resolve to at least one address"
    );
    assert!(addrs.iter().all(|a| a.ip().is_loopback()));
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
#[tokio::test]
async fn tokio_resolves_loopback() {
    dtact_util::io::init_runtime(1, 0, 0, 0, &[]);
    let addrs: Vec<_> = dtact_util::io::lookup_host("localhost:80")
        .await
        .expect("localhost:80 must resolve")
        .collect();
    assert!(
        !addrs.is_empty(),
        "localhost must resolve to at least one address"
    );
    assert!(addrs.iter().all(|a| a.ip().is_loopback()));
}
