#![cfg(all(feature = "native", windows))]

use dtact_io::{DtactTcpListener, DtactTcpStream, init_runtime, shutdown_runtime};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[dtact::dtact_init(workers = 2, capacity = 2048, safety = "Safety1")]
#[test]
fn test_drop_pending_read_does_not_corrupt() {
    init_runtime(2, 1024, 4096, &[], 128);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = listener.local_addr().unwrap();
    let dtact_listener = DtactTcpListener::from_std(listener).unwrap();

    let server_done = Arc::new(AtomicU32::new(0));
    let server_done_clone = server_done.clone();

    dtact::spawn(async move {
        let (stream, _addr) = dtact_listener.accept().await.expect("accept failed");
        let mut buf = [0u8; 64];
        // No data will ever arrive on this connection — start a read, then
        // immediately drop the future (by racing it against a future that
        // resolves first) to exercise the cancel_queue path instead of
        // letting the op complete normally.
        let read_fut = stream.read(&mut buf);
        futures_lite_select(read_fut).await;
        // Give the io-worker a moment to process the cancellation, then do a
        // second, unrelated read that must still work normally afterwards —
        // proving the slot got recycled correctly and nothing corrupted.
        drop(stream);
        server_done_clone.store(1, Ordering::SeqCst);
    });

    let client_done = Arc::new(AtomicU32::new(0));
    let client_done_clone = client_done.clone();
    dtact::spawn(async move {
        let stream = std::net::TcpStream::connect(local_addr).expect("connect failed");
        // Keep the connection open but never send anything, then close it —
        // the server's dropped pending read should have been cancelled
        // cleanly rather than corrupting the slot it used.
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(stream);
        client_done_clone.store(1, Ordering::SeqCst);
    });

    for _ in 0..100 {
        if server_done.load(Ordering::SeqCst) == 1 && client_done.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(server_done.load(Ordering::SeqCst), 1);
    assert_eq!(client_done.load(Ordering::SeqCst), 1);

    // Exercise the recycled slot pool once more after the cancellation to
    // make sure nothing was left in a bad state.
    let listener2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let dtact_listener2 = DtactTcpListener::from_std(listener2).unwrap();
    let done2 = Arc::new(AtomicU32::new(0));
    let done2_clone = done2.clone();
    dtact::spawn(async move {
        let (stream, _) = dtact_listener2.accept().await.expect("accept2 failed");
        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).await.expect("read2 failed");
        assert_eq!(&buf[..n], b"ping");
        done2_clone.store(1, Ordering::SeqCst);
    });
    dtact::spawn(async move {
        let client = DtactTcpStream::connect(addr2)
            .await
            .expect("connect2 failed");
        client.write(b"ping").await.expect("write2 failed");
    });
    for _ in 0..100 {
        if done2.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(done2.load(Ordering::SeqCst), 1);

    shutdown_runtime();
}

/// Poll `fut` exactly once (issuing the op / registering it) then drop it —
/// simulating a fiber that starts an I/O op and is cancelled before it
/// resolves, without pulling in an extra crate dependency just for `select`.
async fn futures_lite_select<F: std::future::Future>(fut: F) {
    struct PollOnce<F>(Option<F>);
    impl<F: std::future::Future> std::future::Future for PollOnce<F> {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            let this = unsafe { self.get_unchecked_mut() };
            if let Some(fut) = this.0.as_mut() {
                let fut = unsafe { std::pin::Pin::new_unchecked(fut) };
                let _ = fut.poll(cx);
            }
            this.0 = None;
            std::task::Poll::Ready(())
        }
    }
    PollOnce(Some(fut)).await
}
