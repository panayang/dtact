//! Exercises the dtact-stream duplex pipe: roundtrip read/write, partial
//! reads across multiple writes, EOF-on-writer-drop, and broken-pipe-on-
//! reader-drop, for both the native (lock-free) and tokio backends.

#[cfg(feature = "native")]
mod native_tests {
    use dtact_util::stream::pair;
    use std::future::Future;

    fn block_on<F: Future>(fut: F) -> F::Output {
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

    #[test]
    fn roundtrip() {
        let (a, b) = pair(16);
        block_on(async {
            a.write_all(b"hello").await.unwrap();
            let mut buf = [0u8; 5];
            let n = b.read(&mut buf).await.unwrap();
            assert_eq!(n, 5);
            assert_eq!(&buf, b"hello");
        });
    }

    #[test]
    fn bidirectional() {
        let (a, b) = pair(16);
        block_on(async {
            a.write_all(b"ping").await.unwrap();
            b.write_all(b"pong").await.unwrap();

            let mut buf = [0u8; 4];
            assert_eq!(b.read(&mut buf).await.unwrap(), 4);
            assert_eq!(&buf, b"ping");
            assert_eq!(a.read(&mut buf).await.unwrap(), 4);
            assert_eq!(&buf, b"pong");
        });
    }

    #[test]
    fn write_larger_than_capacity_splits_across_reads() {
        // Capacity rounds up to a power of two; use 8 explicitly and write
        // more than that in one call so `write_all` must retry internally
        // (backpressure) while the other side drains it via multiple reads.
        let (a, b) = pair(8);
        let payload: Vec<u8> = (0u8..64).collect();
        let payload_clone = payload.clone();

        block_on(async {
            let writer = async {
                a.write_all(&payload_clone).await.unwrap();
            };
            let reader = async {
                let mut received = Vec::new();
                let mut buf = [0u8; 8];
                while received.len() < payload.len() {
                    let n = b.read(&mut buf).await.unwrap();
                    assert!(n > 0, "reader must not observe premature EOF");
                    received.extend_from_slice(&buf[..n]);
                }
                received
            };
            // Poll both concurrently by hand (no executor here): drive a
            // simple round-robin using two boxed futures.
            let mut writer = Box::pin(writer);
            let mut reader = Box::pin(reader);
            let waker = {
                use std::sync::Arc;
                use std::task::Wake;
                struct NoopWaker;
                impl Wake for NoopWaker {
                    fn wake(self: Arc<Self>) {}
                }
                Arc::new(NoopWaker).into()
            };
            let mut cx = std::task::Context::from_waker(&waker);
            let mut writer_done = false;
            let received = loop {
                if !writer_done && writer.as_mut().poll(&mut cx).is_ready() {
                    writer_done = true;
                }
                if let std::task::Poll::Ready(received) = reader.as_mut().poll(&mut cx) {
                    break received;
                }
                std::thread::yield_now();
            };
            assert_eq!(received, payload);
        });
    }

    #[test]
    fn zero_length_write_and_read() {
        let (a, b) = pair(16);
        block_on(async {
            let n = a.write(&[]).await.unwrap();
            assert_eq!(n, 0, "writing an empty buffer must report 0 bytes written");

            // Put a real byte in, then confirm a zero-length read reports
            // 0 without consuming that byte.
            a.write(b"x").await.unwrap();
            let n = b.read(&mut []).await.unwrap();
            assert_eq!(
                n, 0,
                "reading into an empty buffer must report 0 bytes read"
            );

            let mut buf = [0u8; 1];
            let n = b.read(&mut buf).await.unwrap();
            assert_eq!(n, 1);
            assert_eq!(buf, [b'x']);
        });
    }

    #[test]
    fn dropping_a_pending_read_then_writing_still_delivers() {
        // Poll a read on an empty pipe exactly once (registering its waker
        // and returning Pending), then drop it before it ever resolves —
        // simulating a fiber that starts an async read and is cancelled
        // (e.g. via `select!`/timeout) before data arrives. A *fresh* read
        // afterwards must still see data written later, proving the
        // dropped read didn't leave the queue/waker slot in a bad state.
        struct PollOnce<F>(Option<F>);
        impl<F: Future> Future for PollOnce<F> {
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

        let (a, b) = pair(16);
        block_on(async {
            let mut buf = [0u8; 4];
            let pending_read = b.read(&mut buf);
            PollOnce(Some(pending_read)).await;
            // `pending_read` is now dropped without ever completing.

            a.write_all(b"ok").await.unwrap();
            let mut buf2 = [0u8; 4];
            let n = b.read(&mut buf2).await.unwrap();
            assert_eq!(&buf2[..n], b"ok");
        });
    }

    #[test]
    fn eof_on_writer_drop() {
        let (a, b) = pair(16);
        drop(a);
        block_on(async {
            let mut buf = [0u8; 4];
            let n = b.read(&mut buf).await.unwrap();
            assert_eq!(
                n, 0,
                "reading after writer dropped an empty pipe must be EOF"
            );
        });
    }

    #[test]
    fn broken_pipe_on_reader_drop() {
        let (a, b) = pair(16);
        drop(b);
        block_on(async {
            let err = a.write(b"x").await.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        });
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_tests {
    use dtact_util::stream::pair;

    #[tokio::test]
    async fn roundtrip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut a, mut b) = pair(16);
        a.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }
}
