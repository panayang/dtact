//! Criterion bench for dtact-stream's duplex pipe vs tokio::io::duplex:
//! throughput of small-write/read roundtrips, run side by side.
//!
//! Run:  cargo bench --bench stream_performance
//! Test: cargo bench --bench stream_performance -- --test

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

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

fn bench_stream_roundtrip(c: &mut Criterion) {
    use dtact_util::stream::pair;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();

    for size in [64usize, 4096, 65536] {
        let label = if size < 1024 {
            format!("{size}B")
        } else {
            format!("{}KB", size / 1024)
        };
        let mut group = c.benchmark_group(format!("stream_roundtrip ({label})"));
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("dtact-stream", &label),
            &size,
            |b, &size| {
                let (a, r) = pair(65536);
                let data = vec![0xABu8; size];
                b.iter(|| {
                    block_on(async {
                        a.write_all(&data).await.unwrap();
                        let mut buf = vec![0u8; size];
                        let mut got = 0;
                        while got < size {
                            got += r.read(&mut buf[got..]).await.unwrap();
                        }
                    });
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("tokio", &label), &size, |b, &size| {
            let (mut a, mut r) = tokio::io::duplex(65536);
            let data = vec![0xABu8; size];
            b.iter(|| {
                tokio_rt.block_on(async {
                    a.write_all(&data).await.unwrap();
                    let mut buf = vec![0u8; size];
                    r.read_exact(&mut buf).await.unwrap();
                });
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_stream_roundtrip);
criterion_main!(benches);
