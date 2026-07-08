//! Criterion bench for the dtact-stream duplex pipe: measures throughput
//! of small-write/read roundtrips, native vs tokio, mirroring
//! `fs_performance.rs`/`timer_performance.rs`'s structure.

use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(feature = "native")]
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

#[cfg(feature = "native")]
fn bench_stream_native(c: &mut Criterion) {
    use dtact_util::stream::pair;
    let mut group = c.benchmark_group("dtact_stream_roundtrip");
    for size in [64usize, 4096] {
        group.bench_function(format!("native_{size}B"), |b| {
            let (a, r) = pair(65536);
            let data = vec![0xAB; size];
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
        });
    }
    group.finish();
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
fn bench_stream_tokio(c: &mut Criterion) {
    use dtact_util::stream::pair;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("dtact_stream_roundtrip");
    for size in [64usize, 4096] {
        group.bench_function(format!("tokio_{size}B"), |b| {
            let (mut a, mut r) = pair(65536);
            let data = vec![0xABu8; size];
            b.iter(|| {
                rt.block_on(async {
                    a.write_all(&data).await.unwrap();
                    let mut buf = vec![0u8; size];
                    r.read_exact(&mut buf).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

#[cfg(feature = "native")]
criterion_group!(benches, bench_stream_native);
#[cfg(all(feature = "tokio", not(feature = "native")))]
criterion_group!(benches, bench_stream_tokio);
#[cfg(not(any(feature = "native", feature = "tokio")))]
criterion_group!(benches,);

criterion_main!(benches);
