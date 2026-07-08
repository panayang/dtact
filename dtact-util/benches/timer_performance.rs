//! Criterion bench for the dtact-timer backends: measures registration +
//! firing overhead of a single `sleep` for a short duration, native vs
//! tokio, mirroring `fs_performance.rs`'s structure.

use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

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
fn bench_timer_native(c: &mut Criterion) {
    use dtact_util::timer::sleep;
    let mut group = c.benchmark_group("dtact_timer_sleep");
    group.bench_function("native_1ms", |b| {
        b.iter(|| {
            block_on(sleep(Duration::from_millis(1)));
        });
    });
    group.finish();
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
fn bench_timer_tokio(c: &mut Criterion) {
    use dtact_util::timer::sleep;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("dtact_timer_sleep");
    group.bench_function("tokio_1ms", |b| {
        b.iter(|| {
            rt.block_on(sleep(Duration::from_millis(1)));
        });
    });
    group.finish();
}

#[cfg(feature = "native")]
criterion_group!(benches, bench_timer_native);
#[cfg(all(feature = "tokio", not(feature = "native")))]
criterion_group!(benches, bench_timer_tokio);

criterion_main!(benches);
