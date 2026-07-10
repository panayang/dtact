//! Criterion bench for dtact-timer vs tokio::time: registration + firing
//! overhead of a single `sleep` for a few durations, run side by side.
//!
//! Run:  cargo bench --bench timer_performance
//! Test: cargo bench --bench timer_performance -- --test

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::time::Duration;

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

fn bench_timer_sleep(c: &mut Criterion) {
    use dtact_util::timer::sleep;
    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("timer_sleep");

    for millis in [1u64, 10, 50] {
        let label = format!("{millis}ms");

        group.bench_with_input(
            BenchmarkId::new("dtact-timer", &label),
            &millis,
            |b, &ms| {
                b.iter(|| {
                    block_on(sleep(Duration::from_millis(ms)));
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("tokio", &label), &millis, |b, &ms| {
            b.iter(|| {
                tokio_rt.block_on(async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_timer_sleep);
criterion_main!(benches);
