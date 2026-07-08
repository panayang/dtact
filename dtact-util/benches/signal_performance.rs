//! Criterion bench for the dtact-signal backends: per-poll overhead of an
//! already-registered listener that has nothing pending (the common
//! "idle, waiting" case). Deliberately does *not* bench repeated
//! register+drop cycles: `native`'s `ListenerRegistry` intentionally
//! leaks a fixed-capacity slot per registration for the process's
//! lifetime (see `signal::registry`'s module doc) — looping registration
//! thousands of times, which is what Criterion's `b.iter` would do, would
//! just hit that capacity limit and panic. One registration up front,
//! many polls against it, is both what this bench needs and what's safe.

use criterion::{Criterion, criterion_group, criterion_main};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Wake};

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn poll_once<F: Future>(fut: F) {
    let waker = Arc::new(NoopWaker).into();
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    let _ = fut.as_mut().poll(&mut cx);
}

#[cfg(all(feature = "native", unix))]
fn bench_signal_native(c: &mut Criterion) {
    use dtact_util::signal::sigusr2;
    let stream = sigusr2(); // registered once, outside the timed loop
    let mut group = c.benchmark_group("dtact_signal_poll_idle");
    group.bench_function("native_poll_recv", |b| {
        b.iter(|| poll_once(stream.recv()));
    });
    group.finish();
}

#[cfg(all(feature = "native", windows))]
fn bench_signal_native(c: &mut Criterion) {
    use dtact_util::signal::ctrl_c;
    let stream = ctrl_c(); // registered once, outside the timed loop
    let mut group = c.benchmark_group("dtact_signal_poll_idle");
    group.bench_function("native_poll_recv", |b| {
        b.iter(|| poll_once(stream.recv()));
    });
    group.finish();
}

#[cfg(all(feature = "tokio", not(feature = "native"), unix))]
fn bench_signal_tokio(c: &mut Criterion) {
    use dtact_util::signal::sigusr2;
    let mut stream = sigusr2();
    let mut group = c.benchmark_group("dtact_signal_poll_idle");
    group.bench_function("tokio_poll_recv", |b| {
        b.iter(|| poll_once(stream.recv()));
    });
    group.finish();
}

#[cfg(all(feature = "tokio", not(feature = "native"), windows))]
fn bench_signal_tokio(c: &mut Criterion) {
    use dtact_util::signal::ctrl_c;
    let mut stream = ctrl_c();
    let mut group = c.benchmark_group("dtact_signal_poll_idle");
    group.bench_function("tokio_poll_recv", |b| {
        b.iter(|| poll_once(stream.recv()));
    });
    group.finish();
}

#[cfg(feature = "native")]
criterion_group!(benches, bench_signal_native);
#[cfg(all(feature = "tokio", not(feature = "native")))]
criterion_group!(benches, bench_signal_tokio);
#[cfg(not(any(feature = "native", feature = "tokio")))]
criterion_group!(benches,);

criterion_main!(benches);
