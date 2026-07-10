//! Criterion bench for dtact-signal vs tokio::signal: per-poll overhead of
//! an already-registered listener that has nothing pending (the common
//! "idle, waiting" case), run side by side.
//!
//! Deliberately does *not* bench repeated register+drop cycles: `native`'s
//! `ListenerRegistry` intentionally leaks a fixed-capacity slot per
//! registration for the process's lifetime (see `signal::registry`'s
//! module doc) — looping registration thousands of times, which is what
//! Criterion's `b.iter` would do, would just hit that capacity limit and
//! panic. One registration up front, many polls against it, is both what
//! this bench needs and what's safe.
//!
//! Run:  cargo bench --bench signal_performance
//! Test: cargo bench --bench signal_performance -- --test

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

#[cfg(unix)]
fn bench_signal_poll_idle(c: &mut Criterion) {
    use dtact_util::signal::sigusr2;
    let _rt_guard = tokio::runtime::Runtime::new().unwrap();
    let _enter = _rt_guard.enter();

    let dtact_stream = sigusr2(); // registered once, outside the timed loop
    let mut tokio_stream =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2()).unwrap();

    let mut group = c.benchmark_group("signal_poll_idle");
    group.bench_function("dtact-signal", |b| {
        b.iter(|| poll_once(dtact_stream.recv()));
    });
    group.bench_function("tokio", |b| {
        b.iter(|| poll_once(tokio_stream.recv()));
    });
    group.finish();
}

#[cfg(windows)]
fn bench_signal_poll_idle(c: &mut Criterion) {
    use dtact_util::signal::ctrl_c;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _enter = rt.enter();

    let dtact_stream = ctrl_c(); // registered once, outside the timed loop
    let mut tokio_stream = tokio::signal::windows::ctrl_c().unwrap();

    let mut group = c.benchmark_group("signal_poll_idle");
    group.bench_function("dtact-signal", |b| {
        b.iter(|| poll_once(dtact_stream.recv()));
    });
    group.bench_function("tokio", |b| {
        b.iter(|| poll_once(tokio_stream.recv()));
    });
    group.finish();
}

criterion_group!(benches, bench_signal_poll_idle);
criterion_main!(benches);
