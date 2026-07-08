//! Criterion bench for the dtact-process backends: spawn+wait latency of
//! a trivial child process, native vs tokio.

use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(windows)]
fn shell_cmd() -> (&'static str, Vec<&'static str>) {
    ("cmd", vec!["/C", "exit 0"])
}
#[cfg(unix)]
fn shell_cmd() -> (&'static str, Vec<&'static str>) {
    ("sh", vec!["-c", "exit 0"])
}

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
fn bench_process_native(c: &mut Criterion) {
    use dtact_util::process::DtactCommand;
    dtact_util::process::init(4);
    let (prog, args) = shell_cmd();
    let mut group = c.benchmark_group("dtact_process_spawn_wait");
    group.bench_function("native", |b| {
        b.iter(|| {
            let mut cmd = DtactCommand::new(prog);
            cmd.args(args.iter().copied());
            let child = cmd.spawn().unwrap();
            block_on(child.wait()).unwrap();
        });
    });
    group.finish();
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
fn bench_process_tokio(c: &mut Criterion) {
    use dtact_util::process::DtactCommand;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (prog, args) = shell_cmd();
    let mut group = c.benchmark_group("dtact_process_spawn_wait");
    group.bench_function("tokio", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut cmd = DtactCommand::new(prog);
                cmd.args(args.iter().copied());
                let mut child = cmd.spawn().unwrap();
                child.wait().await.unwrap();
            });
        });
    });
    group.finish();
}

#[cfg(feature = "native")]
criterion_group!(benches, bench_process_native);
#[cfg(all(feature = "tokio", not(feature = "native")))]
criterion_group!(benches, bench_process_tokio);
#[cfg(not(any(feature = "native", feature = "tokio")))]
criterion_group!(benches,);

criterion_main!(benches);
