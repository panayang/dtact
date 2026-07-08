//! Criterion bench for dtact-process vs tokio::process: spawn+wait latency
//! of a trivial child process, run side by side in the same report so the
//! numbers are directly comparable (not one-or-the-other depending on
//! which feature happened to be enabled).
//!
//! Run:  cargo bench --bench process_performance
//! Test: cargo bench --bench process_performance -- --test

use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(windows)]
fn shell_cmd() -> (&'static str, Vec<&'static str>) {
    ("cmd", vec!["/C", "exit 0"])
}
#[cfg(unix)]
fn shell_cmd() -> (&'static str, Vec<&'static str>) {
    ("sh", vec!["-c", "exit 0"])
}

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

fn bench_process_spawn_wait(c: &mut Criterion) {
    dtact_util::process::init(4);
    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let (prog, args) = shell_cmd();
    let mut group = c.benchmark_group("process_spawn_wait");

    group.bench_function("dtact-process", |b| {
        use dtact_util::process::DtactCommand;
        b.iter(|| {
            let mut cmd = DtactCommand::new(prog);
            cmd.args(args.iter().copied());
            let child = cmd.spawn().unwrap();
            block_on(child.wait()).unwrap();
        });
    });

    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_rt.block_on(async {
                let mut child = tokio::process::Command::new(prog)
                    .args(args.iter().copied())
                    .spawn()
                    .unwrap();
                child.wait().await.unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_process_spawn_wait);
criterion_main!(benches);
