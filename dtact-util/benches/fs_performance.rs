//! Criterion bench for the dtact-fs native (thread-pool-bridged) backend:
//! measures round-trip latency of write+read through `DtactFile` against a
//! temp file, for a few payload sizes.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dtact_util::fs::DtactFile;
use std::future::Future;
use std::path::PathBuf;

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

fn bench_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("dtact-fs-bench-{}-{}", std::process::id(), name))
}

fn bench_fs_write_read(c: &mut Criterion) {
    dtact_util::fs::init(4);
    let mut group = c.benchmark_group("dtact_fs_write_read");

    for size in [64usize, 4096, 65536] {
        let path = bench_path(&size.to_string());
        group.bench_with_input(BenchmarkId::new("native", size), &size, |b, &size| {
            let payload = vec![0xABu8; size];
            b.iter(|| {
                block_on(async {
                    let file = DtactFile::create(&path).await.unwrap();
                    let (_, _) = file.write(payload.clone()).await.unwrap();
                    file.sync_all().await.unwrap();
                    drop(file);

                    let file = DtactFile::open(&path).await.unwrap();
                    let buf = vec![0u8; size];
                    let (n, _) = file.read(buf).await.unwrap();
                    assert_eq!(n, size);
                });
            });
        });
        let _ = std::fs::remove_file(&path);
    }

    group.finish();
}

criterion_group!(benches, bench_fs_write_read);
criterion_main!(benches);
