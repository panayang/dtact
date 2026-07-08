//! Criterion bench for dtact-fs vs tokio::fs: round-trip latency of
//! write+sync+read through a temp file, for a few payload sizes.
//!
//! Run:  cargo bench --bench fs_performance
//! Test: cargo bench --bench fs_performance -- --test

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
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
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for size in [64usize, 4096, 65536, 1_048_576] {
        let label = if size < 1024 {
            format!("{size}B")
        } else {
            format!("{}KB", size / 1024)
        };
        let mut group = c.benchmark_group(format!("fs_write_read ({label})"));
        group.throughput(Throughput::Bytes(size as u64));

        // ── dtact-fs (native thread-pool-bridged backend) ──────────────────────
        {
            let path = bench_path(&format!("native-{size}"));
            group.bench_with_input(BenchmarkId::new("dtact-fs", &label), &size, |b, &size| {
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

        // ── tokio::fs ────────────────────────────────────────────────────────
        {
            let path = bench_path(&format!("tokio-{size}"));
            group.bench_with_input(BenchmarkId::new("tokio", &label), &size, |b, &size| {
                let payload = vec![0xABu8; size];
                b.iter(|| {
                    tokio_rt.block_on(async {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut file = tokio::fs::File::create(&path).await.unwrap();
                        file.write_all(&payload).await.unwrap();
                        file.sync_all().await.unwrap();
                        drop(file);

                        let mut file = tokio::fs::File::open(&path).await.unwrap();
                        let mut buf = vec![0u8; size];
                        file.read_exact(&mut buf).await.unwrap();
                    });
                });
            });
            let _ = std::fs::remove_file(&path);
        }

        group.finish();
    }
}

criterion_group!(benches, bench_fs_write_read);
criterion_main!(benches);
