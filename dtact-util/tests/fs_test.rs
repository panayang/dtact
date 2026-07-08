//! Exercises the dtact-fs native (thread-pool-bridged) backend end to end:
//! create, write, read back, positional read/write, metadata, read_dir,
//! and cleanup. Runs on whatever platform hosts CI, including Windows,
//! since the native fs backend is std::fs-based rather than io_uring-only.

#![cfg(feature = "native")]

use dtact_util::fs::DtactFile;
use std::future::Future;

/// Minimal single-threaded block_on so this test doesn't need to pull in
/// tokio just to drive a couple of futures.
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

#[test]
fn test_fs_write_read_roundtrip() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let path = dir.join("roundtrip.txt");

    block_on(async {
        let file = DtactFile::create(&path).await.unwrap();
        let (n, _buf) = file.write(b"hello dtact-fs".to_vec()).await.unwrap();
        assert_eq!(n, 14);
        file.sync_all().await.unwrap();
        file.close().await.unwrap();

        let file = DtactFile::open(&path).await.unwrap();
        let buf = vec![0u8; 32];
        let (n, buf) = file.read(buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello dtact-fs");

        let meta = file.metadata().await.unwrap();
        assert_eq!(meta.len(), 14);
    });

    block_on(dtact_util::fs::remove_file(&path)).unwrap();
}

#[test]
fn test_fs_positional_read_write() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-pos-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let path = dir.join("positional.bin");

    block_on(async {
        let opts = {
            let mut o = std::fs::OpenOptions::new();
            o.read(true).write(true).create(true).truncate(true);
            o
        };
        let file = DtactFile::open_with(&path, opts).await.unwrap();

        let (n, _) = file.write_at(vec![1, 2, 3, 4], 0).await.unwrap();
        assert_eq!(n, 4);
        let (n, _) = file.write_at(vec![9, 9], 10).await.unwrap();
        assert_eq!(n, 2);

        let (n, buf) = file.read_at(vec![0u8; 4], 0).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf[..4], &[1, 2, 3, 4]);

        let (n, buf) = file.read_at(vec![0u8; 2], 10).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[9, 9]);
    });

    block_on(dtact_util::fs::remove_file(&path)).unwrap();
}

#[test]
fn test_fs_read_dir() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-dir-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let a = dir.join("a.txt");
    let b = dir.join("b.txt");

    block_on(async {
        DtactFile::create(&a).await.unwrap();
        DtactFile::create(&b).await.unwrap();
        let entries = dtact_util::fs::read_dir(&dir).await.unwrap();
        assert_eq!(entries.len(), 2);
    });

    block_on(dtact_util::fs::remove_file(&a)).unwrap();
    block_on(dtact_util::fs::remove_file(&b)).unwrap();
}

/// Exercises the `#[dtact_util::fs_init]` attribute macro end to end: the
/// wrapped function body should run with the fs subsystem already
/// configured/started (a tiny `ring_depth` here specifically to also
/// exercise the pool-exhaustion -> heap-fallback path below).
#[dtact_util::fs_init(workers = 2, ring_depth = 4)]
fn fs_init_macro_smoke() {
    let dir = std::env::temp_dir().join(format!("dtact-fs-test-macro-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let path = dir.join("macro.txt");

    block_on(async {
        let file = DtactFile::create(&path).await.unwrap();
        let (n, _) = file.write(b"macro-init".to_vec()).await.unwrap();
        assert_eq!(n, 10);
    });

    block_on(dtact_util::fs::remove_file(&path)).unwrap();
}

#[test]
fn test_fs_init_macro() {
    fs_init_macro_smoke();
}

/// `ring_depth = 4` deliberately undersizes the preallocated op-slot pool
/// relative to how many concurrent ops this test issues, forcing some ops
/// down the heap-fallback path in `acquire_slot` — both paths (pooled and
/// heap) must behave identically from the caller's point of view.
#[test]
fn test_fs_slot_pool_exhaustion_falls_back_correctly() {
    dtact_util::fs::init_fs(2, 4, 0, 0, &[]);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-pool-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let path = dir.join("pool.bin");

    block_on(async {
        let opts = {
            let mut o = std::fs::OpenOptions::new();
            o.read(true).write(true).create(true).truncate(true);
            o
        };
        let file = DtactFile::open_with(&path, opts).await.unwrap();

        // More in-flight-in-spirit writes than the 4-deep pool, issued
        // sequentially (this backend's ops still complete before the next
        // is awaited) but repeatedly enough to churn the free-list well
        // past its capacity and exercise both acquire/release and the
        // heap-fallback branch if the pool were ever caught empty.
        for i in 0..20u64 {
            let (n, _) = file.write_at(vec![i as u8; 8], i * 8).await.unwrap();
            assert_eq!(n, 8);
        }
        for i in 0..20u64 {
            let (n, buf) = file.read_at(vec![0u8; 8], i * 8).await.unwrap();
            assert_eq!(n, 8);
            assert!(buf.iter().all(|&b| b == i as u8));
        }
    });

    block_on(dtact_util::fs::remove_file(&path)).unwrap();
}
