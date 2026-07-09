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

/// Opening a file that does not exist must surface an error rather than
/// panicking or hanging — the most basic failure mode for `DtactFile::open`.
#[test]
fn test_fs_open_nonexistent_file_errors() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-missing-{}", std::process::id()));
    // Deliberately do NOT create `dir`/the file inside it.
    let path = dir.join("does-not-exist.txt");

    let result = block_on(DtactFile::open(&path));
    match result {
        Ok(_) => panic!("opening a nonexistent file must return Err"),
        Err(e) => assert_eq!(
            e.kind(),
            std::io::ErrorKind::NotFound,
            "expected NotFound for a missing path, got {e:?}"
        ),
    }
}

/// Reading again after already having consumed the whole file must report
/// EOF (`Ok(0)`), not an error and not the same bytes twice.
#[test]
fn test_fs_read_past_eof_returns_zero() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-eof-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let path = dir.join("eof.txt");

    block_on(async {
        let file = DtactFile::create(&path).await.unwrap();
        file.write(b"abc".to_vec()).await.unwrap();
        file.close().await.unwrap();

        let file = DtactFile::open(&path).await.unwrap();
        let (n, _buf) = file.read(vec![0u8; 32]).await.unwrap();
        assert_eq!(n, 3, "first read should return the full 3-byte file");

        // The shared cursor is now past the end of the file — reading
        // again must report EOF (0 bytes), not error or repeat data.
        let (n, _buf) = file.read(vec![0u8; 32]).await.unwrap();
        assert_eq!(n, 0, "reading past EOF must return Ok(0)");
    });

    block_on(dtact_util::fs::remove_file(&path)).unwrap();
}

/// Zero-length reads/writes are a common edge case (e.g. a caller handing
/// an empty buffer through generic code) and must be handled as a trivial
/// no-op success rather than erroring or blocking.
#[test]
fn test_fs_zero_length_read_write() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-zero-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();
    let path = dir.join("zero.bin");

    block_on(async {
        let file = DtactFile::create(&path).await.unwrap();
        let (n, buf) = file.write(Vec::new()).await.unwrap();
        assert_eq!(n, 0, "writing an empty buffer must report 0 bytes written");
        assert!(buf.is_empty());

        // Put some real content in via write_at, then confirm a
        // zero-length read reports 0 bytes without disturbing anything.
        file.write_at(b"data".to_vec(), 0).await.unwrap();
        let (n, buf) = file.read_at(Vec::new(), 0).await.unwrap();
        assert_eq!(
            n, 0,
            "reading into an empty buffer must report 0 bytes read"
        );
        assert!(buf.is_empty());
    });

    block_on(dtact_util::fs::remove_file(&path)).unwrap();
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

/// Exercises the free functions added alongside `metadata`/`read_dir`/
/// `create_dir_all`/`remove_file`: `write`/`read`/`read_to_string`,
/// `copy`, `rename`, `create_dir`/`remove_dir`/`remove_dir_all`,
/// `hard_link`, `canonicalize`, `try_exists`, `set_permissions`, and
/// `symlink_metadata`. Unix-only `symlink`/Windows-only
/// `symlink_dir`/`symlink_file` are covered separately below (creating
/// symlinks needs elevated privileges/Developer Mode on Windows, so that
/// one's allowed to fail there rather than asserted unconditionally).
#[test]
fn test_fs_free_functions() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-free-fns-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();

    block_on(async {
        // write / read / read_to_string
        let path = dir.join("hello.txt");
        dtact_util::fs::write(&path, b"hello dtact-fs free functions")
            .await
            .unwrap();
        let bytes = dtact_util::fs::read(&path).await.unwrap();
        assert_eq!(bytes, b"hello dtact-fs free functions");
        let s = dtact_util::fs::read_to_string(&path).await.unwrap();
        assert_eq!(s, "hello dtact-fs free functions");

        // try_exists
        assert!(dtact_util::fs::try_exists(&path).await.unwrap());
        assert!(
            !dtact_util::fs::try_exists(dir.join("does-not-exist"))
                .await
                .unwrap()
        );

        // copy
        let copy_path = dir.join("hello-copy.txt");
        let n = dtact_util::fs::copy(&path, &copy_path).await.unwrap();
        assert_eq!(n, bytes.len() as u64);
        assert_eq!(dtact_util::fs::read(&copy_path).await.unwrap(), bytes);

        // hard_link
        let link_path = dir.join("hello-hardlink.txt");
        dtact_util::fs::hard_link(&path, &link_path).await.unwrap();
        assert_eq!(dtact_util::fs::read(&link_path).await.unwrap(), bytes);

        // rename
        let renamed_path = dir.join("hello-renamed.txt");
        dtact_util::fs::rename(&copy_path, &renamed_path)
            .await
            .unwrap();
        assert!(!dtact_util::fs::try_exists(&copy_path).await.unwrap());
        assert!(dtact_util::fs::try_exists(&renamed_path).await.unwrap());

        // canonicalize
        let canon = dtact_util::fs::canonicalize(&path).await.unwrap();
        assert!(canon.is_absolute());

        // symlink_metadata (on a plain file, just confirms it doesn't
        // error and reports the same file — real symlink behavior is
        // covered in the platform-specific tests below).
        let meta = dtact_util::fs::symlink_metadata(&path).await.unwrap();
        assert!(meta.is_file());

        // set_permissions: flip read-only on, then back off, checking
        // `Permissions::readonly` after each — the concrete bit layout
        // is platform-specific, `readonly()` isn't.
        let mut perm = dtact_util::fs::metadata(&path).await.unwrap().permissions();
        perm.set_readonly(true);
        dtact_util::fs::set_permissions(&path, perm).await.unwrap();
        assert!(
            dtact_util::fs::metadata(&path)
                .await
                .unwrap()
                .permissions()
                .readonly()
        );
        let mut perm = dtact_util::fs::metadata(&path).await.unwrap().permissions();
        perm.set_readonly(false);
        dtact_util::fs::set_permissions(&path, perm).await.unwrap();
        assert!(
            !dtact_util::fs::metadata(&path)
                .await
                .unwrap()
                .permissions()
                .readonly()
        );

        // create_dir / remove_dir / remove_dir_all
        let subdir = dir.join("subdir");
        dtact_util::fs::create_dir(&subdir).await.unwrap();
        assert!(dtact_util::fs::try_exists(&subdir).await.unwrap());
        dtact_util::fs::remove_dir(&subdir).await.unwrap();
        assert!(!dtact_util::fs::try_exists(&subdir).await.unwrap());

        let nested = dir.join("nested/a/b/c");
        dtact_util::fs::create_dir_all(&nested).await.unwrap();
        assert!(dtact_util::fs::try_exists(&nested).await.unwrap());
        dtact_util::fs::remove_dir_all(dir.join("nested"))
            .await
            .unwrap();
        assert!(!dtact_util::fs::try_exists(&nested).await.unwrap());

        let _ = dtact_util::fs::remove_file(&path).await;
        let _ = dtact_util::fs::remove_file(&link_path).await;
        let _ = dtact_util::fs::remove_file(&renamed_path).await;
    });
}

#[cfg(unix)]
#[test]
fn test_fs_symlink_unix() {
    dtact_util::fs::init(2);

    let dir = std::env::temp_dir().join(format!("dtact-fs-test-symlink-{}", std::process::id()));
    block_on(dtact_util::fs::create_dir_all(&dir)).unwrap();

    block_on(async {
        let target = dir.join("target.txt");
        dtact_util::fs::write(&target, b"symlink target")
            .await
            .unwrap();
        let link = dir.join("link.txt");
        dtact_util::fs::symlink(&target, &link).await.unwrap();

        // symlink_metadata must report the link itself, not its target.
        let meta = dtact_util::fs::symlink_metadata(&link).await.unwrap();
        assert!(meta.file_type().is_symlink());

        // read_link must return the recorded target path.
        let resolved = dtact_util::fs::read_link(&link).await.unwrap();
        assert_eq!(resolved, target);

        // Reading through the link must yield the target's contents —
        // confirms it's a real, followable symlink, not just a file that
        // happens to report `is_symlink()`.
        assert_eq!(
            dtact_util::fs::read(&link).await.unwrap(),
            b"symlink target"
        );

        let _ = dtact_util::fs::remove_file(&link).await;
        let _ = dtact_util::fs::remove_file(&target).await;
    });
}
