//! Exercises the dtact-signal backends. Only the paths that are actually
//! safe to trigger from within a test process are exercised end to end:
//! - Windows (this sandbox): registration/drop must not panic. Actually
//!   raising Ctrl+C would terminate the test runner, so delivery itself
//!   is not exercised here — see the module doc in `signal::windows`.
//! - Unix: `libc::raise(SIGUSR1)` self-signals a harmless, non-default-
//!   terminating signal once a handler is installed, which *does*
//!   exercise real delivery end to end (handler -> self-pipe -> reader
//!   thread -> registry broadcast -> waker). This is unverified on this
//!   Windows development sandbox — written carefully by inspection, but
//!   the maintainer's Linux/macOS pass should confirm it actually runs
//!   green there.

#[cfg(all(feature = "native", unix))]
use std::future::Future;

#[cfg(all(feature = "native", unix))]
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

/// Races `a` against `b`, returning `true` if `a` completed first.
/// Minimal inline select so this test file doesn't need an extra
/// dependency just for one helper.
#[cfg(all(feature = "native", unix))]
async fn a_wins<A: Future, B: Future>(a: A, b: B) -> bool {
    use std::pin::pin;
    use std::task::Poll;
    let mut a = pin!(a);
    let mut b = pin!(b);
    std::future::poll_fn(|cx| {
        if a.as_mut().poll(cx).is_ready() {
            return Poll::Ready(true);
        }
        if b.as_mut().poll(cx).is_ready() {
            return Poll::Ready(false);
        }
        Poll::Pending
    })
    .await
}

#[cfg(all(feature = "native", unix))]
#[test]
fn sigusr1_self_raise_is_delivered() {
    use dtact_util::signal::sigusr1;

    let stream = sigusr1();

    // First raise: exercises handler -> self-pipe -> reader thread ->
    // registry broadcast -> waker, end to end.
    unsafe {
        libc::raise(libc::SIGUSR1);
    }
    let delivered = block_on(a_wins(
        stream.recv(),
        dtact_util::timer::sleep(std::time::Duration::from_secs(3)),
    ));
    assert!(
        delivered,
        "first SIGUSR1 delivery was not observed via recv() within 3s"
    );

    // Second raise: confirms the stream keeps delivering (not a one-shot
    // future), matching tokio::signal::unix::Signal's repeat semantics.
    unsafe {
        libc::raise(libc::SIGUSR1);
    }
    let delivered = block_on(a_wins(
        stream.recv(),
        dtact_util::timer::sleep(std::time::Duration::from_secs(3)),
    ));
    assert!(
        delivered,
        "second SIGUSR1 delivery was not observed via recv() within 3s"
    );
}

#[cfg(all(feature = "native", windows))]
#[test]
fn ctrl_c_registration_and_drop_do_not_panic() {
    use dtact_util::signal::{ctrl_break, ctrl_c, ctrl_close, ctrl_logoff, ctrl_shutdown};

    // Actually delivering Ctrl+C would terminate this test process, so
    // this only exercises registration + the DeadOnDrop path — real
    // delivery is covered by the Unix SIGUSR1 test above, which shares
    // the same ListenerRegistry/ListenerState broadcast code path.
    let a = ctrl_c();
    let b = ctrl_c();
    let c = ctrl_break();
    // Same story for the three teardown-adjacent events: registering (and
    // dropping) must not panic or install anything that misbehaves absent
    // a real close/logoff/shutdown notification.
    let d = ctrl_close();
    let e = ctrl_logoff();
    let f = ctrl_shutdown();
    drop(a);
    drop(b);
    drop(c);
    drop(d);
    drop(e);
    drop(f);

    // Registering again after drops must still work (registry slots are
    // leaked-not-reused by design, but that must not corrupt anything).
    let _d = ctrl_c();
}

/// With no console-control event actually delivered, `recv()` must stay
/// `Pending` — no spurious wakeup, and polling it must not panic even
/// though the real signal-delivery path (a separate OS-level handler
/// thread) is never exercised here.
#[cfg(all(feature = "native", windows))]
#[test]
fn recv_is_pending_without_a_real_delivery() {
    use dtact_util::signal::ctrl_c;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Arc::new(NoopWaker).into();
    let mut cx = Context::from_waker(&waker);

    let stream = ctrl_c();
    let mut fut = pin!(stream.recv());
    // Poll a handful of times in a tight loop: without a real Ctrl+C this
    // must never resolve, and must never panic.
    for _ in 0..5 {
        assert!(
            matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
            "recv() must stay Pending without an actual delivery"
        );
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
#[tokio::test]
async fn tokio_backend_registers_without_panicking() {
    #[cfg(unix)]
    {
        let _s = dtact_util::signal::sigusr1();
        // The generic `register(SignalKind)` entry point (mirrors the
        // native backend's arbitrary-signal-number `DtactSignalStream::new`)
        // must also work for a signal that has no named convenience
        // wrapper here.
        let _quit = dtact_util::signal::register(dtact_util::signal::SignalKind::quit())
            .expect("register(SIGQUIT) must succeed");
    }
    #[cfg(windows)]
    {
        let _s = dtact_util::signal::ctrl_c();
        let _close = dtact_util::signal::ctrl_close();
        let _logoff = dtact_util::signal::ctrl_logoff();
        let _shutdown = dtact_util::signal::ctrl_shutdown();
    }
}
