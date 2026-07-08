//! Native timer backend: a hashed timing wheel (Varghese & Lauck), the
//! standard O(1)-amortized-insert/fire structure for high-churn timer
//! workloads — single global heap-based timers do not belong in this
//! codebase.
//!
//! Layout: `WHEEL_SIZE` (256) buckets, each covering one `TICK` (1ms) of
//! wall-clock time, so one full rotation spans `WHEEL_SIZE * TICK` = 256ms.
//! A timer whose deadline falls further out than one rotation is placed in
//! its target slot immediately with a `rounds` counter set to how many full
//! rotations must elapse before it's live; each time the wheel revisits that
//! slot it decrements `rounds` until it reaches zero, at which point the
//! timer actually fires. This is the classic single-level hashed wheel with
//! implicit "cascading via rounds" instead of a multi-tier hierarchy —
//! O(1) insert, O(1) amortized fire, bounded per-tick work.
//!
//! **Zero-lock hot path.** Each bucket is a [`crate::lockfree::MpmcStack`]
//! (lock-free Treiber stack) instead of a `Mutex<Vec<_>>` — 256-way
//! sharding *and* no OS mutex/futex on either the insert or the per-tick
//! drain. Per-timer completion state is plain atomics
//! (`AtomicBool`/`AtomicI8` for the rounds counter) plus an
//! [`crate::lockfree::AtomicWakerSlot`] instead of `Mutex<Option<Waker>>`.
//! The worker thread idles via `thread::park()`/`Thread::unpark()`
//! (a futex under the hood, not a `Condvar`) rather than blocking on a
//! `Mutex`-guarded `Condvar::wait`.
//!
//! Cancellation is lazy: a dropped [`DtactSleep`] simply lets its
//! `Arc<Node>` refcount hit zero without being scrubbed out of its bucket;
//! the wheel drops the last reference silently when its round comes up
//! (waking nobody, since nothing is listening), so no O(n) linear removal
//! is ever needed on the hot cancel path.

use crate::lockfree::{AtomicWakerSlot, MpmcStack};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::thread::Thread;
use std::time::{Duration, Instant};

const WHEEL_BITS: u32 = 8;
const WHEEL_SIZE: usize = 1 << WHEEL_BITS; // 256 slots
const WHEEL_MASK: u64 = (WHEEL_SIZE as u64) - 1;
const TICK: Duration = Duration::from_millis(1);
/// Sentinel meaning "not yet fired" for `SleepState::done`, stored as a
/// plain atomic rather than a bool so the fire path is a single
/// store+wake with no separate flag/result pair to keep in sync.
const PENDING: i32 = 0;
const DONE: i32 = 1;

struct SleepState {
    /// PENDING (0) or DONE (1) — see constants above.
    status: AtomicI32,
    waker: AtomicWakerSlot,
}

struct Node {
    state: Arc<SleepState>,
    /// Remaining full rotations before this node is eligible to fire.
    /// Decremented with `Relaxed` fetch_sub — only ever touched by the
    /// single wheel worker thread, so no synchronization is needed beyond
    /// what already orders bucket drains (each tick fully drains and
    /// rebuilds its bucket via `MpmcStack::drain_all`/`push`).
    rounds: AtomicUsize,
}

struct Wheel {
    buckets: Box<[MpmcStack<Arc<Node>>]>,
    /// Absolute tick counter, advanced only by the single worker thread.
    current_tick: AtomicU64,
    pending: AtomicUsize,
    start: Instant,
    worker: OnceLock<Thread>,
}

static WHEEL: OnceLock<Arc<Wheel>> = OnceLock::new();

fn wheel() -> &'static Arc<Wheel> {
    WHEEL.get_or_init(|| {
        let mut buckets = Vec::with_capacity(WHEEL_SIZE);
        for _ in 0..WHEEL_SIZE {
            buckets.push(MpmcStack::new());
        }
        let w = Arc::new(Wheel {
            buckets: buckets.into_boxed_slice(),
            current_tick: AtomicU64::new(0),
            pending: AtomicUsize::new(0),
            start: Instant::now(),
            worker: OnceLock::new(),
        });
        let worker_wheel = Arc::clone(&w);
        let handle = std::thread::Builder::new()
            .name("dtact-timer-wheel".into())
            .spawn(move || worker_loop(worker_wheel))
            .expect("failed to spawn dtact-timer-wheel worker thread");
        let _ = w.worker.set(handle.thread().clone());
        w
    })
}

fn worker_loop(w: Arc<Wheel>) {
    loop {
        // Idle-park: no pending timers anywhere, avoid ticking for nothing.
        // `park_timeout` also guards against a lost wakeup race between
        // the emptiness check and the park call — worst case we wake up
        // to nothing pending and loop back around within 50ms.
        if w.pending.load(Ordering::Acquire) == 0 {
            std::thread::park_timeout(Duration::from_millis(50));
            continue;
        }

        let tick = w.current_tick.load(Ordering::Relaxed);
        let tick_deadline = w.start + TICK * (tick as u32 + 1);
        let now = Instant::now();
        if tick_deadline > now {
            std::thread::park_timeout(tick_deadline - now);
            // Re-loop regardless of whether we were woken early by a new
            // registration or by the timeout — the top-of-loop deadline
            // check below re-derives whether it's actually time to tick.
            if Instant::now() < tick_deadline {
                continue;
            }
        }

        let tick = w.current_tick.fetch_add(1, Ordering::Relaxed) + 1;
        let slot = (tick & WHEEL_MASK) as usize;

        let bucket = &w.buckets[slot];
        if bucket.is_empty() {
            continue;
        }
        let batch = bucket.drain_all();
        let mut fired = 0usize;
        for node in batch {
            let rounds = node.rounds.load(Ordering::Relaxed);
            if rounds == 0 {
                node.state.status.store(DONE, Ordering::Release);
                node.state.waker.take_and_wake();
                fired += 1;
            } else {
                node.rounds.fetch_sub(1, Ordering::Relaxed);
                bucket.push(node);
            }
        }
        if fired > 0 {
            w.pending.fetch_sub(fired, Ordering::AcqRel);
        }
    }
}

fn register(deadline: Instant) -> Arc<SleepState> {
    let w = wheel();
    let state = Arc::new(SleepState {
        status: AtomicI32::new(PENDING),
        waker: AtomicWakerSlot::new(),
    });

    let now = Instant::now();
    let ticks_from_now = if deadline <= now {
        0u64
    } else {
        let remaining = deadline - now;
        remaining.as_nanos().div_ceil(TICK.as_nanos()) as u64
    };

    let current_tick = w.current_tick.load(Ordering::Relaxed);
    let target_tick = current_tick + ticks_from_now;
    let slot = (target_tick & WHEEL_MASK) as usize;
    let rounds = (ticks_from_now >> WHEEL_BITS) as usize;

    let node = Arc::new(Node {
        state: Arc::clone(&state),
        rounds: AtomicUsize::new(rounds),
    });

    w.buckets[slot].push(node);
    w.pending.fetch_add(1, Ordering::AcqRel);
    if let Some(t) = w.worker.get() {
        t.unpark();
    }

    state
}

/// A future that completes once, after the given [`Duration`] has elapsed.
pub struct DtactSleep {
    state: Arc<SleepState>,
}

impl DtactSleep {
    pub fn new(duration: Duration) -> Self {
        Self::until(Instant::now() + duration)
    }

    pub fn until(deadline: Instant) -> Self {
        Self {
            state: register(deadline),
        }
    }
}

impl Future for DtactSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.state.status.load(Ordering::Acquire) == DONE {
            return Poll::Ready(());
        }
        self.state.waker.register(cx.waker());
        // Re-check after installing the waker to close the race where the
        // wheel fired between the initial load and the waker install.
        if self.state.status.load(Ordering::Acquire) == DONE {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// Convenience free function mirroring `tokio::time::sleep`.
pub fn sleep(duration: Duration) -> DtactSleep {
    DtactSleep::new(duration)
}

/// A repeating timer. `tick()` is a plain async method mirroring
/// `tokio::time::Interval::tick`.
pub struct DtactInterval {
    period: Duration,
    next: Instant,
}

impl DtactInterval {
    pub fn new(period: Duration) -> Self {
        assert!(
            period > Duration::ZERO,
            "dtact-timer: interval period must be > 0"
        );
        Self {
            period,
            next: Instant::now() + period,
        }
    }

    /// Wait for the next tick, returning the `Instant` it fired at.
    pub async fn tick(&mut self) -> Instant {
        DtactSleep::until(self.next).await;
        let fired_at = self.next;
        // Advance by whole periods to avoid drift accumulating from
        // scheduling jitter (MissedTickBehavior::Burst semantics).
        let now = Instant::now();
        let mut next = self.next + self.period;
        while next <= now {
            next += self.period;
        }
        self.next = next;
        fired_at
    }
}

/// Convenience free function mirroring `tokio::time::interval`.
pub fn interval(period: Duration) -> DtactInterval {
    DtactInterval::new(period)
}

/// Error returned by [`DtactTimeout`] when the wrapped future does not
/// complete before the deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutError;

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dtact-timer: deadline elapsed before the future completed"
        )
    }
}

impl std::error::Error for TimeoutError {}

/// Wraps a future with a deadline: resolves to `Ok(F::Output)` if the inner
/// future completes first, or `Err(TimeoutError)` if the deadline elapses
/// first.
pub struct DtactTimeout<F> {
    inner: Pin<Box<F>>,
    sleep: DtactSleep,
}

impl<F> DtactTimeout<F> {
    pub fn new(duration: Duration, inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
            sleep: DtactSleep::new(duration),
        }
    }
}

impl<F: Future> Future for DtactTimeout<F> {
    type Output = Result<F::Output, TimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(v) = this.inner.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut this.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(TimeoutError)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Convenience free function mirroring `tokio::time::timeout`.
pub fn timeout<F: Future>(duration: Duration, fut: F) -> DtactTimeout<F> {
    DtactTimeout::new(duration, fut)
}
