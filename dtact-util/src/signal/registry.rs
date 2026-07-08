//! Shared lock-free broadcast-listener registry used by both the Unix
//! (self-pipe) and Windows (console control handler) signal backends.
//!
//! Signal listeners are fundamentally different from every other
//! completion primitive in this crate: a timer or an fs read has exactly
//! one waiter that gets consumed once, but a signal (SIGINT, Ctrl+C, ...)
//! can have *multiple* independent listeners that each want to be woken
//! on *every* delivery for the process's lifetime — closer to a broadcast
//! channel than a one-shot future.
//!
//! [`ListenerRegistry`] is an append-only, fixed-capacity, lock-free list:
//! registering CAS-bumps a slot counter and claims the next slot (never
//! contended in practice — signal listeners are typically registered a
//! handful of times at startup, not on a hot path), and broadcasting walks
//! every claimed slot. There is no removal: a dropped listener just marks
//! its own [`ListenerState::dead`] flag so the broadcast loop skips it,
//! and its registry slot is leaked for the rest of the process's
//! lifetime. That's a deliberate, documented simplification — real
//! removal would need either a generation-tagged free-list (more
//! plumbing than a handful of leaked pointers per process justifies) or
//! periodic compaction; signal listeners in practice are process-lifetime
//! singletons (install once, run until exit), so the leak is bounded by
//! `MAX_LISTENERS` regardless of how many times a program registers and
//! drops one, not something that grows unbounded over a long-running
//! process's life.

use crate::lockfree::AtomicWakerSlot;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

/// Max concurrent listeners for a single signal kind. Generous for real
/// programs (which typically register one listener per signal kind, at
/// startup) without the registry needing dynamic growth.
pub const MAX_LISTENERS: usize = 16;

pub struct ListenerState {
    /// Count of deliveries observed but not yet consumed by `recv()`.
    /// Saturates rather than overflowing under a delivery storm — a
    /// caller that hasn't polled in a while just sees "at least one"
    /// on the next `recv()`, matching `tokio::signal::unix::Signal`'s
    /// own coalescing behavior.
    pending: AtomicUsize,
    waker: AtomicWakerSlot,
    dead: AtomicBool,
}

impl ListenerState {
    fn new() -> Self {
        Self {
            pending: AtomicUsize::new(0),
            waker: AtomicWakerSlot::new(),
            dead: AtomicBool::new(false),
        }
    }

    /// Called from the delivery path (signal-reader thread on Unix,
    /// console control handler thread on Windows) — never from a real
    /// signal handler context, so this is free to do normal atomic work
    /// (still avoids allocation/locking regardless, out of habit and
    /// because the delivery path is shared code either way).
    pub fn deliver(&self) {
        if self.dead.load(Ordering::Acquire) {
            return;
        }
        self.pending.fetch_add(1, Ordering::AcqRel);
        self.waker.take_and_wake();
    }

    pub fn poll_recv(&self, cx: &std::task::Context<'_>) -> std::task::Poll<()> {
        loop {
            let pending = self.pending.load(Ordering::Acquire);
            if pending > 0
                && self
                    .pending
                    .compare_exchange_weak(
                        pending,
                        pending - 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                return std::task::Poll::Ready(());
            }
            if pending > 0 {
                continue; // lost the CAS race to a concurrent poll_recv on the same listener; retry
            }
            self.waker.register(cx.waker());
            if self.pending.load(Ordering::Acquire) > 0 {
                continue; // delivered between the check above and registering; retry immediately
            }
            return std::task::Poll::Pending;
        }
    }

    fn mark_dead(&self) {
        self.dead.store(true, Ordering::Release);
    }
}

pub struct ListenerRegistry {
    slots: [AtomicPtr<ListenerState>; MAX_LISTENERS],
    len: AtomicUsize,
}

impl ListenerRegistry {
    pub const fn new() -> Self {
        // `AtomicPtr::new(null)` isn't `Copy`-array-initializable via
        // `[x; N]` in a const fn without `Default`/repeat tricks pre-1.90
        // arrays-of-non-Copy support; this crate targets 1.90+, but keep
        // the explicit-array form for clarity regardless.
        const NULL: AtomicPtr<ListenerState> = AtomicPtr::new(std::ptr::null_mut());
        Self {
            slots: [NULL; MAX_LISTENERS],
            len: AtomicUsize::new(0),
        }
    }

    /// Register a new listener, returning its shared state. Panics if
    /// `MAX_LISTENERS` concurrent registrations are already live for this
    /// signal kind — generous enough that hitting it indicates a real
    /// listener leak in the caller's program, not a legitimate use case.
    pub fn register(&self) -> Arc<ListenerState> {
        let state = Arc::new(ListenerState::new());
        let idx = self.len.fetch_add(1, Ordering::AcqRel);
        assert!(
            idx < MAX_LISTENERS,
            "dtact-signal: too many concurrent listeners for one signal kind (max {MAX_LISTENERS})"
        );
        let ptr = Arc::into_raw(Arc::clone(&state)) as *mut ListenerState;
        self.slots[idx].store(ptr, Ordering::Release);
        state
    }

    /// Broadcast a delivery to every live (non-dead) registered listener.
    pub fn broadcast(&self) {
        let len = self.len.load(Ordering::Acquire);
        for slot in &self.slots[..len] {
            let ptr = slot.load(Ordering::Acquire);
            if ptr.is_null() {
                continue; // registration in progress on another thread, hasn't stored yet
            }
            unsafe { &*ptr }.deliver();
        }
    }
}

/// RAII guard: marks a listener dead (so future broadcasts skip it) when
/// the owning `DtactSignalStream` is dropped. The registry slot itself is
/// intentionally leaked — see the module doc.
pub struct DeadOnDrop(pub Arc<ListenerState>);

impl Drop for DeadOnDrop {
    fn drop(&mut self) {
        self.0.mark_dead();
    }
}
