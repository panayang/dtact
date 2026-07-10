//! Single-slot wake-up notification, matching `tokio::sync::Notify`'s
//! semantics: a permit that survives a `notify_one()` arriving before
//! anyone is waiting, but `notify_waiters()` only reaches tasks already
//! parked at the moment it's called.

use super::wait_queue::WaitQueue;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

/// A notification primitive commonly used to hand-wake another async
/// task/structure — e.g. signaling "state changed, go re-check" without a
/// full channel.
#[repr(align(64))]
pub struct Notify {
    /// A single-slot "permit": set by `notify_one()` when nothing was
    /// waiting, consumed by the very next `notified().await`.
    permit: AtomicBool,
    wait: WaitQueue,
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl Notify {
    /// Create a `Notify` with no stored permit and no waiters.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            permit: AtomicBool::new(false),
            wait: WaitQueue::new(),
        }
    }

    /// Wake one waiting `notified().await`, or — if none is currently
    /// waiting — store a permit so the *next* `notified().await` returns
    /// immediately without waiting at all.
    #[inline(always)]
    pub fn notify_one(&self) {
        // Unconditionally store the permit, *then* wake one queued waiter
        // — the same "set flag, then wake the (lock-free) queue" shape
        // `Mutex`/`RwLock`/`Semaphore` all use, rather than the mutex-
        // queue-only version this replaced, which conditionally stored
        // the permit solely when a locked look at the queue found it
        // empty. That conditional version needed the queue-emptiness
        // check and the permit store to be atomic together (true under a
        // shared mutex, false under `WaitQueue`'s lock-free stack: a
        // `notified()` call's `register` could land in the gap between
        // "found empty" and "store permit", registering a waker nothing
        // would ever wake, and permanently stalling that task while a
        // *different*, later `notified()` caller sees the (still-set)
        // permit and gets a wakeup that was never meant for it).
        //
        // This version can't lose a wakeup: any waiter's `Notified::poll`
        // rechecks the permit *after* registering (see below), so a store
        // that lands at any point relative to that registration is either
        // seen by the first check, the second check, or — if the register
        // raced ahead of both checks — by this call's `wake_one()`
        // finding that waiter already queued. If a waiter was already
        // parked, it gets directly woken (by `wake_one`) *and* finds the
        // permit set when it re-polls, consuming it immediately — so no
        // permit is ever left stranded for an unrelated later waiter to
        // pick up, matching `tokio::sync::Notify`'s "at most one buffered
        // permit" semantics exactly, just via a different code path.
        self.permit.store(true, Ordering::Release);
        self.wait.wake_one();
    }

    /// Wake every task currently waiting in `notified().await`. Does
    /// *not* store a permit — a task that calls `notified()` after this
    /// returns will wait for a future notification, not this one.
    #[inline(always)]
    pub fn notify_waiters(&self) {
        self.wait.wake_all();
    }

    /// A future that resolves once this `Notify` is notified — either a
    /// stored permit is consumed immediately, or a future
    /// [`notify_one`](Self::notify_one)/[`notify_waiters`](Self::notify_waiters)
    /// wakes it.
    pub const fn notified(&self) -> Notified<'_> {
        Notified { notify: self }
    }
}

/// Future returned by [`Notify::notified`].
#[repr(align(64))]
pub struct Notified<'a> {
    notify: &'a Notify,
}

impl Future for Notified<'_> {
    type Output = ();

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self
            .notify
            .permit
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return Poll::Ready(());
        }
        let token = self.notify.wait.register(cx.waker());
        if self
            .notify
            .permit
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            self.notify.wait.cancel(token);
            return Poll::Ready(());
        }
        Poll::Pending
    }
}
