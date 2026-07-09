//! Single-value, single-use channel — one [`Sender`] sends at most one
//! value to one [`Receiver`].

use super::wait_queue::WaitQueue;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

struct Inner<T> {
    value: UnsafeCell<Option<T>>,
    sent: AtomicBool,
    sender_dropped: AtomicBool,
    receiver_dropped: AtomicBool,
    wait: WaitQueue,
}

// SAFETY: `sent`'s Release/Acquire pair is what makes writing `value` (by
// the sender, once) and reading it (by the receiver, once) not race —
// `send` writes then sets `sent`; `poll` never reads `value` without
// first observing `sent`.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// Create a connected sender/receiver pair for one value.
#[must_use]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        value: UnsafeCell::new(None),
        sent: AtomicBool::new(false),
        sender_dropped: AtomicBool::new(false),
        receiver_dropped: AtomicBool::new(false),
        wait: WaitQueue::new(),
    });
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

/// The sending half of a [`channel`]. Consumed by [`Sender::send`] — a
/// oneshot sender can only ever send once, so there's no `&self` send
/// method to accidentally call twice.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    /// Send `value` to the receiver.
    ///
    /// # Errors
    /// Returns `value` back if the receiver was already dropped (nobody
    /// left to receive it).
    pub fn send(self, value: T) -> Result<(), T> {
        if self.inner.receiver_dropped.load(Ordering::Acquire) {
            return Err(value);
        }
        // SAFETY: `Sender::send` consumes `self` and is the only writer
        // of `value`, called at most once (ownership prevents a second
        // call); no `Receiver` read can observe `value` before `sent` is
        // published below.
        unsafe {
            *self.inner.value.get() = Some(value);
        }
        self.inner.sent.store(true, Ordering::Release);
        self.inner.wait.wake_all();
        // `self` drops normally here — `Sender`'s `Drop` impl always
        // fires, but it's harmless post-send: it only marks
        // `sender_dropped` and wakes the receiver again, which is a no-op
        // once `sent` is already true (`try_take` checks `sent` first).
        Ok(())
    }

    /// `true` if the receiver has already been dropped — a subsequent
    /// [`send`](Self::send) is guaranteed to fail.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.receiver_dropped.load(Ordering::Acquire)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.sender_dropped.store(true, Ordering::Release);
        // Wake the receiver so a pending `.await` observes the closure
        // (`RecvError`) instead of hanging forever.
        self.inner.wait.wake_all();
    }
}

/// The receiving half of a [`channel`]. Implements [`Future`] directly —
/// `receiver.await` resolves once, either with the sent value or
/// [`RecvError`] if the sender was dropped without sending.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_dropped.store(true, Ordering::Release);
    }
}

/// Error returned by a [`Receiver`] when the [`Sender`] was dropped
/// without sending a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sender dropped without sending a value")
    }
}

impl std::error::Error for RecvError {}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.try_take() {
            return Poll::Ready(result);
        }
        self.inner.wait.register(cx.waker());
        if let Some(result) = self.try_take() {
            return Poll::Ready(result);
        }
        Poll::Pending
    }
}

impl<T> Receiver<T> {
    fn try_take(&self) -> Option<Result<T, RecvError>> {
        if self.inner.sent.load(Ordering::Acquire) {
            // SAFETY: `sent` observed true under Acquire, paired with the
            // Release store in `Sender::send` after writing `value` — the
            // value is visible and, since `sent` only ever transitions
            // false -> true once, this is the only place that ever takes it.
            let value = unsafe { (*self.inner.value.get()).take() };
            return Some(value.ok_or(RecvError));
        }
        if self.inner.sender_dropped.load(Ordering::Acquire) {
            return Some(Err(RecvError));
        }
        None
    }
}
