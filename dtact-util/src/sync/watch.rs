//! Single-value "latest state" broadcast — every [`Receiver`] always sees
//! the most recently sent value, never a backlog of every value sent
//! (unlike [`super::broadcast`]).

use super::wait_queue::WaitQueue;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

struct Shared<T> {
    value: RwLock<T>,
    /// Bumped on every `send`; a `Receiver` compares this against the
    /// version it last observed to know whether the value has changed
    /// since its last `changed()`/construction.
    version: AtomicU64,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
    wait: WaitQueue,
}

/// Create a watch channel seeded with `init`.
#[must_use]
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        value: RwLock::new(init),
        version: AtomicU64::new(0),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
        wait: WaitQueue::new(),
    });
    let receiver = Receiver {
        shared: shared.clone(),
        seen_version: 0,
    };
    (Sender { shared }, receiver)
}

/// The sending half of a [`channel`]. Cheaply [`Clone`]-able.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Deliberately does NOT bump `version` — closing isn't a
            // value change, and `poll_changed` checks `is_closed()` only
            // *after* `try_observe_change()` finds nothing new, so
            // bumping the version here would make a plain close look
            // like an unseen value change and return `Ok(())` instead of
            // `Err`. Just wake any waiter blocked in `changed()` so it
            // re-polls, observes `sender_count == 0` via `is_closed()`,
            // and returns the correct `Err` — rather than waiting forever
            // for a send that will never come.
            self.shared.wait.wake_all();
        }
    }
}

impl<T> Sender<T> {
    /// Replace the current value with `value`, notifying every receiver.
    pub fn send(&self, value: T) {
        *self
            .shared
            .value
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
        self.shared.version.fetch_add(1, Ordering::Release);
        self.shared.wait.wake_all();
    }

    /// A read-only snapshot of the current value.
    pub fn borrow(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.shared
            .value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `true` once every [`Receiver`] has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.receiver_count.load(Ordering::Acquire) == 0
    }
}

/// The receiving half of a [`channel`].
///
/// [`Clone`]-able — each clone tracks its own "last seen version"
/// independently, so every receiver (original and clones) sees every
/// value change via its own [`changed`](Self::changed) calls.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    seen_version: u64,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
            seen_version: self.seen_version,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T: Send + Sync> Receiver<T> {
    /// Wait until the value has changed since the last time this
    /// receiver observed it (via construction or a previous
    /// `changed()`/`borrow_and_update()`), then mark it seen.
    ///
    /// # Errors
    /// Returns [`RecvError`] once every [`Sender`] has been dropped and
    /// there are no further changes to observe.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        std::future::poll_fn(|cx| self.poll_changed(cx)).await
    }
}

impl<T> Receiver<T> {
    /// A read-only snapshot of the current value. Does not mark it as
    /// "seen" — a subsequent [`changed`](Self::changed) still resolves
    /// immediately if the value changed before this call.
    pub fn borrow(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.shared
            .value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Like [`borrow`](Self::borrow), but also marks the current value as
    /// seen — a subsequent [`changed`](Self::changed) only resolves on a
    /// value sent *after* this call.
    pub fn borrow_and_update(&mut self) -> std::sync::RwLockReadGuard<'_, T> {
        self.seen_version = self.shared.version.load(Ordering::Acquire);
        self.borrow()
    }

    fn poll_changed(&mut self, cx: &Context<'_>) -> Poll<Result<(), RecvError>> {
        if self.try_observe_change() {
            return Poll::Ready(Ok(()));
        }
        if self.is_closed() {
            // The last sender could have sent a final value and then
            // dropped concurrently with the check above — see
            // `mpsc::Receiver::poll_recv`'s identical comment for why one
            // more observe attempt is needed before reporting closed.
            return Poll::Ready(if self.try_observe_change() {
                Ok(())
            } else {
                Err(RecvError)
            });
        }
        let token = self.shared.wait.register(cx.waker());
        if self.try_observe_change() {
            self.shared.wait.cancel(token);
            return Poll::Ready(Ok(()));
        }
        if self.is_closed() {
            let result = if self.try_observe_change() {
                Ok(())
            } else {
                Err(RecvError)
            };
            self.shared.wait.cancel(token);
            return Poll::Ready(result);
        }
        Poll::Pending
    }

    fn try_observe_change(&mut self) -> bool {
        let current = self.shared.version.load(Ordering::Acquire);
        if current == self.seen_version {
            false
        } else {
            self.seen_version = current;
            true
        }
    }

    fn is_closed(&self) -> bool {
        self.shared.sender_count.load(Ordering::Acquire) == 0
    }
}

/// Error returned by [`Receiver::changed`] once every [`Sender`] has been
/// dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("channel closed: every sender dropped")
    }
}

impl std::error::Error for RecvError {}
