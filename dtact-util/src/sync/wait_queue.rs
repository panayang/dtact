//! Shared internal waiter list used by every primitive in [`super`]. See
//! the module doc there for why this is a plain `Mutex`-guarded queue
//! rather than a lock-free structure.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::task::Waker;

pub struct WaitQueue {
    wakers: Mutex<VecDeque<Waker>>,
}

impl WaitQueue {
    pub const fn new() -> Self {
        Self {
            wakers: Mutex::new(VecDeque::new()),
        }
    }

    /// Register `waker` to be woken by a future [`Self::wake_one`]/
    /// [`Self::wake_all`]. Idempotent for repeated polls of the same task
    /// (checked via [`Waker::will_wake`]) so a tight poll loop doesn't
    /// grow the queue unbounded.
    pub fn register(&self, waker: &Waker) {
        let mut q = self
            .wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !q.iter().any(|w| w.will_wake(waker)) {
            q.push_back(waker.clone());
        }
    }

    /// Wake and remove the oldest registered waiter, if any. Used where
    /// only one waiter can make progress at a time (a lock/permit
    /// becoming available) — waking every waiter would just have all but
    /// one immediately re-block.
    pub fn wake_one(&self) {
        let woken = self
            .wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front();
        if let Some(w) = woken {
            w.wake();
        }
    }

    /// Like [`Self::wake_one`], but if the queue is empty, calls
    /// `on_empty` instead — atomically with respect to a concurrent
    /// `register` (both hold the same lock), so a waiter registering
    /// between the emptiness check and `on_empty` running can't happen.
    /// Used by [`super::Notify`] to decide, race-free, between handing a
    /// notification straight to a parked waiter or stashing it as a
    /// permit for the next `notified().await`.
    pub fn wake_one_or_else(&self, on_empty: impl FnOnce()) {
        let mut q = self
            .wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match q.pop_front() {
            Some(w) => {
                drop(q);
                w.wake();
            }
            None => on_empty(),
        }
    }

    /// Wake and remove every registered waiter. Used where a state change
    /// is relevant to all waiters at once (a barrier releasing, a watch
    /// value changing, a broadcast send).
    pub fn wake_all(&self) {
        let mut q = self
            .wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for w in q.drain(..) {
            w.wake();
        }
    }
}
