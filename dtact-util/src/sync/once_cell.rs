//! Async once-initialized cell.

use super::wait_queue::WaitQueue;
use std::cell::UnsafeCell;
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};

const UNINIT: u8 = 0;
const INITIALIZING: u8 = 1;
const INIT: u8 = 2;

/// A cell that's initialized at most once, asynchronously.
///
/// Concurrent callers of [`OnceCell::get_or_init`] that race to be first
/// all wait for the same single initialization to complete rather than
/// each running their own initializer.
pub struct OnceCell<T> {
    state: AtomicU8,
    data: UnsafeCell<Option<T>>,
    wait: WaitQueue,
}

// SAFETY: `state`'s CAS transitions are the sole gate on `data` access —
// exactly one caller ever holds `INITIALIZING` and writes `data`, every
// other reader only reads `data` after observing `INIT`.
unsafe impl<T: Send> Send for OnceCell<T> {}
unsafe impl<T: Send + Sync> Sync for OnceCell<T> {}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OnceCell<T> {
    /// Create an empty, uninitialized cell.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(UNINIT),
            data: UnsafeCell::new(None),
            wait: WaitQueue::new(),
        }
    }

    /// Create a cell already initialized with `value`.
    #[must_use]
    pub const fn new_with(value: T) -> Self {
        Self {
            state: AtomicU8::new(INIT),
            data: UnsafeCell::new(Some(value)),
            wait: WaitQueue::new(),
        }
    }

    /// The cell's value, if it's already been initialized.
    #[must_use]
    pub fn get(&self) -> Option<&T> {
        (self.state.load(Ordering::Acquire) == INIT)
            // SAFETY: `INIT` observed under Acquire pairs with the
            // Release store in `get_or_init`/`set` that wrote `data`.
            .then(|| unsafe { (*self.data.get()).as_ref() })
            .flatten()
    }

    /// Set the cell's value if it isn't already initialized.
    ///
    /// # Errors
    /// Returns `value` back if the cell was already initialized (or is
    /// concurrently being initialized by [`Self::get_or_init`]).
    pub fn set(&self, value: T) -> Result<(), T> {
        if self
            .state
            .compare_exchange(UNINIT, INITIALIZING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(value);
        }
        // SAFETY: we hold the sole `INITIALIZING` token for this cell.
        unsafe {
            *self.data.get() = Some(value);
        }
        self.state.store(INIT, Ordering::Release);
        self.wait.wake_all();
        Ok(())
    }

    /// Return the cell's value, initializing it with (the result of
    /// awaiting) `init` first if necessary. Concurrent callers racing to
    /// initialize the same cell all observe the *same* initialization —
    /// only the winner's `init` future actually runs.
    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
        T: Send + Sync,
    {
        loop {
            if let Some(v) = self.get() {
                return v;
            }
            if self
                .state
                .compare_exchange(UNINIT, INITIALIZING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let value = init().await;
                // SAFETY: we hold the sole `INITIALIZING` token, so
                // writing `data` then publishing `INIT` (and reading the
                // just-written reference back out) is exclusive — no
                // `.expect()`/panic path needed the way going back
                // through `self.get()` would require.
                let value_ref = unsafe {
                    let slot = &mut *self.data.get();
                    *slot = Some(value);
                    slot.as_ref().unwrap_unchecked()
                };
                self.state.store(INIT, Ordering::Release);
                self.wait.wake_all();
                return value_ref;
            }
            // Someone else is initializing (or just finished, in which
            // case `self.get()` at the top of the next iteration returns
            // immediately) — wait for them.
            std::future::poll_fn(|cx| self.poll_wait_for_init(cx)).await;
        }
    }

    fn poll_wait_for_init(&self, cx: &Context<'_>) -> Poll<()> {
        if self.state.load(Ordering::Acquire) == INIT {
            return Poll::Ready(());
        }
        let token = self.wait.register(cx.waker());
        if self.state.load(Ordering::Acquire) == INIT {
            self.wait.cancel(token);
            return Poll::Ready(());
        }
        Poll::Pending
    }

    /// Consume the cell, returning its value if initialized.
    pub fn into_inner(self) -> Option<T> {
        self.data.into_inner()
    }
}
