//! Counting semaphore — the usual building block for concurrency limits
//! (e.g. capping in-flight connections/requests).

use super::wait_queue::WaitQueue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

/// A counting semaphore: `.acquire().await` waits (without blocking the
/// OS thread) until a permit is available, then holds it until the
/// returned [`SemaphorePermit`] is dropped.
pub struct Semaphore {
    permits: AtomicUsize,
    wait: WaitQueue,
}

impl Semaphore {
    /// Create a semaphore with `permits` available immediately.
    #[must_use]
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            wait: WaitQueue::new(),
        }
    }

    /// Current number of permits available for immediate acquisition.
    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Relaxed)
    }

    /// Add `n` permits, waking waiters as needed.
    pub fn add_permits(&self, n: usize) {
        self.permits.fetch_add(n, Ordering::Release);
        self.wait.wake_all();
    }

    /// Acquire one permit, waiting if none are currently available.
    pub async fn acquire(&self) -> SemaphorePermit<'_> {
        std::future::poll_fn(|cx| self.poll_acquire(cx)).await
    }

    /// Acquire one permit if immediately available, without waiting.
    ///
    /// # Errors
    /// Returns [`TryAcquireError::NoPermits`] if none are currently free.
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        // See `Mutex::try_lock`'s comment on why this must be `.then(||
        // ...)`, not `.then_some(...)` — the permit's `Drop` releases a
        // permit back, which `then_some`'s eager evaluation would do even
        // when acquisition failed.
        self.try_acquire_one()
            .then(|| SemaphorePermit { sem: self })
            .ok_or(TryAcquireError::NoPermits)
    }

    fn try_acquire_one(&self) -> bool {
        let mut current = self.permits.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return false;
            }
            match self.permits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn poll_acquire(&self, cx: &Context<'_>) -> Poll<SemaphorePermit<'_>> {
        if self.try_acquire_one() {
            return Poll::Ready(SemaphorePermit { sem: self });
        }
        // See `Mutex::poll_lock` for why registration comes before the
        // re-check, not after.
        self.wait.register(cx.waker());
        if self.try_acquire_one() {
            return Poll::Ready(SemaphorePermit { sem: self });
        }
        Poll::Pending
    }
}

/// Error returned by [`Semaphore::try_acquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryAcquireError {
    /// No permits were immediately available.
    NoPermits,
}

impl std::fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no permits available")
    }
}

impl std::error::Error for TryAcquireError {}

/// RAII permit held on a [`Semaphore`]. Returns the permit (and wakes one
/// waiter, if any) on drop.
pub struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.sem.permits.fetch_add(1, Ordering::Release);
        self.sem.wait.wake_one();
    }
}
