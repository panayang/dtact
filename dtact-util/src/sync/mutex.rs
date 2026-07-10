//! Async mutex — the primitive most people mean by "nonblocking mutex".

use super::wait_queue::WaitQueue;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

/// An async mutex: `.lock().await` yields the calling task (not the OS
/// thread) while the lock is held elsewhere, instead of blocking it.
///
/// # Errors
///
/// None of this type's methods can fail (no lock poisoning — a panic
/// while holding the guard simply unlocks on unwind, matching
/// `tokio::sync::Mutex`'s behavior, not `std::sync::Mutex`'s).
pub struct Mutex<T: ?Sized> {
    locked: AtomicBool,
    wait: WaitQueue,
    data: UnsafeCell<T>,
}

// SAFETY: `T: Send` is required to send the guarded value across threads
// (a guard obtained on one thread can be dropped, or the mutex moved, on
// another); the `locked` CAS is the sole gate on `data` access, so `T:
// Sync` isn't needed for `Mutex<T>: Sync` the way it would be for a type
// exposing `&T` without exclusion.
unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Create a new mutex guarding `data`, initially unlocked.
    #[must_use]
    pub const fn new(data: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            wait: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Consume the mutex, returning the guarded value.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Acquire the lock, waiting (without blocking the OS thread) if it's
    /// currently held elsewhere.
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        std::future::poll_fn(|cx| self.poll_lock(cx)).await
    }

    /// Acquire the lock if it's immediately available, without waiting.
    #[must_use]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
            // `then_some` evaluates its argument eagerly — with a guard
            // type whose `Drop` releases the lock, that would construct
            // (and, on the `false` branch, immediately drop-and-release)
            // a guard even when the CAS failed, unlocking a mutex someone
            // else legitimately holds. `then` with a closure defers
            // construction until we know the CAS actually succeeded.
            .then(|| MutexGuard { mutex: self })
    }

    fn poll_lock(&self, cx: &Context<'_>) -> Poll<MutexGuard<'_, T>> {
        // Skip the immediate fast-path CAS if anyone is already
        // registered waiting for the lock — otherwise a fresh `.lock()`
        // call can win the CAS against an already-waiting task every
        // single time the lock is released, starving it indefinitely
        // under sustained contention. See `WaitQueue::has_waiters`'s doc
        // (written up against the identical, confirmed-reproducible bug
        // in `mpsc`'s bounded channel) for the full explanation. `try_lock`
        // itself is unaffected — it's an explicit "don't wait" API and
        // must always attempt the CAS regardless of waiters.
        if !self.wait.has_waiters()
            && let Some(guard) = self.try_lock()
        {
            return Poll::Ready(guard);
        }
        // Register before the re-check below (not after) so a release
        // that happens between our failed CAS above and this register
        // can't be missed: `wake_one` always looks at the queue *after*
        // clearing `locked`, so if we're not in the queue yet when it
        // runs, its wakeup is lost — registering first, then re-checking,
        // closes that window the same way every other primitive in this
        // module does.
        let token = self.wait.register(cx.waker());
        if let Some(guard) = self.try_lock() {
            // Our own CAS succeeded without needing the wake — retract
            // the registration above so it doesn't sit around as dead
            // weight for `wake_one` to work around later. See
            // `WaitQueue::cancel`'s doc for why this matters beyond tidiness.
            self.wait.cancel(token);
            return Poll::Ready(guard);
        }
        Poll::Pending
    }

    /// Get mutable access to the guarded value without locking — sound
    /// because `&mut self` statically proves no other reference (locked
    /// or not) can exist.
    pub const fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self {
            locked: AtomicBool::new(false),
            wait: WaitQueue::new(),
            data: UnsafeCell::new(T::default()),
        }
    }
}

/// RAII guard for a locked [`Mutex`]. Releases the lock (and wakes one
/// waiter, if any) on drop.
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
}

// SAFETY: a `MutexGuard` is proof the lock is held, so `&T`/`&mut T`
// through it never overlaps another guard's access. Sending the guard
// itself across threads (unlocking on whichever thread drops it) is sound
// as long as `T: Send`, same as `std`/`tokio`'s own guards.
unsafe impl<T: ?Sized + Send> Send for MutexGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: holding a `MutexGuard` is exclusive proof of the lock.
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: same as `Deref` above.
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.locked.store(false, Ordering::Release);
        self.mutex.wait.wake_one();
    }
}
