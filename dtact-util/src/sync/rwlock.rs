//! Async reader-writer lock.

use super::wait_queue::WaitQueue;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::task::{Context, Poll};

/// `state == 0`: unlocked. `state > 0`: that many readers hold the lock.
/// `state == WRITE_LOCKED`: one writer holds the lock. No writer-starves-
/// readers fairness — a steady stream of readers can, in principle, keep
/// a waiting writer pending indefinitely, the same simplification
/// `std::sync::RwLock` itself makes on most platforms.
const WRITE_LOCKED: isize = -1;

/// An async reader-writer lock: `.read().await`/`.write().await` yield
/// the calling task rather than blocking the OS thread while contended.
pub struct RwLock<T: ?Sized> {
    state: AtomicIsize,
    wait: WaitQueue,
    data: UnsafeCell<T>,
}

// SAFETY: same reasoning as `Mutex`'s — `state`'s CAS/fetch-based
// transitions are the sole gate on `data` access, so `Sync` needs `T:
// Send + Sync` (shared reads must actually be safe to share, unlike a
// plain mutex which never exposes concurrent `&T`).
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    /// Create a new lock guarding `data`, initially unlocked.
    #[must_use]
    pub const fn new(data: T) -> Self {
        Self {
            state: AtomicIsize::new(0),
            wait: WaitQueue::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Consume the lock, returning the guarded value.
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

impl<T: ?Sized + Send + Sync> RwLock<T> {
    /// Acquire a shared read lock, waiting while a writer holds it.
    /// Multiple readers may hold the lock concurrently.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        std::future::poll_fn(|cx| self.poll_read(cx)).await
    }

    /// Acquire the exclusive write lock, waiting while any reader or
    /// writer holds it.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        std::future::poll_fn(|cx| self.poll_write(cx)).await
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Acquire a read lock if immediately available, without waiting.
    #[must_use]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        // See `Mutex::try_lock`'s comment on why this must be `.then(||
        // ...)`, not `.then_some(...)` — the guard's `Drop` releases a
        // read-lock slot, which `then_some`'s eager evaluation would do
        // even when acquisition failed.
        self.try_acquire_read()
            .then(|| RwLockReadGuard { lock: self })
    }

    /// Acquire the write lock if immediately available, without waiting.
    #[must_use]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.state
            .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
            .then(|| RwLockWriteGuard { lock: self })
    }

    fn try_acquire_read(&self) -> bool {
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            if current < 0 {
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn poll_read(&self, cx: &Context<'_>) -> Poll<RwLockReadGuard<'_, T>> {
        if self.try_acquire_read() {
            return Poll::Ready(RwLockReadGuard { lock: self });
        }
        // See `Mutex::poll_lock` for why registration comes before the
        // re-check, not after.
        self.wait.register(cx.waker());
        if self.try_acquire_read() {
            return Poll::Ready(RwLockReadGuard { lock: self });
        }
        Poll::Pending
    }

    fn poll_write(&self, cx: &Context<'_>) -> Poll<RwLockWriteGuard<'_, T>> {
        if let Some(guard) = self.try_write() {
            return Poll::Ready(guard);
        }
        self.wait.register(cx.waker());
        if let Some(guard) = self.try_write() {
            return Poll::Ready(guard);
        }
        Poll::Pending
    }

    /// Get mutable access to the guarded value without locking — sound
    /// because `&mut self` statically proves no other reference exists.
    pub const fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self {
            state: AtomicIsize::new(0),
            wait: WaitQueue::new(),
            data: UnsafeCell::new(T::default()),
        }
    }
}

/// RAII guard for a shared read lock on a [`RwLock`].
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Send for RwLockReadGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: holding a read guard is proof no writer holds the lock.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let prev = self.lock.state.fetch_sub(1, Ordering::Release);
        // Only the last reader to leave can possibly unblock a waiting
        // writer — waking on every reader release would just have every
        // earlier wakeup find the lock still held and re-park.
        if prev == 1 {
            self.lock.wait.wake_all();
        }
    }
}

/// RAII guard for the exclusive write lock on a [`RwLock`].
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Send> Send for RwLockWriteGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: holding a write guard is exclusive proof of the lock.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: same as `Deref` above.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.state.store(0, Ordering::Release);
        self.lock.wait.wake_all();
    }
}
