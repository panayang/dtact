//! In-process async synchronization primitives — the `tokio::sync`
//! counterpart of this crate's I/O modules.
//!
//! Unlike `io`/`fs`/`process`/`signal`/`stream`/`timer`, this module has
//! no `native`/`tokio` backend split and isn't gated behind either
//! feature: none of these primitives touch an OS reactor or thread pool —
//! they're pure in-memory coordination (a lock, a permit counter, a
//! channel buffer) built directly on `std::sync`/`core::sync::atomic`, so
//! there's nothing backend-specific to choose between. They work
//! identically whether the calling future is driven by a `dtact` fiber, a
//! `tokio` task, or a hand-rolled executor.
//!
//! **Lock-free waiter bookkeeping.** Every primitive's own fast-path
//! state (a lock bit, a permit count, a version number, ...) is a plain
//! atomic, and the shared waiter list every primitive registers/wakes
//! through ([`wait_queue::WaitQueue`]) is backed by
//! [`crate::lockfree::MpmcStack`] — a lock-free Treiber stack, the same
//! building block the native `io`/`fs`/`timer` backends use for
//! cross-thread handoff — rather than a `std::sync::Mutex<VecDeque<Waker>>`.
//! See `wait_queue`'s module doc for the LIFO-ordering and no-dedup
//! trade-offs that come with dropping the mutex there, and each
//! primitive's own `register`/wake call sites for why the "unconditional
//! state change, then wake; waiter re-checks its own state after
//! registering" shape they all share stays race-free without a lock
//! serializing the two sides (see [`Notify`] specifically for a case
//! where the *naive* translation of a mutex-based version was not
//! race-free, and what shape was needed instead).
//!
//! [`mpsc`] and [`broadcast`] are the one exception: their actual message
//! buffer (not the waiter list, which is the lock-free `WaitQueue` like
//! everything else) is still a small `std::sync::Mutex<VecDeque<_>>`,
//! because both need strict FIFO order across concurrent producers/a
//! bounded backlog — a correctness requirement a Treiber stack (LIFO)
//! can't provide, and a proper lock-free MPSC/ring-buffer replacement
//! wasn't done in this pass. The lock is held only for a `VecDeque`
//! push/pop, never across an `.await` point.
//!
//! Covers: [`Mutex`]/[`RwLock`] (async locks), [`Semaphore`], [`Notify`],
//! [`Barrier`], [`OnceCell`], and three channel flavors —
//! [`oneshot`]/[`mpsc`]/[`watch`]/[`broadcast`] — matching the "big
//! categories" of `tokio::sync` without chasing every method (owned
//! guards, mapped guards, permit iterators, weak senders, etc. are
//! deliberately not included).

mod wait_queue;

mod barrier;
mod mutex;
mod notify;
mod once_cell;
mod rwlock;
mod semaphore;

pub mod broadcast;
pub mod mpsc;
pub mod oneshot;
pub mod watch;

pub use barrier::{Barrier, BarrierWaitResult};
pub use mutex::{Mutex, MutexGuard};
pub use notify::Notify;
pub use once_cell::OnceCell;
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{Semaphore, SemaphorePermit, TryAcquireError};
