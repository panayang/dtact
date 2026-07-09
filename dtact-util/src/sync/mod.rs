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
//! **Correctness-first, not lock-free.** Every primitive here guards its
//! waiter bookkeeping with a small `std::sync::Mutex` held only for the
//! handful of instructions it takes to push/pop a `Waker` — never across
//! an `.await` point or while running caller code. This mirrors the same
//! judgment call this crate's `fs` backend already documents for its own
//! completion state: these operations aren't hot enough (measured in
//! task-switches, not nanoseconds-per-syscall the way socket I/O is) to
//! justify a bespoke lock-free wait queue per primitive.
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
