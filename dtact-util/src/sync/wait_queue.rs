//! Shared internal waiter list used by every primitive in [`super`].
//!
//! Lock-free, built on [`crate::lockfree::BoundedMpmcQueue`] (Vyukov's
//! ring-buffer algorithm — the same one this crate's `sync::mpsc` uses
//! for its own message buffer, independently stress-tested there) for
//! genuine FIFO delivery, with [`crate::lockfree::MpmcStack`] (proven
//! correct elsewhere in this crate) as a rare overflow fallback beyond
//! [`READY_CAPACITY`] concurrent registrations.
//!
//! # Why FIFO, not LIFO
//!
//! An earlier version of this module used a LIFO stack for the main
//! waiter list, on the reasoning that "no primitive here promises
//! fairness about *which* waiter wakes next" — true in isolation, but
//! LIFO has a much sharper failure mode than mere unfairness under
//! *sustained* contention from a fixed, small set of repeatedly-blocking
//! participants (a bounded `mpsc` channel's producer threads are exactly
//! this): each new registration can jump the queue ahead of an older one,
//! so if the same handful of contenders keep re-registering, one specific
//! unlucky waiter can be buried under an endless stream of newer
//! registrations and never surface — not just "served out of order" but
//! **starved indefinitely** once external activity quiets down (nothing
//! is left to keep digging through the pile via further `wake_one`
//! calls). This was measured as a real, reproducible deadlock: a
//! multi-producer stress test against `sync::mpsc`'s bounded channel
//! would have most producer threads finish all their sends while one
//! specific thread never made another byte of progress, parked forever.
//! FIFO closes this — the *oldest* registration is always the next one
//! popped, so no waiter can be perpetually skipped by newer arrivals.
//!
//! # Why cancellable
//!
//! Every primitive in this module follows a "check state; if not ready,
//! register; check state again; if *that* check also succeeds, proceed
//! without waiting" pattern (necessary to avoid a lost-wakeup race — see
//! e.g. `Mutex::poll_lock`'s comment). That second check succeeding is
//! the *common* case under real contention, not a rare one, and every
//! time it happens the registration from the first step would otherwise
//! sit around as dead weight forever (nothing re-polls that exact
//! task-already-succeeded path again to remove it). [`WaitQueue::cancel`]
//! marks it tombstoned instead; [`WaitQueue::wake_one`]/[`WaitQueue::wake_all`]
//! skip tombstoned entries when they're popped rather than wasting a real
//! wake on a task that already made progress on its own.
use crate::lockfree::{BoundedMpmcQueue, MpmcStack};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Waker;

/// Fixed capacity of the FIFO ring every `WaitQueue` allocates on its
/// first registration (see `ready`'s field doc for why this is lazy, not
/// eager). Comfortably above realistic concurrent-waiter counts for any
/// single primitive; registrations beyond this rare capacity fall back to
/// a heap-allocating (and LIFO, so best-effort-fair only) overflow stack —
/// see the module doc's "why FIFO" section for why exceeding this in
/// practice is the scenario worth avoiding, not routing through cleanly.
const READY_CAPACITY: usize = 256;

/// One registration: a `Waker` plus its own tombstone flag. See the
/// module doc's "why cancellable" section.
struct Entry {
    valid: AtomicBool,
    waker: Waker,
}

/// Returned by [`WaitQueue::register`]; hand back to [`WaitQueue::cancel`]
/// if the caller ends up not needing to wait after all.
pub struct RegistrationToken(Arc<Entry>);

pub struct WaitQueue {
    /// Lazily allocated on the first `register` call rather than in
    /// `new()`: most `WaitQueue`s in a typical program (a `Mutex` that's
    /// never contended, a oneshot whose value arrives before anyone
    /// polls) never register a single waiter in their whole lifetime.
    /// Eagerly allocating a `READY_CAPACITY`-slot ring for every one of
    /// them regardless — measured as a genuine, severe regression
    /// (`oneshot_send_recv` benchmark: +130%) versus the original
    /// zero-allocation `MpmcStack::new()`/`AtomicPtr::new(null)`
    /// construction — pays real allocator cost for primitives that will
    /// never need it. `OnceLock` defers that cost to whichever call
    /// actually needs the ring, and is itself a one-time, already-lazy
    /// cost shared by every subsequent registration.
    ready: OnceLock<BoundedMpmcQueue<Arc<Entry>>>,
    overflow: MpmcStack<Arc<Entry>>,
}

impl WaitQueue {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ready: OnceLock::new(),
            overflow: MpmcStack::new(),
        }
    }

    fn ready(&self) -> &BoundedMpmcQueue<Arc<Entry>> {
        self.ready
            .get_or_init(|| BoundedMpmcQueue::new(READY_CAPACITY))
    }

    /// Register `waker` to be woken by a future [`Self::wake_one`]/
    /// [`Self::wake_all`]. Returns a token to hand back to
    /// [`Self::cancel`] if it turns out not to be needed after all.
    pub fn register(&self, waker: &Waker) -> RegistrationToken {
        let entry = Arc::new(Entry {
            valid: AtomicBool::new(true),
            waker: waker.clone(),
        });
        if let Err(entry) = self.ready().try_push(entry.clone()) {
            self.overflow.push(entry);
        }
        RegistrationToken(entry)
    }

    /// Retract a registration obtained from [`Self::register`] that
    /// turned out not to be needed. See the module doc's "why
    /// cancellable" section for why this matters beyond tidiness.
    // `token` is intentionally taken by value: it's a one-time-use handle
    // and consuming it here (even though the body only borrows through
    // it) statically prevents a caller from accidentally reusing it.
    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub fn cancel(&self, token: RegistrationToken) {
        token.0.valid.store(false, Ordering::Release);
    }

    /// Wake and remove one registered waiter, if any (the oldest still
    /// pending in the common case — see the module doc). Used where only
    /// one waiter can make progress at a time (a lock/permit becoming
    /// available) — waking every waiter would just have all but one
    /// immediately re-block.
    pub fn wake_one(&self) {
        // Unlike `wake_all`, this must stop at the first *valid* entry —
        // it can't keep draining up to an enqueue snapshot the way
        // `wake_all` does, since anything popped past that point and not
        // woken would be permanently lost rather than left queued for a
        // later `wake_one`/`wake_all` to find. So this keeps the simpler
        // "pop until empty or found one" shape, which is why it's used
        // for the repeated-release primitives (`Mutex`, `Semaphore`,
        // `RwLock`) where an occasional missed wake here just delays that
        // one waiter until the next release rather than stranding it
        // forever — unlike `wake_all`'s "this might be the last call
        // ever" case in `sync::broadcast`/`Notify`/`Barrier`.
        if let Some(ready) = self.ready.get() {
            while let Some(entry) = ready.try_pop() {
                if entry.valid.swap(false, Ordering::AcqRel) {
                    entry.waker.wake_by_ref();
                    return;
                }
            }
        }
        while let Some(entry) = self.overflow.pop() {
            if entry.valid.swap(false, Ordering::AcqRel) {
                entry.waker.wake_by_ref();
                return;
            }
        }
    }

    /// Wake and remove every registered waiter. Used where a state change
    /// is relevant to all waiters at once (a barrier releasing, a watch
    /// value changing, a broadcast send).
    pub fn wake_all(&self) {
        if let Some(ready) = self.ready.get() {
            // Drain up to a snapshot of the enqueue side taken *now*,
            // not just until the first empty read — see
            // `BoundedMpmcQueue::drain_until`'s doc for why the naive
            // "pop until None" loop can permanently miss a registration
            // that's concurrently claiming a slot but hasn't published
            // yet.
            let target = ready.enqueue_snapshot();
            ready.drain_until(target, |entry| {
                if entry.valid.swap(false, Ordering::AcqRel) {
                    entry.waker.wake_by_ref();
                }
            });
        }
        for entry in self.overflow.drain_all() {
            if entry.valid.swap(false, Ordering::AcqRel) {
                entry.waker.wake_by_ref();
            }
        }
    }

    /// Approximate "does anyone currently have a registration pending?"
    /// check — racy (a concurrent register/wake can make the answer
    /// stale the instant it's returned), and not meant to be exact.
    ///
    /// Used as a fairness heuristic for callers with a "try the fast path
    /// directly, only register if that fails" shape
    /// (`mpsc::Sender::poll_send` is the motivating case): a producer
    /// that never needs to block could otherwise perpetually win a bare
    /// CAS race against one that already committed to waiting its turn,
    /// simply by virtue of skipping the register/park/wake round-trip
    /// entirely. Checking this before attempting the fast path and
    /// falling back to registering-like-everyone-else when it's true
    /// closes that gap: once there's contention, every producer joins
    /// the same queue.
    #[must_use]
    pub fn has_waiters(&self) -> bool {
        self.ready.get().is_some_and(|r| !r.is_empty()) || !self.overflow.is_empty()
    }
}
