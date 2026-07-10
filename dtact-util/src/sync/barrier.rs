//! Async rendezvous point for a fixed number of tasks.

use super::wait_queue::WaitQueue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

/// A barrier that releases all `n` participants together once every one
/// of them has called [`Barrier::wait`].
#[repr(align(64))]
pub struct Barrier {
    n: usize,
    /// Arrivals for the current generation; reset to 0 by whichever
    /// arrival completes the generation.
    count: AtomicUsize,
    /// Bumped every time the barrier releases, so waiters parked in an
    /// old generation know to stop waiting (and so a barrier can be
    /// reused indefinitely, matching `tokio::sync::Barrier`).
    generation: AtomicUsize,
    wait: WaitQueue,
}

impl Barrier {
    /// Create a barrier requiring `n` participants per generation. `n ==
    /// 0` behaves like `n == 1` (a single `wait()` call releases
    /// immediately as the leader) rather than a barrier no `wait()` call
    /// could ever complete.
    #[must_use]
    pub const fn new(n: usize) -> Self {
        Self {
            n: if n == 0 { 1 } else { n },
            count: AtomicUsize::new(0),
            generation: AtomicUsize::new(0),
            wait: WaitQueue::new(),
        }
    }

    /// Wait for every one of this barrier's `n` participants to arrive.
    /// Exactly one of the `n` calls in each generation gets back a
    /// [`BarrierWaitResult`] with [`is_leader`](BarrierWaitResult::is_leader)
    /// `true`; the rest get `false`. All `n` calls return together.
    #[inline(always)]
    pub async fn wait(&self) -> BarrierWaitResult {
        let observed_gen = self.generation.load(Ordering::Acquire);
        let arrived = self.count.fetch_add(1, Ordering::AcqRel) + 1;

        if arrived == self.n {
            self.count.store(0, Ordering::Relaxed);
            self.generation.fetch_add(1, Ordering::Release);
            self.wait.wake_all();
            return BarrierWaitResult { is_leader: true };
        }

        std::future::poll_fn(|cx| self.poll_generation_advanced(cx, observed_gen)).await;
        BarrierWaitResult { is_leader: false }
    }

    #[inline(always)]
    fn poll_generation_advanced(&self, cx: &Context<'_>, observed_gen: usize) -> Poll<()> {
        if self.generation.load(Ordering::Acquire) != observed_gen {
            return Poll::Ready(());
        }
        let token = self.wait.register(cx.waker());
        if self.generation.load(Ordering::Acquire) != observed_gen {
            self.wait.cancel(token);
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// Returned by [`Barrier::wait`]; tells the caller whether it was the one
/// call (per generation) whose arrival released everyone else. Purely
/// informational — every participant proceeds regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(align(64))]
pub struct BarrierWaitResult {
    is_leader: bool,
}

impl BarrierWaitResult {
    /// `true` for exactly one of the `n` [`Barrier::wait`] calls per
    /// generation — the one whose arrival completed it.
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        self.is_leader
    }
}
