//! `tokio::time`-backed timer primitives, for callers who'd rather share
//! tokio's own timer wheel/reactor than run dtact-timer's own worker thread.
//!
//! Exposes the same-shaped API as [`crate::timer::native`] so callers can
//! swap backends purely via feature flags.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// A future that completes once, after the given [`Duration`] has elapsed.
///
/// Stores the inner `tokio::time::Sleep` inline (structurally pinned)
/// rather than in a `Box`. `Sleep` is `!Unpin` (it links itself into
/// tokio's timer wheel), but it doesn't need to be heap-allocated to be
/// pinned — only to be pinned *without* pinning its owner, which nothing
/// here requires. Every [`sleep`]/[`DtactSleep::new`] call is a plausible
/// hot path (e.g. per-connection timeouts under the short-lived-connection
/// workloads this backend targets), so avoiding one allocation per call is
/// worth the small amount of unsafe pin-projection boilerplate.
pub struct DtactSleep {
    inner: tokio::time::Sleep,
}

impl DtactSleep {
    /// Create a sleep future that fires after `duration` elapses, measured
    /// from now.
    #[must_use]
    pub fn new(duration: Duration) -> Self {
        Self {
            inner: tokio::time::sleep(duration),
        }
    }

    /// Create a sleep future that fires once `deadline` is reached.
    #[must_use]
    pub fn until(deadline: Instant) -> Self {
        Self {
            inner: tokio::time::sleep_until(deadline.into()),
        }
    }
}

impl Future for DtactSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `inner` is never moved out of `self` (no public API
        // exposes `&mut inner` outside a pinned context), so projecting
        // the pin onto it upholds the structural-pinning contract.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll(cx)
    }
}

/// Convenience free function mirroring `tokio::time::sleep`.
#[must_use]
pub fn sleep(duration: Duration) -> DtactSleep {
    DtactSleep::new(duration)
}

/// A repeating timer. `tick()` mirrors `tokio::time::Interval::tick`.
pub struct DtactInterval {
    inner: tokio::time::Interval,
}

impl DtactInterval {
    /// Create a repeating timer that ticks every `period`, starting one
    /// `period` from now (matches `tokio::time::interval` semantics: the
    /// first `tick()` fires immediately, subsequent ticks are spaced by
    /// `period`).
    #[must_use]
    pub fn new(period: Duration) -> Self {
        Self {
            inner: tokio::time::interval(period),
        }
    }

    /// Wait for the next tick, returning the `Instant` it fired at.
    pub async fn tick(&mut self) -> Instant {
        self.inner.tick().await.into_std()
    }

    /// The current missed-tick catch-up policy. Thin wrapper over
    /// `tokio::time::Interval::missed_tick_behavior`.
    #[must_use]
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.inner.missed_tick_behavior()
    }

    /// Change the missed-tick catch-up policy. Thin wrapper over
    /// `tokio::time::Interval::set_missed_tick_behavior`.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.inner.set_missed_tick_behavior(behavior);
    }
}

/// Re-exported directly from `tokio::time` — see
/// `tokio::time::MissedTickBehavior`'s own documentation for the
/// `Burst`/`Delay`/`Skip` variants. The native backend defines its own
/// equivalent enum of the same name and shape rather than depending on
/// tokio, so this is a different concrete type per backend despite
/// identical semantics — matches how every other primitive in this crate
/// favors a same-shaped API over exact cross-backend type parity.
pub use tokio::time::MissedTickBehavior;

/// Convenience free function mirroring `tokio::time::interval`.
#[must_use]
pub fn interval(period: Duration) -> DtactInterval {
    DtactInterval::new(period)
}

/// Error returned by [`DtactTimeout`] when the wrapped future does not
/// complete before the deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutError;

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dtact-timer: deadline elapsed before the future completed"
        )
    }
}

impl std::error::Error for TimeoutError {}

impl From<tokio::time::error::Elapsed> for TimeoutError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self
    }
}

/// Wraps a future with a deadline: resolves to `Ok(F::Output)` if the inner
/// future completes first, or `Err(TimeoutError)` if the deadline elapses
/// first.
///
/// Stores the inner `tokio::time::Timeout<F>` inline rather than in a
/// `Box` — see [`DtactSleep`]'s doc for why that heap allocation is
/// avoidable here too.
pub struct DtactTimeout<F> {
    inner: tokio::time::Timeout<F>,
}

impl<F: Future> DtactTimeout<F> {
    /// Wrap `inner` with a `duration` deadline, measured from now.
    pub fn new(duration: Duration, inner: F) -> Self {
        Self {
            inner: tokio::time::timeout(duration, inner),
        }
    }
}

impl<F: Future> Future for DtactTimeout<F> {
    type Output = Result<F::Output, TimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: same structural-pinning contract as `DtactSleep::poll`.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll(cx).map(|r| r.map_err(TimeoutError::from))
    }
}

/// Convenience free function mirroring `tokio::time::timeout`.
pub fn timeout<F: Future>(duration: Duration, fut: F) -> DtactTimeout<F> {
    DtactTimeout::new(duration, fut)
}
