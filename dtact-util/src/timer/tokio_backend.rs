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
pub struct DtactSleep {
    inner: Pin<Box<tokio::time::Sleep>>,
}

impl DtactSleep {
    pub fn new(duration: Duration) -> Self {
        Self {
            inner: Box::pin(tokio::time::sleep(duration)),
        }
    }

    pub fn until(deadline: Instant) -> Self {
        Self {
            inner: Box::pin(tokio::time::sleep_until(deadline.into())),
        }
    }
}

impl Future for DtactSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.get_mut().inner.as_mut().poll(cx)
    }
}

/// Convenience free function mirroring `tokio::time::sleep`.
pub fn sleep(duration: Duration) -> DtactSleep {
    DtactSleep::new(duration)
}

/// A repeating timer. `tick()` mirrors `tokio::time::Interval::tick`.
pub struct DtactInterval {
    inner: tokio::time::Interval,
}

impl DtactInterval {
    pub fn new(period: Duration) -> Self {
        Self {
            inner: tokio::time::interval(period),
        }
    }

    /// Wait for the next tick, returning the `Instant` it fired at.
    pub async fn tick(&mut self) -> Instant {
        self.inner.tick().await.into_std()
    }
}

/// Convenience free function mirroring `tokio::time::interval`.
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
        TimeoutError
    }
}

/// Wraps a future with a deadline: resolves to `Ok(F::Output)` if the inner
/// future completes first, or `Err(TimeoutError)` if the deadline elapses
/// first.
pub struct DtactTimeout<F> {
    inner: Pin<Box<tokio::time::Timeout<F>>>,
}

impl<F: Future> DtactTimeout<F> {
    pub fn new(duration: Duration, inner: F) -> Self {
        Self {
            inner: Box::pin(tokio::time::timeout(duration, inner)),
        }
    }
}

impl<F: Future> Future for DtactTimeout<F> {
    type Output = Result<F::Output, TimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut()
            .inner
            .as_mut()
            .poll(cx)
            .map(|r| r.map_err(TimeoutError::from))
    }
}

/// Convenience free function mirroring `tokio::time::timeout`.
pub fn timeout<F: Future>(duration: Duration, fut: F) -> DtactTimeout<F> {
    DtactTimeout::new(duration, fut)
}
