//! `tokio::io::duplex`-backed stream primitives, for callers who'd rather
//! share tokio's own runtime than dtact-stream's lock-free in-process pipe.

pub use tokio::io::DuplexStream as DtactStream;

/// Create a connected pair of duplex streams, each with `capacity` bytes
/// of buffering.
#[must_use]
pub fn pair(capacity: usize) -> (DtactStream, DtactStream) {
    tokio::io::duplex(capacity)
}

// `tokio::io::DuplexStream` already implements `tokio::io::AsyncRead`/
// `AsyncWrite` natively; only `futures_io` needs a compat bridge, mirroring
// `fs::tokio_backend`'s `DtactCompat`.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a [`DtactStream`] to additionally implement
/// `futures_io::AsyncRead`/`AsyncWrite`.
pub struct DtactCompat<T>(T);

impl<T> DtactCompat<T> {
    /// Wrap `inner`.
    pub const fn new(inner: T) -> Self {
        Self(inner)
    }
    /// Unwrap back to the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
    /// Borrow the inner value.
    pub const fn get_ref(&self) -> &T {
        &self.0
    }
    /// Mutably borrow the inner value.
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Extension trait: call `.compat()` on a [`DtactStream`] to obtain a
/// [`DtactCompat`] adapter implementing `futures_io::AsyncRead`/`AsyncWrite`.
pub trait DtactCompatExt: Sized {
    /// Wrap `self` in a [`DtactCompat`] adapter.
    fn compat(self) -> DtactCompat<Self>;
}

impl DtactCompatExt for DtactStream {
    fn compat(self) -> DtactCompat<Self> {
        DtactCompat(self)
    }
}

impl<F: Future> Future for DtactCompat<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll(cx)
    }
}

impl futures_io::AsyncRead for DtactCompat<DtactStream> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        let mut read_buf = tokio::io::ReadBuf::new(buf);
        match tokio::io::AsyncRead::poll_read(this, cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl futures_io::AsyncWrite for DtactCompat<DtactStream> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        tokio::io::AsyncWrite::poll_write(this, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        tokio::io::AsyncWrite::poll_flush(this, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        tokio::io::AsyncWrite::poll_shutdown(this, cx)
    }
}
