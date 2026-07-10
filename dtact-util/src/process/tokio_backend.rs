//! `tokio::process`-backed process primitives, for callers who'd rather
//! share tokio's own reactor/thread pool than dtact-process's own.

pub use tokio::process::Child as DtactChild;
pub use tokio::process::ChildStderr as DtactChildStderr;
pub use tokio::process::ChildStdin as DtactChildStdin;
pub use tokio::process::ChildStdout as DtactChildStdout;
pub use tokio::process::Command as DtactCommand;

// =============================================================================
// COMPAT: futures_io for the child stdio handles
// =============================================================================
// tokio::process::Child{Stdin,Stdout,Stderr} already implement
// tokio::io::AsyncRead/AsyncWrite directly; only futures_io needs a
// bridge, mirroring fs::tokio_backend's DtactCompat.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a `tokio::process` child stdio handle to additionally implement
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

/// Extension trait: call `.compat()` on a child stdio handle to obtain a
/// [`DtactCompat`] adapter implementing `futures_io::AsyncRead`/`AsyncWrite`.
pub trait DtactCompatExt: Sized {
    /// Wrap `self` in a [`DtactCompat`] adapter.
    fn compat(self) -> DtactCompat<Self>;
}

impl DtactCompatExt for DtactChildStdout {
    fn compat(self) -> DtactCompat<Self> {
        DtactCompat(self)
    }
}
impl DtactCompatExt for DtactChildStderr {
    fn compat(self) -> DtactCompat<Self> {
        DtactCompat(self)
    }
}
impl DtactCompatExt for DtactChildStdin {
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

macro_rules! impl_read_compat {
    ($ty:ty) => {
        impl futures_io::AsyncRead for DtactCompat<$ty> {
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
    };
}

macro_rules! impl_write_compat {
    ($ty:ty) => {
        impl futures_io::AsyncWrite for DtactCompat<$ty> {
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
    };
}

impl_read_compat!(DtactChildStdout);
impl_read_compat!(DtactChildStderr);
impl_write_compat!(DtactChildStdin);
