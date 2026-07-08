//! `tokio::fs`-backed filesystem primitives, for callers who'd rather share
//! tokio's own threadpool/reactor than spin up dtact-fs's own thread pool.

use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

pub use tokio::fs::File as DtactFile;

// =========================================================================
// COMPAT: convert DtactFile to futures-io AsyncRead+AsyncWrite
// =========================================================================
// `tokio::fs::File` already implements `tokio::io::AsyncRead`/`AsyncWrite`
// directly, so the only real gap is `futures_io`. Mirrors the
// `DtactCompat`/`DtactCompatExt` shape used by `crate::io`'s tokio backend.

/// Wraps a [`DtactFile`] to additionally implement `futures_io::AsyncRead`
/// and `futures_io::AsyncWrite` (on top of the `tokio::io` impls it already
/// has natively).
pub struct DtactCompat<T>(T);

impl<T> DtactCompat<T> {
    pub fn new(inner: T) -> Self {
        Self(inner)
    }

    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn get_ref(&self) -> &T {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Extension trait: call `.compat()` on a [`DtactFile`] to obtain a
/// [`DtactCompat`] adapter that implements `futures_io::AsyncRead`/`AsyncWrite`.
pub trait DtactCompatExt: Sized {
    fn compat(self) -> DtactCompat<Self>;
}

impl DtactCompatExt for DtactFile {
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

impl futures_io::AsyncRead for DtactCompat<DtactFile> {
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

impl futures_io::AsyncWrite for DtactCompat<DtactFile> {
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

pub async fn metadata(path: impl Into<PathBuf>) -> io::Result<std::fs::Metadata> {
    tokio::fs::metadata(path.into()).await
}

pub async fn read_dir(path: impl Into<PathBuf>) -> io::Result<Vec<tokio::fs::DirEntry>> {
    let mut rd = tokio::fs::read_dir(path.into()).await?;
    let mut out = Vec::new();
    while let Some(entry) = rd.next_entry().await? {
        out.push(entry);
    }
    Ok(out)
}

pub async fn create_dir_all(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::create_dir_all(path.into()).await
}

pub async fn remove_file(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::remove_file(path.into()).await
}
