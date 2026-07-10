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

/// Extension trait: call `.compat()` on a [`DtactFile`] to obtain a
/// [`DtactCompat`] adapter that implements `futures_io::AsyncRead`/`AsyncWrite`.
pub trait DtactCompatExt: Sized {
    /// Wrap `self` in a [`DtactCompat`] adapter.
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

/// Query a path's metadata.
///
/// # Errors
///
/// Returns whatever `tokio::fs::metadata` returns: `NotFound` if `path`
/// doesn't exist, `PermissionDenied` if it can't be traversed, etc.
pub async fn metadata(path: impl Into<PathBuf>) -> io::Result<std::fs::Metadata> {
    tokio::fs::metadata(path.into()).await
}

/// List a directory's entries.
///
/// # Errors
///
/// Returns whatever `tokio::fs::read_dir` (to open the directory) or
/// `next_entry` (per-entry, while draining it into a `Vec`) returns —
/// `NotFound`/`NotADirectory`/`PermissionDenied` being the common cases.
pub async fn read_dir(path: impl Into<PathBuf>) -> io::Result<Vec<tokio::fs::DirEntry>> {
    let mut rd = tokio::fs::read_dir(path.into()).await?;
    let mut out = Vec::new();
    while let Some(entry) = rd.next_entry().await? {
        out.push(entry);
    }
    Ok(out)
}

/// Recursively create a directory and all missing parent directories.
///
/// # Errors
///
/// Returns whatever `tokio::fs::create_dir_all` returns (e.g.
/// `PermissionDenied`).
pub async fn create_dir_all(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::create_dir_all(path.into()).await
}

/// Remove a file.
///
/// # Errors
///
/// Returns whatever `tokio::fs::remove_file` returns (`NotFound`,
/// `PermissionDenied`, etc).
pub async fn remove_file(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::remove_file(path.into()).await
}

/// Resolve `path` to an absolute path with all intermediate components
/// resolved.
///
/// # Errors
///
/// Returns whatever `tokio::fs::canonicalize` returns, e.g. `NotFound`.
pub async fn canonicalize(path: impl Into<PathBuf>) -> io::Result<PathBuf> {
    tokio::fs::canonicalize(path.into()).await
}

/// Copy the contents (and permission bits) of the file at `from` to `to`,
/// returning the byte count copied.
///
/// # Errors
///
/// Returns whatever `tokio::fs::copy` returns, e.g. `NotFound` if `from`
/// doesn't exist.
pub async fn copy(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> io::Result<u64> {
    tokio::fs::copy(from.into(), to.into()).await
}

/// Create a single new directory. Unlike [`create_dir_all`], fails if any
/// parent component doesn't already exist.
///
/// # Errors
///
/// Returns whatever `tokio::fs::create_dir` returns, e.g. `AlreadyExists`.
pub async fn create_dir(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::create_dir(path.into()).await
}

/// Create a hard link at `dst` pointing at the same file as `src`.
///
/// # Errors
///
/// Returns whatever `tokio::fs::hard_link` returns, e.g. `NotFound` if
/// `src` doesn't exist.
pub async fn hard_link(src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::hard_link(src.into(), dst.into()).await
}

/// Read the entire contents of the file at `path` into a `Vec<u8>`.
///
/// # Errors
///
/// Returns whatever `tokio::fs::read` returns, e.g. `NotFound`.
pub async fn read(path: impl Into<PathBuf>) -> io::Result<Vec<u8>> {
    tokio::fs::read(path.into()).await
}

/// Read the target of the symbolic link at `path`.
///
/// # Errors
///
/// Returns whatever `tokio::fs::read_link` returns, e.g. `NotFound`, or
/// an error if `path` isn't actually a symlink.
pub async fn read_link(path: impl Into<PathBuf>) -> io::Result<PathBuf> {
    tokio::fs::read_link(path.into()).await
}

/// Read the entire contents of the file at `path` into a `String`.
///
/// # Errors
///
/// Returns whatever `tokio::fs::read_to_string` returns, e.g. an
/// `InvalidData` error if the file isn't valid UTF-8.
pub async fn read_to_string(path: impl Into<PathBuf>) -> io::Result<String> {
    tokio::fs::read_to_string(path.into()).await
}

/// Remove an empty directory. Fails if `path` is non-empty — see
/// [`remove_dir_all`] for the recursive version.
///
/// # Errors
///
/// Returns whatever `tokio::fs::remove_dir` returns, e.g. `NotFound`, or
/// an error if the directory isn't empty.
pub async fn remove_dir(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::remove_dir(path.into()).await
}

/// Recursively remove a directory and everything under it.
///
/// # Errors
///
/// Returns whatever `tokio::fs::remove_dir_all` returns, e.g. `NotFound`.
pub async fn remove_dir_all(path: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::remove_dir_all(path.into()).await
}

/// Rename (move) the file or directory at `from` to `to`.
///
/// Replaces `to` if it already exists — see `std::fs::rename`'s own
/// documentation for cross-platform caveats (`tokio::fs::rename` shares
/// the same semantics, it just runs on the blocking pool).
///
/// # Errors
///
/// Returns whatever `tokio::fs::rename` returns.
pub async fn rename(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::rename(from.into(), to.into()).await
}

/// Set `path`'s permission bits to `perm`.
///
/// # Errors
///
/// Returns whatever `tokio::fs::set_permissions` returns, e.g.
/// `NotFound`.
pub async fn set_permissions(
    path: impl Into<PathBuf>,
    perm: std::fs::Permissions,
) -> io::Result<()> {
    tokio::fs::set_permissions(path.into(), perm).await
}

/// Create a symbolic link at `dst` pointing at `src`.
///
/// Unix only — Windows has separate [`symlink_dir`]/[`symlink_file`]
/// instead (a symlink on Windows must know up front whether it targets a
/// directory or a file).
///
/// # Errors
///
/// Returns whatever `tokio::fs::symlink` returns, e.g. `AlreadyExists` if
/// `dst` already exists.
#[cfg(unix)]
pub async fn symlink(src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::symlink(src.into(), dst.into()).await
}

/// Create a directory symbolic link at `dst` pointing at `src`. Windows
/// only — see [`symlink`] for the Unix equivalent.
///
/// # Errors
///
/// Returns whatever `tokio::fs::symlink_dir` returns, e.g.
/// `AlreadyExists` if `dst` already exists, or a permissions error —
/// creating symlinks on Windows normally requires either an elevated
/// process or Developer Mode enabled.
#[cfg(windows)]
pub async fn symlink_dir(src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::symlink_dir(src.into(), dst.into()).await
}

/// Create a file symbolic link at `dst` pointing at `src`. Windows only —
/// see [`symlink`] for the Unix equivalent.
///
/// # Errors
///
/// Same as [`symlink_dir`], for a file target instead of a directory.
#[cfg(windows)]
pub async fn symlink_file(src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> io::Result<()> {
    tokio::fs::symlink_file(src.into(), dst.into()).await
}

/// Query `path`'s metadata *without* following a trailing symlink (unlike
/// [`metadata`], which does).
///
/// # Errors
///
/// Returns whatever `tokio::fs::symlink_metadata` returns, e.g.
/// `NotFound`.
pub async fn symlink_metadata(path: impl Into<PathBuf>) -> io::Result<std::fs::Metadata> {
    tokio::fs::symlink_metadata(path.into()).await
}

/// Check whether `path` exists, following symlinks.
///
/// A permission error while checking is propagated as `Err` rather than
/// silently read as "doesn't exist" — see `std::fs::exists`'s own
/// documentation for the exact distinction `tokio::fs::try_exists`
/// shares.
///
/// # Errors
///
/// Returns an `io::Error` for any failure *other than* "doesn't exist".
pub async fn try_exists(path: impl Into<PathBuf>) -> io::Result<bool> {
    tokio::fs::try_exists(path.into()).await
}

/// Write `contents` to the file at `path`, creating it if it doesn't
/// exist and truncating it if it does.
///
/// # Errors
///
/// Returns whatever `tokio::fs::write` returns, e.g. `PermissionDenied`.
pub async fn write(path: impl Into<PathBuf>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    tokio::fs::write(path.into(), contents).await
}
