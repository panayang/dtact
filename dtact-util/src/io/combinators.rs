//! [`AsyncRead`]/[`AsyncWrite`] traits shared by every socket-like stream
//! type in this crate (on both backends) plus the handful of generic
//! combinators built on them. See the parent module's doc for what's
//! deliberately not included and why.

use std::future::Future;
use std::io;

/// Async byte-stream read, implemented by every socket-like stream type
/// in this crate on both backends.
///
/// Exists purely so [`BufReader`]/[`copy`] below can be written once
/// instead of duplicated per stream type per backend — not meant as a
/// general-purpose extension point the way `tokio::io::AsyncRead` is.
///
/// `&self`, not `&mut self`: matches every implementor's own inherent
/// `read` method, which already supports concurrent calls from different
/// tasks via a shared reference. See this module's parent doc for why
/// that means no `split()` is needed here.
pub trait AsyncRead {
    /// Read into `buf`, returning the number of bytes read (`0` = EOF).
    ///
    /// # Errors
    /// Returns whatever the underlying stream's own `read` returns.
    fn read(&self, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send;
}

/// Async byte-stream write — the write-side twin of [`AsyncRead`]; see
/// its doc for the shared rationale.
pub trait AsyncWrite {
    /// Write from `buf`, returning the number of bytes written.
    ///
    /// # Errors
    /// Returns whatever the underlying stream's own `write` returns.
    fn write(&self, buf: &[u8]) -> impl Future<Output = io::Result<usize>> + Send;
}

/// Copy every byte from `reader` to `writer` until `reader` reports EOF,
/// returning the total byte count copied.
///
/// # Errors
/// Returns whatever `reader.read`/`writer.write` returns, or
/// [`io::ErrorKind::WriteZero`] if `writer.write` ever reports `0`
/// written bytes for a nonempty buffer (mirrors `std::io::copy`'s own
/// handling of a "stuck" writer).
pub async fn copy<R: AsyncRead + Sync + ?Sized, W: AsyncWrite + Sync + ?Sized>(
    reader: &R,
    writer: &W,
) -> io::Result<u64> {
    let mut buf = [0u8; 8192];
    let mut total: u64 = 0;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(total);
        }
        let mut written = 0;
        while written < n {
            let w = writer.write(&buf[written..n]).await?;
            if w == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "dtact-io: copy's writer reported 0 bytes written for a nonempty buffer",
                ));
            }
            written += w;
        }
        total += written as u64;
    }
}

/// Wraps an [`AsyncRead`] with an internal buffer, amortizing many small
/// `.read()` calls into fewer, larger reads on the underlying stream.
pub struct BufReader<T> {
    inner: T,
    buf: Box<[u8]>,
    pos: usize,
    filled: usize,
}

impl<T> BufReader<T> {
    /// Wrap `inner` with an 8 KiB internal buffer.
    pub fn new(inner: T) -> Self {
        Self::with_capacity(8192, inner)
    }

    /// Wrap `inner` with a `capacity`-byte internal buffer.
    #[must_use]
    pub fn with_capacity(capacity: usize, inner: T) -> Self {
        Self {
            inner,
            buf: vec![0u8; capacity.max(1)].into_boxed_slice(),
            pos: 0,
            filled: 0,
        }
    }

    /// Borrow the wrapped stream.
    pub const fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Unwrap back to the underlying stream. Any bytes already buffered
    /// but not yet consumed via [`Self::read`] are discarded.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: AsyncRead> BufReader<T> {
    /// Read into `buf`, drawing from the internal buffer first and only
    /// refilling it (via one `.read()` on the wrapped stream) once it's
    /// fully drained.
    ///
    /// # Errors
    /// Returns whatever the wrapped stream's `read` returns.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.filled {
            let n = self.inner.read(&mut self.buf).await?;
            self.pos = 0;
            self.filled = n;
            if n == 0 {
                return Ok(0);
            }
        }
        let available = &self.buf[self.pos..self.filled];
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.pos += n;
        Ok(n)
    }
}

/// Wraps an [`AsyncWrite`] with an internal buffer, amortizing many small
/// `.write()` calls into fewer, larger writes on the underlying stream.
///
/// **Does not flush on drop** (there's no async destructor to do it
/// with) — call [`Self::flush`] explicitly before the writer goes out of
/// scope, or buffered-but-unwritten bytes are silently lost.
pub struct BufWriter<T> {
    inner: T,
    buf: Vec<u8>,
    capacity: usize,
}

impl<T> BufWriter<T> {
    /// Wrap `inner` with an 8 KiB internal buffer.
    pub fn new(inner: T) -> Self {
        Self::with_capacity(8192, inner)
    }

    /// Wrap `inner` with a `capacity`-byte internal buffer.
    #[must_use]
    pub fn with_capacity(capacity: usize, inner: T) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner,
            buf: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Borrow the wrapped stream.
    pub const fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Unwrap back to the underlying stream. Any buffered bytes not yet
    /// flushed via [`Self::flush`] are silently discarded — see this
    /// type's doc.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: AsyncWrite> BufWriter<T> {
    /// Buffer `data`, flushing the internal buffer first if `data`
    /// wouldn't fit, and bypassing the buffer entirely (writing straight
    /// through) for a chunk at least as large as the buffer's own
    /// capacity — buffering it first would just be an extra copy.
    ///
    /// # Errors
    /// Returns whatever an internal [`Self::flush`] or the wrapped
    /// stream's `write` returns.
    pub async fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() >= self.capacity {
            self.flush().await?;
            return self.inner.write(data).await;
        }
        if self.buf.len() + data.len() > self.capacity {
            self.flush().await?;
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    /// Write out every buffered byte, retrying a short write until the
    /// whole buffer is flushed.
    ///
    /// # Errors
    /// Returns whatever the wrapped stream's `write` returns, or
    /// [`io::ErrorKind::WriteZero`] if it ever reports `0` written bytes
    /// for a nonempty buffer.
    pub async fn flush(&mut self) -> io::Result<()> {
        let mut written = 0;
        while written < self.buf.len() {
            let n = self.inner.write(&self.buf[written..]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "dtact-io: BufWriter's wrapped stream reported 0 bytes written",
                ));
            }
            written += n;
        }
        self.buf.clear();
        Ok(())
    }
}
