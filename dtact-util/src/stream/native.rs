//! Native duplex-pipe backend: a lock-free, in-process byte pipe built
//! directly on [`crate::lockfree::SpscQueue`] — no OS transport, no
//! `Mutex`, no per-call heap allocation on the hot path.
//!
//! A pipe pair is two [`HalfPipe`]s (one per direction), each a fixed-
//! capacity SPSC ring buffer plus a pair of [`AtomicWakerSlot`]s: one for
//! a blocked reader (woken when a writer pushes into an empty-to-nonempty
//! ring or when the writer drops), one for a blocked writer (woken when a
//! reader pops from a full-to-nonfull ring or when the reader drops).
//! `writer_dropped`/`reader_dropped` flags give EOF-on-drop (read side)
//! and broken-pipe-on-drop (write side) semantics without either side
//! needing to synchronously coordinate shutdown.
//!
//! **Not registered with any OS reactor** — this is purely an in-process
//! handoff primitive (think `tokio::io::duplex`, not a Unix domain
//! socket). An OS-transport variant (Unix domain sockets on Linux/macOS,
//! named pipes on Windows) would be the natural next step for cross-
//! process use, and isn't implemented in this pass — see the crate-level
//! notes for what's deferred where.

use crate::lockfree::{AtomicWakerSlot, SpscQueue};
use std::io;
#[cfg(feature = "compat")]
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

struct HalfPipe {
    queue: SpscQueue<u8>,
    read_waker: AtomicWakerSlot,
    write_waker: AtomicWakerSlot,
    writer_dropped: AtomicBool,
    reader_dropped: AtomicBool,
}

impl HalfPipe {
    fn new(capacity: usize) -> Self {
        Self {
            queue: SpscQueue::new(capacity),
            read_waker: AtomicWakerSlot::new(),
            write_waker: AtomicWakerSlot::new(),
            writer_dropped: AtomicBool::new(false),
            reader_dropped: AtomicBool::new(false),
        }
    }
}

/// One end of an in-process duplex pipe. Create a connected pair with
/// [`pair`].
pub struct DtactStream {
    tx: Arc<HalfPipe>,
    rx: Arc<HalfPipe>,
}

unsafe impl Send for DtactStream {}
unsafe impl Sync for DtactStream {}

/// Create a connected pair of duplex streams, each with `capacity` bytes
/// of buffering per direction (rounded up to a power of two — required by
/// the underlying [`SpscQueue`]).
#[must_use]
pub fn pair(capacity: usize) -> (DtactStream, DtactStream) {
    let capacity = capacity.next_power_of_two().max(1);
    let a = Arc::new(HalfPipe::new(capacity));
    let b = Arc::new(HalfPipe::new(capacity));
    (
        DtactStream {
            tx: Arc::clone(&a),
            rx: Arc::clone(&b),
        },
        DtactStream { tx: b, rx: a },
    )
}

impl DtactStream {
    /// Non-blocking poll-based read, usable directly from a hand-rolled
    /// `Future` or via the `async fn read` convenience below.
    #[inline]
    pub fn poll_read(&self, cx: &Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        // An empty buffer is a trivial no-op success (matching `std`/
        // `tokio` convention) — must be handled before any `buf[0]`
        // indexing below, which would otherwise panic for a 0-length
        // buffer on the waker re-check fast path.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // Bulk-drain the contiguous occupied run(s) in one shot rather than
        // popping a byte at a time — one `copy_nonoverlapping` (two at a
        // wrap) plus a single `head` store instead of N atomic pairs.
        let n = self.rx.queue.pop_slice(buf);
        if n > 0 {
            self.rx.write_waker.take_and_wake();
            return Poll::Ready(Ok(n));
        }
        if self.rx.writer_dropped.load(Ordering::Acquire) && self.rx.queue.is_empty() {
            return Poll::Ready(Ok(0)); // EOF
        }
        self.rx.read_waker.register(cx.waker());
        // Re-check after registering to close the race where the writer
        // pushed (or dropped) between our drain above and the register call.
        let n = self.rx.queue.pop_slice(buf);
        if n > 0 {
            self.rx.write_waker.take_and_wake();
            return Poll::Ready(Ok(n));
        }
        if self.rx.writer_dropped.load(Ordering::Acquire) && self.rx.queue.is_empty() {
            return Poll::Ready(Ok(0));
        }
        Poll::Pending
    }

    /// Non-blocking poll-based write.
    #[inline]
    pub fn poll_write(&self, cx: &Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // An empty buffer is a trivial no-op success — must be handled
        // before any `buf[0]` indexing below, which would otherwise panic
        // for a 0-length buffer on the waker re-check fast path. Note this
        // intentionally skips the `reader_dropped` check other branches
        // make: writing zero bytes never actually touches the pipe, so
        // there's nothing to report as a broken pipe for.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.tx.reader_dropped.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "dtact-stream: peer dropped its read half",
            )));
        }
        // Bulk-fill the contiguous free run(s) in one shot rather than
        // pushing a byte at a time.
        let n = self.tx.queue.push_slice(buf);
        if n > 0 {
            self.tx.read_waker.take_and_wake();
            return Poll::Ready(Ok(n));
        }
        self.tx.write_waker.register(cx.waker());
        if self.tx.reader_dropped.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "dtact-stream: peer dropped its read half",
            )));
        }
        let n = self.tx.queue.push_slice(buf);
        if n > 0 {
            self.tx.read_waker.take_and_wake();
            return Poll::Ready(Ok(n));
        }
        Poll::Pending
    }

    /// Read into `buf`, returning the number of bytes read (`0` = EOF).
    ///
    /// # Errors
    ///
    /// Returns `io::ErrorKind::BrokenPipe` if the peer's write half has
    /// been dropped with no more buffered bytes to deliver (analogous to
    /// EOF on a real pipe, surfaced as `Ok(0)` in that case — see
    /// `poll_read`'s implementation — so in practice this async wrapper
    /// itself only ever returns `Ok`; the `Result` exists for symmetry
    /// with `write`/`write_all` and to leave room for future error paths).
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_read(cx, buf)).await
    }

    /// Write from `buf`, returning the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns `io::ErrorKind::BrokenPipe` if the peer has dropped its
    /// read half — there is no reader left to ever drain the queue, so
    /// the write can never succeed.
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_write(cx, buf)).await
    }

    /// Write the entirety of `buf`, retrying partial writes.
    ///
    /// # Errors
    ///
    /// Returns the same `io::ErrorKind::BrokenPipe` as [`Self::write`] if
    /// the peer drops its read half partway through.
    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.write(buf).await?;
            buf = &buf[n..];
        }
        Ok(())
    }
}

impl Drop for DtactStream {
    fn drop(&mut self) {
        self.tx.writer_dropped.store(true, Ordering::Release);
        self.tx.read_waker.take_and_wake();
        self.rx.reader_dropped.store(true, Ordering::Release);
        self.rx.write_waker.take_and_wake();
    }
}

// =============================================================================
// COMPAT: futures_io / tokio::io AsyncRead+AsyncWrite
// =============================================================================
// DtactStream's poll_read/poll_write already match the shape these traits
// want, so — unlike `io`/`fs`, which needed a `DtactCompat<T>` wrapper —
// these impl directly on `DtactStream` itself, gated behind the `compat`
// feature (pulled in automatically by the `tokio` feature) since that's
// what brings in the `futures-io`/`tokio` dependencies.

#[cfg(feature = "compat")]
impl futures_io::AsyncRead for DtactStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Self::poll_read(&self, cx, buf)
    }
}

#[cfg(feature = "compat")]
impl futures_io::AsyncWrite for DtactStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Self::poll_write(&self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "compat")]
impl tokio::io::AsyncRead for DtactStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        match Self::poll_read(&self, cx, unfilled) {
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "compat")]
impl tokio::io::AsyncWrite for DtactStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Self::poll_write(&self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
