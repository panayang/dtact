use super::*;

// The `Runtime` itself is wrapped in a Mutex<Option<…>> purely so
// `shutdown_runtime()` can drop it rather than leaking it until process
// exit — that Mutex is touched exactly twice (init, shutdown), never on
// the hot path. `runtime_handle()` used to lock it on *every single*
// poll of every in-flight op (a `Handle` clone is cheap, but the Mutex
// acquisition serialises all fibers doing I/O on one global lock); the
// `Handle` is now cached separately in its own lock-free `OnceLock` so
// reading it never contends with anything.
static TOKIO_RUNTIME: std::sync::OnceLock<std::sync::Mutex<Option<tokio::runtime::Runtime>>> =
    std::sync::OnceLock::new();
static TOKIO_HANDLE: std::sync::OnceLock<tokio::runtime::Handle> = std::sync::OnceLock::new();

fn runtime_handle() -> tokio::runtime::Handle {
    TOKIO_HANDLE.get().cloned().expect(
        "dtact-io tokio runtime not initialised — \
                 call dtact_io::init_runtime() before performing any I/O",
    )
}

// ── Public initialisation API ──────────────────────────────────────────

/// Initialise the backing Tokio runtime.
///
/// Matches the signature of the native driver so call-sites can
/// switch drivers with a single feature flag.  The extra parameters
/// (`buffer_pool_size`, `chunk_size`, `pin_cpus`, `ring_depth`) are
/// accepted for API compatibility but are ignored by the Tokio backend.
pub fn init_runtime(
    workers: usize,
    _buffer_pool_size: usize,
    _chunk_size: usize,
    _pin_cpus: &[usize],
    _ring_depth: u32,
) {
    TOKIO_RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers.max(1))
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime");
        let _ = TOKIO_HANDLE.set(rt.handle().clone());
        std::sync::Mutex::new(Some(rt))
    });
}

/// Shorthand initialiser — uses `workers` Tokio worker threads.
///
/// Equivalent to `init_runtime(workers, 0, 0, &[], 0)`.
pub fn init(workers: usize) {
    init_runtime(workers, 0, 0, &[], 0);
}

/// Gracefully shut down the Tokio runtime, waiting for all spawned
/// tasks to complete.
pub fn shutdown_runtime() {
    if let Some(cell) = TOKIO_RUNTIME.get()
        && let Ok(mut guard) = cell.lock()
        && let Some(rt) = guard.take()
    {
        rt.shutdown_background();
    }
}

/// Obtain a handle to the underlying Tokio runtime.
///
/// Useful for spawning Tokio tasks from within a dtact fiber.
///
/// # Panics
/// Panics if `init_runtime()` / `init()` has not been called.
pub fn get_runtime_handle() -> tokio::runtime::Handle {
    runtime_handle()
}

#[doc(hidden)]
pub struct TokioFutureWrapper<F> {
    inner: F,
}

impl<F: Future> Future for TokioFutureWrapper<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = runtime_handle().enter();
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll(cx)
    }
}

// =========================================================================
// OPCODES & DtactIoFuture  (tokio backend)
// =========================================================================
//
// This raw-fd primitive is Unix-only: it wraps `tokio::io::unix::AsyncFd`,
// which does not exist on Windows (Windows has no readiness-based fd
// polling model for sockets/pipes — tokio's Windows reactor is IOCP-based
// and only exposed through the higher-level `TcpStream`/`TcpListener`
// types). `DtactTcpStream`/`DtactTcpListener` below already ride on top of
// `tokio::net`, so they work cross-platform without this type; only the
// low-level `DtactIoFuture`/`OpCode` API is unavailable on Windows for now.
#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpCode {
    Read,
    Write,
    Accept,
    Connect,
}

/// Tokio-backend equivalent of the native `DtactIoFuture`.
///
/// Accepts the same public fields as the native variant so
/// call-sites compile without change when switching backends.
/// Internally it wraps the raw fd in a `tokio::io::unix::AsyncFd`
/// (registered with the tokio reactor) and issues direct `libc`
/// syscalls when the fd becomes ready.
///
/// `worker_idx`, `direct_fd_idx`, and `slot_idx` are present for API
/// compatibility only and are ignored by this backend.
#[cfg(unix)]
pub struct DtactIoFuture {
    pub worker_idx: usize,
    pub fd: u32,
    pub direct_fd_idx: u32,
    pub op: OpCode,
    pub buf_ptr: *mut u8,
    pub len: usize,
    pub offset: i64,
    pub addr: Option<libc::sockaddr_storage>,
    pub addr_len: libc::socklen_t,
    pub slot_idx: Option<usize>,
    // Internal: lazily created on the first WouldBlock.
    async_fd: Option<tokio::io::unix::AsyncFd<std::os::unix::io::RawFd>>,
}

#[cfg(unix)]
unsafe impl Send for DtactIoFuture {}
#[cfg(unix)]
unsafe impl Sync for DtactIoFuture {}

#[cfg(unix)]
impl DtactIoFuture {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_idx: usize,
        fd: u32,
        direct_fd_idx: u32,
        op: OpCode,
        buf_ptr: *mut u8,
        len: usize,
        offset: i64,
        addr: Option<libc::sockaddr_storage>,
        addr_len: libc::socklen_t,
        slot_idx: Option<usize>,
    ) -> Self {
        Self {
            worker_idx,
            fd,
            direct_fd_idx,
            op,
            buf_ptr,
            len,
            offset,
            addr,
            addr_len,
            slot_idx,
            async_fd: None,
        }
    }

    /// Attempt the underlying syscall once, returning the byte count or an
    /// error (including `WouldBlock` / `EAGAIN`).
    #[inline]
    fn try_syscall(
        fd: std::os::unix::io::RawFd,
        op: OpCode,
        buf_ptr: *mut u8,
        len: usize,
        addr: *const libc::sockaddr_storage,
        addr_len: libc::socklen_t,
    ) -> std::io::Result<usize> {
        let r = match op {
            OpCode::Read => unsafe { libc::read(fd, buf_ptr as *mut libc::c_void, len) },
            OpCode::Write => unsafe { libc::write(fd, buf_ptr as *const libc::c_void, len) },
            OpCode::Accept => unsafe {
                libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) as isize
            },
            OpCode::Connect => {
                // Check SO_ERROR first to see if a previous async connect attempt completed with an error.
                let mut err: libc::c_int = 0;
                let mut err_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                let r = unsafe {
                    libc::getsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        &mut err as *mut libc::c_int as *mut libc::c_void,
                        &mut err_len,
                    )
                };
                if r == 0 && err != 0 {
                    return Err(std::io::Error::from_raw_os_error(err));
                }

                let r = unsafe { libc::connect(fd, addr as *const libc::sockaddr, addr_len) };
                if r < 0 {
                    let e = std::io::Error::last_os_error();
                    let os_err = e.raw_os_error();
                    if os_err == Some(libc::EISCONN) {
                        return Ok(0);
                    }
                    return Err(e);
                }
                return Ok(0);
            }
        };
        if r < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(r as usize)
        }
    }

    #[inline]
    fn is_blocking_error(e: &std::io::Error) -> bool {
        let kind = e.kind();
        kind == std::io::ErrorKind::WouldBlock
            || e.raw_os_error() == Some(libc::EINPROGRESS)
            || e.raw_os_error() == Some(libc::EALREADY)
            || e.raw_os_error() == Some(libc::EINTR)
    }
}

#[cfg(unix)]
impl Future for DtactIoFuture {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Always enter the tokio runtime context so AsyncFd can register
        // with the reactor even when polled from a dtact fiber.
        let _guard = runtime_handle().enter();

        // SAFETY: DtactIoFuture is !Unpin only through PhantomPinned; the
        // fields we mutate here (async_fd) are not structurally pinned.
        let this = unsafe { self.get_unchecked_mut() };

        let fd = this.fd as std::os::unix::io::RawFd;
        let op = this.op;
        let buf_ptr = this.buf_ptr;
        let len = this.len;
        let addr_ptr: *const libc::sockaddr_storage = this
            .addr
            .as_ref()
            .map_or(std::ptr::null(), |a| a as *const _);
        let addr_len = this.addr_len;

        // ── Phase 1: first attempt, no registration yet ─────────────────
        if this.async_fd.is_none() {
            match Self::try_syscall(fd, op, buf_ptr, len, addr_ptr, addr_len) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref e) if Self::is_blocking_error(e) => {
                    // Register with the tokio reactor.
                    match tokio::io::unix::AsyncFd::new(fd) {
                        Ok(afd) => this.async_fd = Some(afd),
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        // ── Phase 2: wait for reactor readiness then retry ───────────────
        let is_read_op = matches!(op, OpCode::Read | OpCode::Accept);
        let afd = this.async_fd.as_ref().unwrap();

        let mut guard = if is_read_op {
            match afd.poll_read_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        } else {
            match afd.poll_write_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        };

        // Retry the syscall now that the fd is reportedly ready.
        match Self::try_syscall(fd, op, buf_ptr, len, addr_ptr, addr_len) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(ref e) if Self::is_blocking_error(e) => {
                // Spurious wakeup — clear the readiness flag so the reactor
                // will re-arm and we'll be polled again when truly ready.
                guard.clear_ready();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[cfg(unix)]
impl Drop for DtactIoFuture {
    fn drop(&mut self) {
        // Dropping async_fd deregisters the fd from the reactor automatically.
        // We do NOT close the fd — ownership remains with DtactTcpStream.
        drop(self.async_fd.take());
    }
}

pub struct DtactTcpStream {
    inner: tokio::net::TcpStream,
}

impl DtactTcpStream {
    pub fn from_std(stream: std::net::TcpStream) -> std::io::Result<Self> {
        stream.set_nonblocking(true)?;
        // See the equivalent comment on the native backend's
        // `from_std` — Nagle + delayed ACK stalls small request/response
        // traffic by tens to hundreds of milliseconds.
        stream.set_nodelay(true)?;
        let _guard = runtime_handle().enter();
        let inner = tokio::net::TcpStream::from_std(stream)?;
        Ok(Self { inner })
    }

    pub async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.try_read(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.readable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.try_write(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.writable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    pub async fn connect(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        let handle = runtime_handle();
        // Build the future inside the runtime context, then drop the guard before awaiting.
        let fut = {
            let _guard = handle.enter();
            tokio::net::TcpStream::connect(addr)
        };
        let inner = TokioFutureWrapper { inner: fut }.await?;
        inner.set_nodelay(true)?;
        Ok(Self { inner })
    }
}

pub struct DtactTcpListener {
    inner: tokio::net::TcpListener,
}

impl DtactTcpListener {
    pub fn from_std(listener: std::net::TcpListener) -> std::io::Result<Self> {
        listener.set_nonblocking(true)?;
        let _guard = runtime_handle().enter();
        let inner = tokio::net::TcpListener::from_std(listener)?;
        Ok(Self { inner })
    }

    pub async fn accept(&self) -> std::io::Result<(DtactTcpStream, std::net::SocketAddr)> {
        // Build the future while inside the runtime context, drop the guard before awaiting
        // so the future remains Send (EnterGuard is !Send).
        let fut = {
            let _guard = runtime_handle().enter();
            self.inner.accept()
        };
        let (stream, addr) = TokioFutureWrapper { inner: fut }.await?;
        stream.set_nodelay(true)?;
        Ok((DtactTcpStream { inner: stream }, addr))
    }
}

// =========================================================================
// COMPAT: convert DtactTcpStream to futures-io / tokio AsyncRead+AsyncWrite
// =========================================================================

/// Wraps a `DtactTcpStream` to implement standard async I/O traits:
/// - `futures_io::AsyncRead` / `futures_io::AsyncWrite`
/// - `tokio::io::AsyncRead`  / `tokio::io::AsyncWrite`
pub struct DtactCompat<T>(T);

impl<T> DtactCompat<T> {
    /// Wrap `inner` in a compat adapter.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }

    /// Unwrap back to the original type.
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Shared reference to the wrapped value.
    pub fn get_ref(&self) -> &T {
        &self.0
    }

    /// Exclusive reference to the wrapped value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Extension trait: call `.compat()` on a `DtactTcpStream` to obtain a
/// [`DtactCompat`] adapter that implements `AsyncRead`/`AsyncWrite`.
pub trait DtactCompatExt: Sized {
    fn compat(self) -> DtactCompat<Self>;
}

impl DtactCompatExt for DtactTcpStream {
    fn compat(self) -> DtactCompat<Self> {
        DtactCompat(self)
    }
}

#[cfg(unix)]
impl DtactCompatExt for DtactIoFuture {
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

// ── futures-io ──────────────────────────────────────────────────────────

impl futures_io::AsyncRead for DtactCompat<DtactTcpStream> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match this.0.inner.try_read(buf) {
                Ok(n) => return Poll::Ready(Ok(n)), // 0 == EOF, bubble up
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            match this.0.inner.poll_read_ready(cx) {
                Poll::Ready(Ok(())) => {} // re-try try_read
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl futures_io::AsyncWrite for DtactCompat<DtactTcpStream> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match this.0.inner.try_write(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            match this.0.inner.poll_write_ready(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // TCP has no user-visible flush; writes go directly to the kernel buffer.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// ── tokio::io ───────────────────────────────────────────────────────────

impl tokio::io::AsyncRead for DtactCompat<DtactTcpStream> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            let unfilled = buf.initialize_unfilled();
            match this.0.inner.try_read(unfilled) {
                Ok(0) => return Poll::Ready(Ok(())), // EOF
                Ok(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            match this.0.inner.poll_read_ready(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl tokio::io::AsyncWrite for DtactCompat<DtactTcpStream> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match this.0.inner.try_write(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
            match this.0.inner.poll_write_ready(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
