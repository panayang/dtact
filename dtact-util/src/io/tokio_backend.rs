use super::{Context, Future, Pin, Poll};

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
///
/// # Panics
///
/// Panics if building the underlying `tokio::runtime::Runtime` fails
/// (e.g. the OS refuses to spawn its worker threads).
pub fn init_runtime(
    workers: usize,
    _ring_depth: u32,
    _buffer_pool_size: usize,
    _chunk_size: usize,
    _pin_cpus: &[usize],
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
    init_runtime(workers, 0, 0, 0, &[]);
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
#[must_use]
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
/// Which async operation a [`DtactIoFuture`] represents.
#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpCode {
    /// A socket read.
    Read,
    /// A socket write.
    Write,
    /// Accept a new connection on a listening socket.
    Accept,
    /// Connect to a remote address.
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
    /// Ignored by this backend — present for API compatibility with the
    /// native backend's field of the same name.
    pub worker_idx: usize,
    /// The raw fd this op operates on.
    pub fd: u32,
    /// Ignored by this backend — present for API compatibility with the
    /// native backend's field of the same name.
    pub direct_fd_idx: u32,
    /// Which op this future performs.
    pub op: OpCode,
    /// Read/Write only: pointer to the caller-supplied buffer.
    pub buf_ptr: *mut u8,
    /// Read/Write only: length of the buffer at `buf_ptr`.
    pub len: usize,
    /// Unused by this backend (no positional read/write here); always `0`.
    pub offset: i64,
    /// Connect only: the remote address to connect to.
    pub addr: Option<libc::sockaddr_storage>,
    /// Connect only: byte length of the valid prefix of `addr`.
    pub addr_len: libc::socklen_t,
    /// Ignored by this backend — present for API compatibility with the
    /// native backend's field of the same name.
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
    /// Build a not-yet-submitted future for the given op. The underlying
    /// syscall is attempted lazily on first [`Future::poll`], not here.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
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
            OpCode::Read => unsafe { libc::read(fd, buf_ptr.cast::<libc::c_void>(), len) },
            OpCode::Write => unsafe { libc::write(fd, buf_ptr.cast::<libc::c_void>(), len) },
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
                        (&raw mut err).cast::<libc::c_void>(),
                        &raw mut err_len,
                    )
                };
                if r == 0 && err != 0 {
                    return Err(std::io::Error::from_raw_os_error(err));
                }

                let r = unsafe { libc::connect(fd, addr.cast::<libc::sockaddr>(), addr_len) };
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
            .map_or(std::ptr::null(), std::ptr::from_ref);
        let addr_len = this.addr_len;

        // ── Phase 1: first attempt, no registration yet ─────────────────
        //
        // Entering the runtime context here used to wrap the *entire*
        // poll, including this phase's syscall attempt — which never
        // touches tokio at all (`try_syscall` is a raw `libc` call) and,
        // for any op that completes without blocking, was the only work
        // this poll did. `AsyncFd::new` (below) does need an active
        // `Handle` (it calls `Handle::current()` internally to register
        // with the reactor), so entry is still required there — just
        // scoped to the one call that needs it instead of every poll,
        // including the common no-reactor-needed fast path.
        if this.async_fd.is_none() {
            match Self::try_syscall(fd, op, buf_ptr, len, addr_ptr, addr_len) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref e) if Self::is_blocking_error(e) => {
                    // Register with the tokio reactor.
                    let afd = {
                        let _guard = runtime_handle().enter();
                        tokio::io::unix::AsyncFd::new(fd)
                    };
                    match afd {
                        Ok(afd) => this.async_fd = Some(afd),
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        // ── Phase 2 doesn't re-enter the runtime ─────────────────────────
        // `AsyncFd` captured its own driver `Handle` at registration time
        // above (`poll_read_ready`/`poll_write_ready` read that stored
        // handle, not `Handle::current()`), so no thread-local runtime
        // context is needed here — verified against `future_test.rs`'s
        // `test_io_future_complex`, which drives this future from a
        // from-scratch executor that never itself enters the tokio
        // runtime, only relying on whatever this `poll` does internally.

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

/// Tokio-backed TCP stream. Mirrors the native backend's `DtactTcpStream`
/// API surface, but drives readiness through tokio's reactor instead of the
/// crate's own `IOCP`/`io_uring`/kqueue driver.
pub struct DtactTcpStream {
    inner: tokio::net::TcpStream,
}

impl DtactTcpStream {
    /// Wrap an existing `std::net::TcpStream`, switching it to non-blocking
    /// mode and disabling Nagle's algorithm.
    ///
    /// # Errors
    /// Returns an error if the OS refuses to set the socket non-blocking or
    /// disable `TCP_NODELAY` (e.g. an already-closed or invalid socket).
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

    /// Read into `buf`, waiting on tokio's reactor for readability between
    /// `WouldBlock` retries rather than busy-polling.
    ///
    /// # Errors
    /// Returns any I/O error surfaced by the underlying socket other than
    /// `WouldBlock`, which is retried internally.
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

    /// Write `buf`, waiting on tokio's reactor for writability between
    /// `WouldBlock` retries rather than busy-polling.
    ///
    /// # Errors
    /// Returns any I/O error surfaced by the underlying socket other than
    /// `WouldBlock`, which is retried internally.
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

    /// Connect to `addr`, disabling Nagle's algorithm once established.
    ///
    /// # Errors
    /// Returns any error from `tokio::net::TcpStream::connect` (refused
    /// connection, timeout at the OS level, unreachable host, etc.) or from
    /// setting `TCP_NODELAY` afterward.
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

impl crate::io::AsyncRead for DtactTcpStream {
    async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read(buf).await
    }
}

impl crate::io::AsyncWrite for DtactTcpStream {
    async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.write(buf).await
    }
}

/// Tokio-backed TCP listener. Mirrors the native backend's
/// `DtactTcpListener` API surface.
pub struct DtactTcpListener {
    inner: tokio::net::TcpListener,
}

impl DtactTcpListener {
    /// Wrap an existing `std::net::TcpListener`, switching it to
    /// non-blocking mode.
    ///
    /// # Errors
    /// Returns an error if the OS refuses to set the socket non-blocking.
    pub fn from_std(listener: std::net::TcpListener) -> std::io::Result<Self> {
        listener.set_nonblocking(true)?;
        let _guard = runtime_handle().enter();
        let inner = tokio::net::TcpListener::from_std(listener)?;
        Ok(Self { inner })
    }

    /// Accept a single incoming connection, disabling Nagle's algorithm on
    /// the accepted stream.
    ///
    /// # Errors
    /// Returns any error surfaced by the OS while accepting (e.g. the
    /// listener was closed, or a transient per-connection accept failure).
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
// HIGH-LEVEL API: DtactUnixStream / DtactUnixListener  (tokio backend)
// =========================================================================
// Unix-only (matches `tokio::net::unix`'s own availability, and is broader
// than the native backend's Linux-only `DtactUnixStream`/`DtactUnixListener`
// — see `io::native`'s module doc for why macOS/BSD isn't wired up there
// yet). Thin wrappers over `tokio::net::UnixStream`/`UnixListener`, same
// shape as `DtactTcpStream`/`DtactTcpListener` above minus `TCP_NODELAY`
// (no Unix-domain-socket equivalent to disable).

/// Tokio-backed Unix-domain-socket stream. Mirrors the native backend's
/// `DtactUnixStream` API surface.
#[cfg(unix)]
pub struct DtactUnixStream {
    inner: tokio::net::UnixStream,
}

#[cfg(unix)]
impl DtactUnixStream {
    /// Wrap an existing `std::os::unix::net::UnixStream`, switching it to
    /// non-blocking mode.
    ///
    /// # Errors
    /// Returns an error if the OS refuses to set the socket non-blocking.
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> std::io::Result<Self> {
        stream.set_nonblocking(true)?;
        let _guard = runtime_handle().enter();
        let inner = tokio::net::UnixStream::from_std(stream)?;
        Ok(Self { inner })
    }

    /// Read into `buf`, waiting on tokio's reactor for readability between
    /// `WouldBlock` retries rather than busy-polling.
    ///
    /// # Errors
    /// Returns any I/O error surfaced by the underlying socket other than
    /// `WouldBlock`, which is retried internally.
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

    /// Write `buf`, waiting on tokio's reactor for writability between
    /// `WouldBlock` retries rather than busy-polling.
    ///
    /// # Errors
    /// Returns any I/O error surfaced by the underlying socket other than
    /// `WouldBlock`, which is retried internally.
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

    /// Connect to the filesystem path `path`.
    ///
    /// # Errors
    /// Returns any error from `tokio::net::UnixStream::connect` (e.g.
    /// `NotFound`/`ConnectionRefused` if nothing is listening at `path`).
    pub async fn connect(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let handle = runtime_handle();
        let fut = {
            let _guard = handle.enter();
            tokio::net::UnixStream::connect(path)
        };
        let inner = TokioFutureWrapper { inner: fut }.await?;
        Ok(Self { inner })
    }

    /// The connected peer's credentials (PID/UID/GID). Thin wrapper over
    /// `tokio::net::UnixStream::peer_cred` — a plain synchronous syscall
    /// under the hood, not routed through the reactor.
    ///
    /// # Errors
    /// Returns an `io::Error` if the underlying syscall fails (e.g. the
    /// socket was closed concurrently).
    pub fn peer_cred(&self) -> std::io::Result<tokio::net::unix::UCred> {
        self.inner.peer_cred()
    }
}

#[cfg(unix)]
impl crate::io::AsyncRead for DtactUnixStream {
    async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read(buf).await
    }
}

#[cfg(unix)]
impl crate::io::AsyncWrite for DtactUnixStream {
    async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.write(buf).await
    }
}

/// Tokio-backed Unix-domain-socket listener. Mirrors the native backend's
/// `DtactUnixListener` API surface.
#[cfg(unix)]
pub struct DtactUnixListener {
    inner: tokio::net::UnixListener,
}

#[cfg(unix)]
impl DtactUnixListener {
    /// Bind a new listener to the filesystem path `path`. `path` must not
    /// already exist — like `std::os::unix::net::UnixListener::bind`,
    /// this does not remove a stale socket file left behind by a
    /// previous run.
    ///
    /// # Errors
    /// Returns any error from binding the underlying OS socket (e.g.
    /// `AddrInUse` if `path` already exists).
    pub fn bind(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let _guard = runtime_handle().enter();
        let inner = tokio::net::UnixListener::bind(path)?;
        Ok(Self { inner })
    }

    /// Wrap an existing `std::os::unix::net::UnixListener`, switching it
    /// to non-blocking mode.
    ///
    /// # Errors
    /// Returns an error if the OS refuses to set the socket non-blocking.
    pub fn from_std(listener: std::os::unix::net::UnixListener) -> std::io::Result<Self> {
        listener.set_nonblocking(true)?;
        let _guard = runtime_handle().enter();
        let inner = tokio::net::UnixListener::from_std(listener)?;
        Ok(Self { inner })
    }

    /// Accept a single incoming connection.
    ///
    /// # Errors
    /// Returns any error surfaced by the OS while accepting (e.g. the
    /// listener was closed, or a transient per-connection accept failure).
    pub async fn accept(&self) -> std::io::Result<(DtactUnixStream, tokio::net::unix::SocketAddr)> {
        let fut = {
            let _guard = runtime_handle().enter();
            self.inner.accept()
        };
        let (stream, addr) = TokioFutureWrapper { inner: fut }.await?;
        Ok((DtactUnixStream { inner: stream }, addr))
    }
}

// =========================================================================
// HIGH-LEVEL API: DtactUdpSocket  (tokio backend)
// =========================================================================

/// Async UDP socket — tokio-backend equivalent of the native
/// `DtactUdpSocket`, a thin wrapper over [`tokio::net::UdpSocket`].
///
/// Mirrors the connectionless (`send_to`/`recv_from`) and connected
/// (`connect`/`send`/`recv`) halves of `std::net::UdpSocket`'s and
/// `tokio::net::UdpSocket`'s API so call-sites port across backends with a
/// single feature flag.
pub struct DtactUdpSocket {
    inner: tokio::net::UdpSocket,
}

impl DtactUdpSocket {
    /// Bind a new UDP socket to `addr`.
    ///
    /// # Errors
    /// Returns any error from binding the underlying OS socket (e.g. the
    /// address is already in use) or from registering it with the reactor.
    pub fn bind(addr: std::net::SocketAddr) -> impl Future<Output = std::io::Result<Self>> {
        std::future::ready(std::net::UdpSocket::bind(addr).and_then(Self::from_std))
    }

    /// Register an existing (already-bound) `std::net::UdpSocket` with the
    /// driver, taking ownership of it.
    ///
    /// # Errors
    /// Returns any error from switching the socket to non-blocking mode or
    /// registering it with the tokio reactor.
    pub fn from_std(socket: std::net::UdpSocket) -> std::io::Result<Self> {
        socket.set_nonblocking(true)?;
        let _guard = runtime_handle().enter();
        let inner = tokio::net::UdpSocket::from_std(socket)?;
        Ok(Self { inner })
    }

    /// The local address this socket is bound to.
    ///
    /// # Errors
    /// Returns any error from the underlying `getsockname` call.
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }

    /// Send `buf` as a single datagram to `target`, returning the number of
    /// bytes sent.
    ///
    /// # Errors
    /// Returns any error from the underlying `sendto`.
    pub async fn send_to(
        &self,
        buf: &[u8],
        target: std::net::SocketAddr,
    ) -> std::io::Result<usize> {
        loop {
            match self.inner.try_send_to(buf, target) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.writable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    /// Receive a single datagram into `buf`, returning the byte count and
    /// the peer address it came from.
    ///
    /// # Errors
    /// Returns any error from the underlying `recvfrom`.
    pub async fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> std::io::Result<(usize, std::net::SocketAddr)> {
        loop {
            match self.inner.try_recv_from(buf) {
                Ok(pair) => return Ok(pair),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.readable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    /// Connect this socket to `addr` so [`send`](Self::send)/[`recv`](Self::recv)
    /// can be used without repeating the peer address.
    ///
    /// # Errors
    /// Returns any error from the underlying `connect`.
    pub async fn connect(&self, addr: std::net::SocketAddr) -> std::io::Result<()> {
        let fut = {
            let _guard = runtime_handle().enter();
            self.inner.connect(addr)
        };
        TokioFutureWrapper { inner: fut }.await
    }

    /// Send `buf` to the connected peer (see [`connect`](Self::connect)),
    /// returning the number of bytes sent.
    ///
    /// # Errors
    /// Returns any error from the underlying `send`, including if the socket
    /// is not connected.
    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.try_send(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.writable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    /// Receive a datagram from the connected peer into `buf`, returning the
    /// byte count.
    ///
    /// # Errors
    /// Returns any error from the underlying `recv`, including if the socket
    /// is not connected.
    pub async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.try_recv(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.readable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }
}

/// Async Unix-domain datagram socket — tokio-backend equivalent of the
/// native `DtactUnixDatagram`, a thin wrapper over
/// [`tokio::net::UnixDatagram`].
///
/// Mirrors [`DtactUdpSocket`]'s connectionless/connected split. Unlike
/// the native backend (which has to hand-parse a raw `sockaddr_un` into
/// its own address type), `recv_from` here just returns tokio's own
/// `tokio::net::unix::SocketAddr` directly.
#[cfg(unix)]
pub struct DtactUnixDatagram {
    inner: tokio::net::UnixDatagram,
}

#[cfg(unix)]
impl DtactUnixDatagram {
    /// Bind a new datagram socket to the filesystem path `path`.
    ///
    /// # Errors
    /// Returns any error from binding the underlying OS socket or
    /// registering it with the reactor.
    pub fn bind(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let _guard = runtime_handle().enter();
        let inner = tokio::net::UnixDatagram::bind(path)?;
        Ok(Self { inner })
    }

    /// Create an unbound datagram socket (matches
    /// `tokio::net::UnixDatagram::unbound`).
    ///
    /// # Errors
    /// Returns any error from creating the underlying OS socket or
    /// registering it with the reactor.
    pub fn unbound() -> std::io::Result<Self> {
        let _guard = runtime_handle().enter();
        let inner = tokio::net::UnixDatagram::unbound()?;
        Ok(Self { inner })
    }

    /// Send `buf` as a single datagram to the socket bound at `target`,
    /// returning the number of bytes sent.
    ///
    /// # Errors
    /// Returns any error from the underlying `sendto`.
    pub async fn send_to(
        &self,
        buf: &[u8],
        target: impl AsRef<std::path::Path>,
    ) -> std::io::Result<usize> {
        loop {
            match self.inner.try_send_to(buf, target.as_ref()) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.writable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    /// Receive a single datagram into `buf`, returning the byte count and
    /// the peer address it came from.
    ///
    /// # Errors
    /// Returns any error from the underlying `recvfrom`.
    pub async fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> std::io::Result<(usize, tokio::net::unix::SocketAddr)> {
        loop {
            match self.inner.try_recv_from(buf) {
                Ok(pair) => return Ok(pair),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.readable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    /// Connect this socket to the path `target` so
    /// [`send`](Self::send)/[`recv`](Self::recv) can omit the peer
    /// address.
    ///
    /// # Errors
    /// Returns any error from the underlying `connect`.
    pub async fn connect(&self, target: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        self.inner.connect(target)
    }

    /// Send `buf` to the connected peer, returning the number of bytes
    /// sent.
    ///
    /// # Errors
    /// Returns any error from the underlying `send`, including if the
    /// socket is not connected.
    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.try_send(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.writable();
            TokioFutureWrapper { inner: fut }.await?;
        }
    }

    /// Receive a datagram from the connected peer into `buf`, returning
    /// the byte count.
    ///
    /// # Errors
    /// Returns any error from the underlying `recv`, including if the
    /// socket is not connected.
    pub async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.try_recv(buf) {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let fut = self.inner.readable();
            TokioFutureWrapper { inner: fut }.await?;
        }
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
    pub const fn new(inner: T) -> Self {
        Self(inner)
    }

    /// Unwrap back to the original type.
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Shared reference to the wrapped value.
    pub const fn get_ref(&self) -> &T {
        &self.0
    }

    /// Exclusive reference to the wrapped value.
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Extension trait: call `.compat()` on a `DtactTcpStream` to obtain a
/// [`DtactCompat`] adapter that implements `AsyncRead`/`AsyncWrite`.
pub trait DtactCompatExt: Sized {
    /// Wrap `self` in a [`DtactCompat`] adapter that implements the
    /// standard `AsyncRead`/`AsyncWrite` traits.
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

/// Resolve `host` (a `"host:port"` string) to one or more `SocketAddr`s.
///
/// Via tokio's own async resolver, without blocking the calling task's
/// thread. Thin wrapper over `tokio::net::lookup_host` — matches the
/// native backend's `lookup_host` signature (a plain iterator, not
/// tokio's own `LookupHost` type) so call-sites port across backends with
/// a single feature flag.
///
/// # Errors
/// Returns whatever `tokio::net::lookup_host` returns (e.g. `host:port`
/// doesn't parse, or the name doesn't resolve).
pub async fn lookup_host(
    host: impl tokio::net::ToSocketAddrs,
) -> std::io::Result<impl Iterator<Item = std::net::SocketAddr>> {
    let fut = {
        let _guard = runtime_handle().enter();
        tokio::net::lookup_host(host)
    };
    let resolved = TokioFutureWrapper { inner: fut }.await?;
    Ok(resolved.collect::<Vec<_>>().into_iter())
}

// =========================================================================
// HIGH-LEVEL API: named pipes  (tokio backend, Windows only)
// =========================================================================
// Thin wrappers over `tokio::net::windows::named_pipe`, mirroring the
// native backend's `DtactNamedPipeServer`/`DtactNamedPipeClient` shape
// (including the "no persistent listener, create one server instance per
// client" semantics — see that module's doc for the rationale, identical
// here since it's inherent to Windows named pipes, not a native-backend
// simplification).

/// One named-pipe connection — the read/write half shared by
/// [`DtactNamedPipeServer`] (after a client connects) and
/// [`DtactNamedPipeClient`].
#[cfg(windows)]
pub struct DtactNamedPipeHandle {
    inner: NamedPipeInner,
}

#[cfg(windows)]
enum NamedPipeInner {
    Server(tokio::net::windows::named_pipe::NamedPipeServer),
    Client(tokio::net::windows::named_pipe::NamedPipeClient),
}

#[cfg(windows)]
impl DtactNamedPipeHandle {
    /// Read into `buf`, returning the number of bytes read (`0` = the
    /// peer closed its end).
    ///
    /// # Errors
    /// Returns any I/O error surfaced by the underlying pipe other than
    /// `WouldBlock`, which is retried internally.
    pub async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let result = match &self.inner {
                NamedPipeInner::Server(s) => s.try_read(buf),
                NamedPipeInner::Client(c) => c.try_read(buf),
            };
            match result {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            // Awaited inside each arm (not hoisted into a shared `let
            // fut = match ...`) because `NamedPipeServer::readable()` and
            // `NamedPipeClient::readable()` return distinct opaque
            // `impl Future` types — `TokioFutureWrapper` of one doesn't
            // unify with `TokioFutureWrapper` of the other even though
            // both eventually resolve to the same `io::Result<()>`.
            match &self.inner {
                NamedPipeInner::Server(s) => {
                    TokioFutureWrapper {
                        inner: s.readable(),
                    }
                    .await?
                }
                NamedPipeInner::Client(c) => {
                    TokioFutureWrapper {
                        inner: c.readable(),
                    }
                    .await?
                }
            }
        }
    }

    /// Write from `buf`, returning the number of bytes written.
    ///
    /// # Errors
    /// Returns any I/O error surfaced by the underlying pipe other than
    /// `WouldBlock`, which is retried internally.
    pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            let result = match &self.inner {
                NamedPipeInner::Server(s) => s.try_write(buf),
                NamedPipeInner::Client(c) => c.try_write(buf),
            };
            match result {
                Ok(n) => return Ok(n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            // See the matching comment in `read` above for why this is
            // awaited per-arm rather than hoisted out of the `match`.
            match &self.inner {
                NamedPipeInner::Server(s) => {
                    TokioFutureWrapper {
                        inner: s.writable(),
                    }
                    .await?
                }
                NamedPipeInner::Client(c) => {
                    TokioFutureWrapper {
                        inner: c.writable(),
                    }
                    .await?
                }
            }
        }
    }
}

#[cfg(windows)]
impl crate::io::AsyncRead for DtactNamedPipeHandle {
    async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read(buf).await
    }
}

#[cfg(windows)]
impl crate::io::AsyncWrite for DtactNamedPipeHandle {
    async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.write(buf).await
    }
}

/// A single named-pipe server instance, before a client has connected.
///
/// Create one per client you intend to accept — see this module's doc.
#[cfg(windows)]
pub struct DtactNamedPipeServer {
    inner: tokio::net::windows::named_pipe::NamedPipeServer,
}

#[cfg(windows)]
impl DtactNamedPipeServer {
    /// Create a new duplex, byte-mode pipe instance named `name` (e.g.
    /// `r"\\.\pipe\my-app"`).
    ///
    /// # Errors
    /// Returns an `io::Error` if the underlying `CreateNamedPipeW` fails
    /// (e.g. `name` is malformed).
    pub fn create(name: &str) -> std::io::Result<Self> {
        let _guard = runtime_handle().enter();
        let inner = tokio::net::windows::named_pipe::ServerOptions::new().create(name)?;
        Ok(Self { inner })
    }

    /// Wait for a client to connect to this pipe instance.
    ///
    /// # Errors
    /// Returns an `io::Error` if the underlying `ConnectNamedPipe`
    /// completion reports one.
    pub async fn connect(self) -> std::io::Result<DtactNamedPipeHandle> {
        let fut = {
            let _guard = runtime_handle().enter();
            self.inner.connect()
        };
        TokioFutureWrapper { inner: fut }.await?;
        Ok(DtactNamedPipeHandle {
            inner: NamedPipeInner::Server(self.inner),
        })
    }
}

/// A named-pipe client. Connects to an already-`create`d server instance
/// by name.
#[cfg(windows)]
pub struct DtactNamedPipeClient;

#[cfg(windows)]
impl DtactNamedPipeClient {
    /// Connect to the server pipe instance named `name`, retrying while
    /// every existing instance is busy — `tokio::net::windows::named_pipe`
    /// already implements this retry internally in `ClientOptions::open`.
    ///
    /// # Errors
    /// Returns an `io::Error` if the underlying `CreateFileW` fails for
    /// any reason other than transient busy (e.g. `NotFound` if no server
    /// is listening at `name` at all).
    ///
    /// # Panics
    /// Panics if the OS refuses to spawn the connect-retry thread (fatal
    /// resource exhaustion) — same class of failure every other native
    /// backend in this crate treats as unrecoverable at thread-spawn time.
    pub async fn connect(name: &str) -> std::io::Result<DtactNamedPipeHandle> {
        let (tx, rx) = crate::sync::oneshot::channel();
        let name_owned = name.to_string();
        let handle_rt = runtime_handle();
        // `ClientOptions::open`'s internal retry-on-busy loop blocks the
        // calling thread, so run it on a throwaway one rather than
        // stalling the awaiting task's thread — same rationale as
        // `crate::io::lookup_host`. `open()` itself must run inside the
        // tokio runtime context to register the resulting handle with
        // tokio's reactor, hence entering `handle_rt` on this thread too.
        std::thread::Builder::new()
            .name("dtact-io-namedpipe-connect".into())
            .spawn(move || {
                let _guard = handle_rt.enter();
                let result =
                    tokio::net::windows::named_pipe::ClientOptions::new().open(&name_owned);
                let _ = tx.send(result);
            })
            .expect("failed to spawn dtact-io named-pipe connect thread");
        let inner = rx.await.unwrap_or_else(|_| {
            Err(std::io::Error::other(
                "dtact-io: named-pipe connect thread panicked before sending a result",
            ))
        })?;
        Ok(DtactNamedPipeHandle {
            inner: NamedPipeInner::Client(inner),
        })
    }
}
