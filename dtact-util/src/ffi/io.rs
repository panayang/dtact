//! C FFI for [`crate::io`]: TCP listener + stream (bind / accept / connect
//! / read / write / close) and UDP socket (bind / `send_to` / `recv_from` /
//! connect / send / recv / close).
//!
//! The native TCP/UDP driver needs its worker runtime started before any
//! socket is used; [`dtact_util_io_init`] does that explicitly, and every
//! constructor here also lazily starts a single-worker runtime if none was
//! configured, so a caller can use the simple functions without an explicit
//! init call.

use crate::ffi::{block_on, clear_last_error, cstr_to_str, set_io_error, set_last_error};
use crate::io::{DtactTcpListener, DtactTcpStream, DtactUdpSocket};
use std::ffi::c_char;
use std::net::SocketAddr;
use std::sync::OnceLock;

static IO_INIT: OnceLock<()> = OnceLock::new();

fn ensure_io_init(workers: usize) {
    IO_INIT.get_or_init(|| {
        crate::io::init(workers.max(1));
    });
}

/// Initialize the TCP runtime with `workers` I/O worker threads. Idempotent;
/// the first call wins.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_init(workers: usize) {
    clear_last_error();
    ensure_io_init(workers);
}

fn parse_addr(s: &str) -> Option<SocketAddr> {
    s.parse::<SocketAddr>().map_or_else(
        |_| {
            set_last_error(format!("invalid socket address: {s}"));
            None
        },
        Some,
    )
}

/// Bind a TCP listener to `addr` (e.g. `"127.0.0.1:8080"`). Returns an
/// owning handle or null on error. Free with
/// [`dtact_util_io_listener_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_listener_bind(addr: *const c_char) -> *mut DtactTcpListener {
    clear_last_error();
    ensure_io_init(1);
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return std::ptr::null_mut();
    };
    let Some(addr) = parse_addr(addr) else {
        return std::ptr::null_mut();
    };
    let std_listener = match std::net::TcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            set_io_error(&e);
            return std::ptr::null_mut();
        }
    };
    match DtactTcpListener::from_std(std_listener) {
        Ok(l) => Box::into_raw(Box::new(l)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Block until a client connects, returning an owning stream handle (free
/// with [`dtact_util_io_stream_close`]) or null on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `listener` must be a
/// live handle from [`dtact_util_io_listener_bind`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_listener_accept(
    listener: *mut DtactTcpListener,
) -> *mut DtactTcpStream {
    clear_last_error();
    if listener.is_null() {
        set_last_error("null listener handle");
        return std::ptr::null_mut();
    }
    let listener = unsafe { &*listener };
    match block_on(listener.accept()) {
        Ok((stream, _addr)) => Box::into_raw(Box::new(stream)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Close and free a listener handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_listener_close(listener: *mut DtactTcpListener) {
    if !listener.is_null() {
        drop(unsafe { Box::from_raw(listener) });
    }
}

/// Connect to `addr`, blocking until connected. Returns an owning stream
/// handle or null on error. Free with [`dtact_util_io_stream_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_stream_connect(addr: *const c_char) -> *mut DtactTcpStream {
    clear_last_error();
    ensure_io_init(1);
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return std::ptr::null_mut();
    };
    let Some(addr) = parse_addr(addr) else {
        return std::ptr::null_mut();
    };
    match block_on(DtactTcpStream::connect(addr)) {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Read up to `len` bytes from `stream` into `buf`. Returns the byte count
/// read (0 = orderly shutdown by peer) or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_stream_read(
    stream: *mut DtactTcpStream,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stream.is_null() || buf.is_null() {
        set_last_error("null stream handle or buffer");
        return -1;
    }
    let stream = unsafe { &*stream };
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match block_on(stream.read(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Write up to `len` bytes from `buf` to `stream`. Returns the byte count
/// written or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_stream_write(
    stream: *mut DtactTcpStream,
    buf: *const u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stream.is_null() || buf.is_null() {
        set_last_error("null stream handle or buffer");
        return -1;
    }
    let stream = unsafe { &*stream };
    let slice = unsafe { std::slice::from_raw_parts(buf, len) };
    match block_on(stream.write(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Close and free a TCP stream handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_stream_close(stream: *mut DtactTcpStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}

// =========================================================================
// UDP
// =========================================================================

/// Bind a UDP socket to `addr` (e.g. `"127.0.0.1:8080"`, or `"0.0.0.0:0"`
/// for an ephemeral port). Returns an owning handle or null on error. Free
/// with [`dtact_util_io_udp_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_bind(addr: *const c_char) -> *mut DtactUdpSocket {
    clear_last_error();
    ensure_io_init(1);
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return std::ptr::null_mut();
    };
    let Some(addr) = parse_addr(addr) else {
        return std::ptr::null_mut();
    };
    match block_on(DtactUdpSocket::bind(addr)) {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Send `len` bytes from `buf` as a single datagram to `target` (a
/// NUL-terminated `"host:port"` string). Returns the byte count sent, or -1
/// on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `target` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_send_to(
    sock: *mut DtactUdpSocket,
    buf: *const u8,
    len: usize,
    target: *const c_char,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let Some(target) = (unsafe { cstr_to_str(target) }) else {
        return -1;
    };
    let Some(target) = parse_addr(target) else {
        return -1;
    };
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts(buf, len) };
    match block_on(sock.send_to(slice, target)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Receive a single datagram into `buf` (capacity `len`).
///
/// On success writes the sender's `"host:port"` address as a
/// NUL-terminated string into `out_addr` (capacity `out_addr_cap`,
/// truncated if it doesn't fit) and returns the byte count received;
/// returns -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `out_addr`, if
/// non-null, must point to at least `out_addr_cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_recv_from(
    sock: *mut DtactUdpSocket,
    buf: *mut u8,
    len: usize,
    out_addr: *mut c_char,
    out_addr_cap: usize,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match block_on(sock.recv_from(slice)) {
        Ok((n, from)) => {
            write_addr_out(from, out_addr, out_addr_cap);
            n as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Connect `sock` to `addr` so [`dtact_util_io_udp_send`]/
/// [`dtact_util_io_udp_recv`] can omit the peer address. Returns 0 on
/// success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_connect(
    sock: *mut DtactUdpSocket,
    addr: *const c_char,
) -> i32 {
    clear_last_error();
    if sock.is_null() {
        set_last_error("null socket handle");
        return -1;
    }
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return -1;
    };
    let Some(addr) = parse_addr(addr) else {
        return -1;
    };
    let sock = unsafe { &*sock };
    match block_on(sock.connect(addr)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Send `len` bytes from `buf` to the connected peer (see
/// [`dtact_util_io_udp_connect`]). Returns the byte count sent, or -1 on
/// error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_send(
    sock: *mut DtactUdpSocket,
    buf: *const u8,
    len: usize,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts(buf, len) };
    match block_on(sock.send(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Receive a datagram from the connected peer into `buf`. Returns the byte
/// count received, or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_recv(
    sock: *mut DtactUdpSocket,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match block_on(sock.recv(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Close and free a UDP socket handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_udp_close(sock: *mut DtactUdpSocket) {
    if !sock.is_null() {
        drop(unsafe { Box::from_raw(sock) });
    }
}

/// Write `addr` as a NUL-terminated string into `out` (capacity `cap`),
/// truncating (but always NUL-terminating, if `cap > 0`) rather than
/// overflowing. A null or zero-capacity `out` is a silent no-op — the
/// caller just doesn't get the address back.
fn write_addr_out(addr: SocketAddr, out: *mut c_char, cap: usize) {
    if out.is_null() || cap == 0 {
        return;
    }
    let s = addr.to_string();
    let bytes = s.as_bytes();
    let n = bytes.len().min(cap - 1);
    // SAFETY: caller contract (see this function's callers' `# Safety`
    // sections) guarantees `out` points to at least `cap` writable bytes;
    // `n < cap`, and we NUL-terminate at `n`, leaving room for it.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), out, n);
        *out.add(n) = 0;
    }
}

// =========================================================================
// Unix domain sockets (stream listener/stream, and datagram) — Unix only,
// same shape as the TCP/UDP surface above. See `crate::io::DtactUnixStream`
// et al.'s own docs for the native/tokio backend split.
// =========================================================================

#[cfg(unix)]
use crate::io::{DtactUnixDatagram, DtactUnixListener, DtactUnixStream};

/// Bind a Unix-domain-socket listener to the filesystem path `addr`.
/// Returns an owning handle or null on error. Free with
/// [`dtact_util_io_unix_listener_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_listener_bind(
    addr: *const c_char,
) -> *mut DtactUnixListener {
    clear_last_error();
    ensure_io_init(1);
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return std::ptr::null_mut();
    };
    match DtactUnixListener::bind(addr) {
        Ok(l) => Box::into_raw(Box::new(l)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Block until a client connects, returning an owning stream handle (free
/// with [`dtact_util_io_unix_stream_close`]) or null on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `listener` must
/// be a live handle from [`dtact_util_io_unix_listener_bind`].
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_listener_accept(
    listener: *mut DtactUnixListener,
) -> *mut DtactUnixStream {
    clear_last_error();
    if listener.is_null() {
        set_last_error("null listener handle");
        return std::ptr::null_mut();
    }
    let listener = unsafe { &*listener };
    match block_on(listener.accept()) {
        Ok((stream, _addr)) => Box::into_raw(Box::new(stream)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Close and free a Unix-domain-socket listener handle. Passing null is a
/// no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_listener_close(listener: *mut DtactUnixListener) {
    if !listener.is_null() {
        drop(unsafe { Box::from_raw(listener) });
    }
}

/// Connect to the Unix-domain-socket path `addr`, blocking until
/// connected. Returns an owning stream handle or null on error. Free with
/// [`dtact_util_io_unix_stream_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_stream_connect(
    addr: *const c_char,
) -> *mut DtactUnixStream {
    clear_last_error();
    ensure_io_init(1);
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return std::ptr::null_mut();
    };
    match block_on(DtactUnixStream::connect(addr)) {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Read up to `len` bytes from `stream` into `buf`. Returns the byte
/// count read (0 = orderly shutdown by peer) or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_stream_read(
    stream: *mut DtactUnixStream,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stream.is_null() || buf.is_null() {
        set_last_error("null stream handle or buffer");
        return -1;
    }
    let stream = unsafe { &*stream };
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match block_on(stream.read(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Write up to `len` bytes from `buf` to `stream`. Returns the byte count
/// written or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_stream_write(
    stream: *mut DtactUnixStream,
    buf: *const u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stream.is_null() || buf.is_null() {
        set_last_error("null stream handle or buffer");
        return -1;
    }
    let stream = unsafe { &*stream };
    let slice = unsafe { std::slice::from_raw_parts(buf, len) };
    match block_on(stream.write(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Fetch `stream`'s connected peer's credentials into `out_uid`/
/// `out_gid`/`out_pid`.
///
/// `out_pid` is set to -1 on platforms that don't report a PID — see
/// [`crate::io::DtactUnixStream::peer_cred`]. Any of the three output
/// pointers may be null to skip that field. Returns 0 on success, -1 on
/// error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `out_uid`/
/// `out_gid`/`out_pid`, if non-null, must each point to one writable
/// value of the matching type.
#[cfg(unix)]
#[allow(clippy::similar_names)] // `out_uid`/`out_gid`/`out_pid` are intentionally parallel
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_stream_peer_cred(
    stream: *mut DtactUnixStream,
    out_uid: *mut u32,
    out_gid: *mut u32,
    out_pid: *mut i32,
) -> i32 {
    clear_last_error();
    if stream.is_null() {
        set_last_error("null stream handle");
        return -1;
    }
    let stream = unsafe { &*stream };
    match stream.peer_cred() {
        Ok(cred) => {
            if !out_uid.is_null() {
                unsafe { *out_uid = cred.uid() };
            }
            if !out_gid.is_null() {
                unsafe { *out_gid = cred.gid() };
            }
            if !out_pid.is_null() {
                unsafe { *out_pid = cred.pid().unwrap_or(-1) };
            }
            0
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Close and free a Unix-domain-socket stream handle. Passing null is a
/// no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_stream_close(stream: *mut DtactUnixStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}

/// Bind a Unix-domain datagram socket to the filesystem path `addr`.
/// Returns an owning handle or null on error. Free with
/// [`dtact_util_io_unix_datagram_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_bind(
    addr: *const c_char,
) -> *mut DtactUnixDatagram {
    clear_last_error();
    ensure_io_init(1);
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return std::ptr::null_mut();
    };
    match DtactUnixDatagram::bind(addr) {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Send `len` bytes from `buf` as a single datagram to the socket bound
/// at `target`. Returns the byte count sent, or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `target` must be
/// a valid NUL-terminated C string.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_send_to(
    sock: *mut DtactUnixDatagram,
    buf: *const u8,
    len: usize,
    target: *const c_char,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let Some(target) = (unsafe { cstr_to_str(target) }) else {
        return -1;
    };
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts(buf, len) };
    match block_on(sock.send_to(slice, target)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Receive a single datagram into `buf` (capacity `len`).
///
/// On success writes the sender's path (or an empty string if unnamed —
/// see [`crate::io::DtactUnixSocketAddr::is_unnamed`]) as a
/// NUL-terminated string into `out_addr` (capacity `out_addr_cap`,
/// truncated if it doesn't fit) and returns the byte count received;
/// returns -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `out_addr`, if
/// non-null, must point to at least `out_addr_cap` writable bytes.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_recv_from(
    sock: *mut DtactUnixDatagram,
    buf: *mut u8,
    len: usize,
    out_addr: *mut c_char,
    out_addr_cap: usize,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match block_on(sock.recv_from(slice)) {
        Ok((n, from)) => {
            let path_str = from
                .as_pathname()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            write_str_bytes_out(path_str.as_bytes(), out_addr, out_addr_cap);
            n as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Connect `sock` to the path `addr` so
/// [`dtact_util_io_unix_datagram_send`]/
/// [`dtact_util_io_unix_datagram_recv`] can omit the peer address.
/// Returns 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `addr` must be a
/// valid NUL-terminated C string.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_connect(
    sock: *mut DtactUnixDatagram,
    addr: *const c_char,
) -> i32 {
    clear_last_error();
    if sock.is_null() {
        set_last_error("null socket handle");
        return -1;
    }
    let Some(addr) = (unsafe { cstr_to_str(addr) }) else {
        return -1;
    };
    let sock = unsafe { &*sock };
    match block_on(sock.connect(addr)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Send `len` bytes from `buf` to the connected peer (see
/// [`dtact_util_io_unix_datagram_connect`]). Returns the byte count sent,
/// or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_send(
    sock: *mut DtactUnixDatagram,
    buf: *const u8,
    len: usize,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts(buf, len) };
    match block_on(sock.send(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Receive a datagram from the connected peer into `buf`. Returns the
/// byte count received, or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_recv(
    sock: *mut DtactUnixDatagram,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if sock.is_null() || buf.is_null() {
        set_last_error("null socket handle or buffer");
        return -1;
    }
    let sock = unsafe { &*sock };
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match block_on(sock.recv(slice)) {
        Ok(n) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Close and free a Unix-domain datagram socket handle. Passing null is a
/// no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_io_unix_datagram_close(sock: *mut DtactUnixDatagram) {
    if !sock.is_null() {
        drop(unsafe { Box::from_raw(sock) });
    }
}

/// Write `bytes` as a NUL-terminated string into `out` (capacity `cap`),
/// truncating (but always NUL-terminating, if `cap > 0`) rather than
/// overflowing. A null or zero-capacity `out` is a silent no-op.
#[cfg(unix)]
fn write_str_bytes_out(bytes: &[u8], out: *mut c_char, cap: usize) {
    if out.is_null() || cap == 0 {
        return;
    }
    let n = bytes.len().min(cap - 1);
    // SAFETY: caller contract (see this function's callers' `# Safety`
    // sections) guarantees `out` points to at least `cap` writable bytes;
    // `n < cap`, and we NUL-terminate at `n`, leaving room for it.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), out, n);
        *out.add(n) = 0;
    }
}
