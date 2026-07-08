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
