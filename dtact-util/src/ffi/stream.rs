//! C FFI for [`crate::stream`]: in-process duplex byte pipes.

use crate::ffi::{block_on, clear_last_error, set_io_error, set_last_error};
use crate::stream::{DtactStream, pair};

/// Create a connected pair of duplex streams.
///
/// Each side buffers `capacity` bytes per direction (rounded up to a power
/// of two). On success writes two owning handles into `out_a` / `out_b` and
/// returns 0; on error returns -1 and records a message.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `out_a` and `out_b`
/// must be non-null, writable, and point to storage for one pointer each.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_stream_pair_create(
    capacity: usize,
    out_a: *mut *mut DtactStream,
    out_b: *mut *mut DtactStream,
) -> i32 {
    clear_last_error();
    if out_a.is_null() || out_b.is_null() {
        set_last_error("null out-pointer");
        return -1;
    }
    let (a, b) = pair(capacity);
    unsafe {
        *out_a = Box::into_raw(Box::new(a));
        *out_b = Box::into_raw(Box::new(b));
    }
    0
}

/// Read up to `len` bytes from `stream` into `buf`. Returns the number of
/// bytes read (0 = EOF, peer's write half dropped), or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_stream_read(
    stream: *mut DtactStream,
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

/// Write up to `len` bytes from `buf` into `stream`. Returns the number of
/// bytes written, or -1 on error (e.g. the peer dropped its read half).
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_stream_write(
    stream: *mut DtactStream,
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

/// Free (close) a stream endpoint. Dropping it lets the peer observe EOF on
/// its next read. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_stream_free(stream: *mut DtactStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}
