//! C FFI for [`crate::fs`]: create / open / read / write / sync / close of
//! a [`DtactFile`].

use crate::ffi::{block_on, clear_last_error, cstr_to_str, set_io_error, set_last_error};
use crate::fs::DtactFile;
use std::ffi::c_char;

/// Initialize the fs backend with `workers` worker threads. Idempotent.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_init(workers: usize) {
    clear_last_error();
    crate::fs::init(workers.max(1));
}

/// Create (truncating) the file at `path`, returning an owning handle or
/// null on error. Free with [`dtact_util_fs_file_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_file_create(path: *const c_char) -> *mut DtactFile {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return std::ptr::null_mut();
    };
    match block_on(DtactFile::create(path)) {
        Ok(f) => Box::into_raw(Box::new(f)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open the existing file at `path` for reading, returning an owning handle
/// or null on error. Free with [`dtact_util_fs_file_close`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_file_open(path: *const c_char) -> *mut DtactFile {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return std::ptr::null_mut();
    };
    match block_on(DtactFile::open(path)) {
        Ok(f) => Box::into_raw(Box::new(f)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Read up to `len` bytes from `file` (advancing its cursor) into `buf`.
/// Returns the byte count read (0 = EOF) or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_file_read(
    file: *mut DtactFile,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if file.is_null() || buf.is_null() {
        set_last_error("null file handle or buffer");
        return -1;
    }
    let file = unsafe { &*file };
    let scratch = vec![0u8; len];
    match block_on(file.read(scratch)) {
        Ok((n, data)) => {
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf, n) };
            n as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Write `len` bytes from `buf` to `file` (advancing its cursor). Returns
/// the byte count written or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_file_write(
    file: *mut DtactFile,
    buf: *const u8,
    len: usize,
) -> isize {
    clear_last_error();
    if file.is_null() || buf.is_null() {
        set_last_error("null file handle or buffer");
        return -1;
    }
    let file = unsafe { &*file };
    let data = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
    match block_on(file.write(data)) {
        Ok((n, _)) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Flush `file`'s buffers to disk (`FlushFileBuffers`/`fsync`). Returns 0 on
/// success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_file_sync(file: *mut DtactFile) -> i32 {
    clear_last_error();
    if file.is_null() {
        set_last_error("null file handle");
        return -1;
    }
    let file = unsafe { &*file };
    match block_on(file.sync_all()) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Close and free a file handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `file` must have
/// come from a `dtact_util_fs_file_*` constructor and must not be used
/// afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_file_close(file: *mut DtactFile) {
    if !file.is_null() {
        drop(unsafe { Box::from_raw(file) });
    }
}
