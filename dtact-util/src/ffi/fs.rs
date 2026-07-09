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

/// Copy the contents (and permission bits) of the file at `from` to `to`,
/// returning the byte count copied, or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `from`/`to` must
/// be valid NUL-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_copy(from: *const c_char, to: *const c_char) -> i64 {
    clear_last_error();
    crate::fs::init(1);
    let Some(from) = (unsafe { cstr_to_str(from) }) else {
        return -1;
    };
    let Some(to) = (unsafe { cstr_to_str(to) }) else {
        return -1;
    };
    match block_on(crate::fs::copy(from, to)) {
        Ok(n) => n as i64,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Rename (move) the file or directory at `from` to `to`, replacing `to`
/// if it already exists. Returns 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `from`/`to` must
/// be valid NUL-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_rename(from: *const c_char, to: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(from) = (unsafe { cstr_to_str(from) }) else {
        return -1;
    };
    let Some(to) = (unsafe { cstr_to_str(to) }) else {
        return -1;
    };
    match block_on(crate::fs::rename(from, to)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Create a hard link at `dst` pointing at the same file as `src`.
/// Returns 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `src`/`dst` must
/// be valid NUL-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_hard_link(src: *const c_char, dst: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(src) = (unsafe { cstr_to_str(src) }) else {
        return -1;
    };
    let Some(dst) = (unsafe { cstr_to_str(dst) }) else {
        return -1;
    };
    match block_on(crate::fs::hard_link(src, dst)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Create a symbolic link at `dst` pointing at `src`. Unix only —
/// Windows callers need [`dtact_util_fs_symlink_dir`]/
/// [`dtact_util_fs_symlink_file`] instead. Returns 0 on success, -1 on
/// error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `src`/`dst` must
/// be valid NUL-terminated C strings.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_symlink(src: *const c_char, dst: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(src) = (unsafe { cstr_to_str(src) }) else {
        return -1;
    };
    let Some(dst) = (unsafe { cstr_to_str(dst) }) else {
        return -1;
    };
    match block_on(crate::fs::symlink(src, dst)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Create a directory symbolic link at `dst` pointing at `src`. Windows
/// only — see [`dtact_util_fs_symlink`] for the Unix equivalent. Returns
/// 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `src`/`dst` must
/// be valid NUL-terminated C strings.
#[cfg(windows)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_symlink_dir(src: *const c_char, dst: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(src) = (unsafe { cstr_to_str(src) }) else {
        return -1;
    };
    let Some(dst) = (unsafe { cstr_to_str(dst) }) else {
        return -1;
    };
    match block_on(crate::fs::symlink_dir(src, dst)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Create a file symbolic link at `dst` pointing at `src`. Windows only —
/// see [`dtact_util_fs_symlink`] for the Unix equivalent. Returns 0 on
/// success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `src`/`dst` must
/// be valid NUL-terminated C strings.
#[cfg(windows)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_symlink_file(src: *const c_char, dst: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(src) = (unsafe { cstr_to_str(src) }) else {
        return -1;
    };
    let Some(dst) = (unsafe { cstr_to_str(dst) }) else {
        return -1;
    };
    match block_on(crate::fs::symlink_file(src, dst)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Read the target of the symbolic link at `path` into `out` (capacity
/// `out_cap`), NUL-terminated and truncated if it doesn't fit.
///
/// Returns the untruncated target's byte length (which may exceed
/// `out_cap`) on success, or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string; `out`, if non-null, must point to at
/// least `out_cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_read_link(
    path: *const c_char,
    out: *mut c_char,
    out_cap: usize,
) -> isize {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::read_link(path)) {
        Ok(target) => {
            let s = target.to_string_lossy();
            let bytes = s.as_bytes();
            write_str_out(bytes, out, out_cap);
            bytes.len() as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Resolve `path` to an absolute path with all intermediate components
/// resolved, writing it (NUL-terminated, truncated if it doesn't fit)
/// into `out` (capacity `out_cap`).
///
/// Returns the untruncated resolved path's byte length on success, or -1
/// on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string; `out`, if non-null, must point to at
/// least `out_cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_canonicalize(
    path: *const c_char,
    out: *mut c_char,
    out_cap: usize,
) -> isize {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::canonicalize(path)) {
        Ok(resolved) => {
            let s = resolved.to_string_lossy();
            let bytes = s.as_bytes();
            write_str_out(bytes, out, out_cap);
            bytes.len() as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Read the entire contents of the file at `path` into `buf` (capacity
/// `len`).
///
/// Returns the file's total byte length (which may exceed `len`, in
/// which case `buf` received only the first `len` bytes) on success, or
/// -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string; `buf`, if non-null, must point to at
/// least `len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_read(
    path: *const c_char,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::read(path)) {
        Ok(data) => {
            if !buf.is_null() && len > 0 {
                let n = data.len().min(len);
                unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf, n) };
            }
            data.len() as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Read the entire contents of the file at `path` as UTF-8 text into
/// `out` (capacity `out_cap`), NUL-terminated and truncated if it doesn't
/// fit.
///
/// Returns the untruncated content's byte length on success, or -1 on
/// error (including `InvalidData` if the file isn't valid UTF-8).
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string; `out`, if non-null, must point to at
/// least `out_cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_read_to_string(
    path: *const c_char,
    out: *mut c_char,
    out_cap: usize,
) -> isize {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::read_to_string(path)) {
        Ok(s) => {
            let bytes = s.as_bytes();
            write_str_out(bytes, out, out_cap);
            bytes.len() as isize
        }
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Write `len` bytes from `buf` to the file at `path`, creating it if it
/// doesn't exist and truncating it if it does. Returns 0 on success, -1
/// on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string; `buf` must point to at least `len`
/// readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_write(
    path: *const c_char,
    buf: *const u8,
    len: usize,
) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    let data = if buf.is_null() {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(buf, len) }
    };
    match block_on(crate::fs::write(path, data)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Create a single new directory. Unlike
/// [`dtact_util_fs_create_dir_all`], fails if any parent component
/// doesn't already exist. Returns 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_create_dir(path: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::create_dir(path)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Recursively create `path` and any missing parent directories. Returns
/// 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_create_dir_all(path: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::create_dir_all(path)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Remove an empty directory. Fails if `path` is non-empty — see
/// [`dtact_util_fs_remove_dir_all`] for the recursive version. Returns 0
/// on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_remove_dir(path: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::remove_dir(path)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Recursively remove a directory and everything under it. Returns 0 on
/// success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_remove_dir_all(path: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::remove_dir_all(path)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Check whether `path` exists, following symlinks. Returns `1` if it
/// exists, `0` if it doesn't, or `-1` on any other error (e.g.
/// `PermissionDenied` on a parent directory).
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_try_exists(path: *const c_char) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    match block_on(crate::fs::try_exists(path)) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Set `path` read-only (`readonly != 0`) or read-write (`readonly ==
/// 0`).
///
/// A thin, portable slice of `std::fs::Permissions` — the only bit
/// consistently meaningful to set/query across Unix and Windows. Returns
/// 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `path` must be a
/// valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_fs_set_readonly(path: *const c_char, readonly: i32) -> i32 {
    clear_last_error();
    crate::fs::init(1);
    let Some(path) = (unsafe { cstr_to_str(path) }) else {
        return -1;
    };
    let meta = match block_on(crate::fs::metadata(path)) {
        Ok(m) => m,
        Err(e) => {
            set_io_error(&e);
            return -1;
        }
    };
    let mut perm = meta.permissions();
    perm.set_readonly(readonly != 0);
    match block_on(crate::fs::set_permissions(path, perm)) {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Write `bytes` as a NUL-terminated string into `out` (capacity `cap`),
/// truncating (but always NUL-terminating, if `cap > 0`) rather than
/// overflowing. A null or zero-capacity `out` is a silent no-op.
fn write_str_out(bytes: &[u8], out: *mut c_char, cap: usize) {
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
