//! C FFI for [`crate::process`]: spawn a child, configure/read/write its
//! stdio pipes, wait for exit, kill.
//!
//! Stdio configuration is a bitmask passed to [`dtact_util_process_spawn`]:
//! set [`DTACT_STDIN_PIPE`] / [`DTACT_STDOUT_PIPE`] / [`DTACT_STDERR_PIPE`]
//! to route that stream through a pipe you can then read/write from C;
//! unset bits inherit the parent's corresponding handle.

use crate::ffi::{block_on, clear_last_error, cstr_to_str, set_io_error, set_last_error};
use crate::process::{
    DtactChild, DtactChildStderr, DtactChildStdin, DtactChildStdout, DtactCommand,
};
use std::ffi::c_char;
use std::process::Stdio;

/// Stdio bitmask flag: route the child's stdin through a pipe.
pub const DTACT_STDIN_PIPE: u32 = 1;
/// Stdio bitmask flag: route the child's stdout through a pipe.
pub const DTACT_STDOUT_PIPE: u32 = 2;
/// Stdio bitmask flag: route the child's stderr through a pipe.
pub const DTACT_STDERR_PIPE: u32 = 4;

/// Initialize the process backend's blocking-op pool with `workers`
/// threads. Idempotent (the pool also self-initializes on first use).
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_init(workers: usize) {
    clear_last_error();
    crate::process::init(workers.max(1));
}

/// Spawn `program` as a child process.
///
/// Takes `arg_count` arguments (`argv[0..arg_count]`, none of which is the
/// program name) and the given `stdio_flags` (see [`DTACT_STDIN_PIPE`]
/// etc.). Returns an owning child handle or null on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `program` must be a
/// valid C string; `argv` must be non-null when `arg_count > 0` and point to
/// `arg_count` valid C-string pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_spawn(
    program: *const c_char,
    argv: *const *const c_char,
    arg_count: usize,
    stdio_flags: u32,
) -> *mut DtactChild {
    clear_last_error();
    let Some(program) = (unsafe { cstr_to_str(program) }) else {
        return std::ptr::null_mut();
    };
    let mut cmd = DtactCommand::new(program);
    if arg_count > 0 {
        if argv.is_null() {
            set_last_error("null argv with arg_count > 0");
            return std::ptr::null_mut();
        }
        let arg_ptrs = unsafe { std::slice::from_raw_parts(argv, arg_count) };
        for &arg in arg_ptrs {
            let Some(arg) = (unsafe { cstr_to_str(arg) }) else {
                return std::ptr::null_mut();
            };
            cmd.arg(arg);
        }
    }
    if stdio_flags & DTACT_STDIN_PIPE != 0 {
        cmd.stdin(Stdio::piped());
    }
    if stdio_flags & DTACT_STDOUT_PIPE != 0 {
        cmd.stdout(Stdio::piped());
    }
    if stdio_flags & DTACT_STDERR_PIPE != 0 {
        cmd.stderr(Stdio::piped());
    }
    match cmd.spawn() {
        Ok(child) => Box::into_raw(Box::new(child)),
        Err(e) => {
            set_io_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Return the child's OS process id, or 0 if `child` is null.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_id(child: *mut DtactChild) -> u32 {
    clear_last_error();
    if child.is_null() {
        set_last_error("null child handle");
        return 0;
    }
    unsafe { &*child }.id()
}

/// Kill the child (does not reap it — still call
/// [`dtact_util_process_child_wait`] or [`dtact_util_process_child_free`]).
/// Returns 0 on success, -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_kill(child: *mut DtactChild) -> i32 {
    clear_last_error();
    if child.is_null() {
        set_last_error("null child handle");
        return -1;
    }
    match unsafe { &mut *child }.kill() {
        Ok(()) => 0,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Block until the child exits, writing its exit code into `*out_code`.
///
/// **Consumes and frees `child`** — do not use the handle afterwards.
/// Returns 0 on success, -1 on error (e.g. the platform reported no exit
/// code, as for a signal-terminated process on Unix).
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `child` must be a
/// live handle; `out_code`, if non-null, must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_wait(
    child: *mut DtactChild,
    out_code: *mut i32,
) -> i32 {
    clear_last_error();
    if child.is_null() {
        set_last_error("null child handle");
        return -1;
    }
    let child = unsafe { Box::from_raw(child) };
    match block_on(child.wait()) {
        Ok(status) => status.code().map_or_else(
            || {
                set_last_error("process terminated without an exit code");
                -1
            },
            |code| {
                if !out_code.is_null() {
                    unsafe { *out_code = code };
                }
                0
            },
        ),
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Free a child handle without waiting (leaves a zombie on Unix if the
/// child hasn't been reaped). Prefer [`dtact_util_process_child_wait`].
/// Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_free(child: *mut DtactChild) {
    if !child.is_null() {
        drop(unsafe { Box::from_raw(child) });
    }
}

/// Take ownership of the child's stdin pipe (only if spawned with
/// [`DTACT_STDIN_PIPE`]). Returns null if unavailable or already taken.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_take_stdin(
    child: *mut DtactChild,
) -> *mut DtactChildStdin {
    clear_last_error();
    if child.is_null() {
        set_last_error("null child handle");
        return std::ptr::null_mut();
    }
    unsafe { &mut *child }.take_stdin().map_or_else(
        || {
            set_last_error("stdin pipe unavailable or already taken");
            std::ptr::null_mut()
        },
        |h| Box::into_raw(Box::new(h)),
    )
}

/// Take ownership of the child's stdout pipe (only if spawned with
/// [`DTACT_STDOUT_PIPE`]). Returns null if unavailable or already taken.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_take_stdout(
    child: *mut DtactChild,
) -> *mut DtactChildStdout {
    clear_last_error();
    if child.is_null() {
        set_last_error("null child handle");
        return std::ptr::null_mut();
    }
    unsafe { &mut *child }.take_stdout().map_or_else(
        || {
            set_last_error("stdout pipe unavailable or already taken");
            std::ptr::null_mut()
        },
        |h| Box::into_raw(Box::new(h)),
    )
}

/// Take ownership of the child's stderr pipe (only if spawned with
/// [`DTACT_STDERR_PIPE`]). Returns null if unavailable or already taken.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_child_take_stderr(
    child: *mut DtactChild,
) -> *mut DtactChildStderr {
    clear_last_error();
    if child.is_null() {
        set_last_error("null child handle");
        return std::ptr::null_mut();
    }
    unsafe { &mut *child }.take_stderr().map_or_else(
        || {
            set_last_error("stderr pipe unavailable or already taken");
            std::ptr::null_mut()
        },
        |h| Box::into_raw(Box::new(h)),
    )
}

/// Write `len` bytes from `buf` to the child's stdin. Returns the byte count
/// written or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_stdin_write(
    stdin: *mut DtactChildStdin,
    buf: *const u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stdin.is_null() || buf.is_null() {
        set_last_error("null stdin handle or buffer");
        return -1;
    }
    let stdin = unsafe { &mut *stdin };
    let data = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
    match block_on(stdin.write(data)) {
        Ok((n, _)) => n as isize,
        Err(e) => {
            set_io_error(&e);
            -1
        }
    }
}

/// Close (free) the child's stdin pipe, letting the child observe EOF.
/// Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_stdin_close(stdin: *mut DtactChildStdin) {
    if !stdin.is_null() {
        unsafe { Box::from_raw(stdin) }.close();
    }
}

/// Read up to `len` bytes from the child's stdout into `buf`. Returns the
/// byte count read (0 = EOF) or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_stdout_read(
    stdout: *mut DtactChildStdout,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stdout.is_null() || buf.is_null() {
        set_last_error("null stdout handle or buffer");
        return -1;
    }
    let stdout = unsafe { &mut *stdout };
    let scratch = vec![0u8; len];
    match block_on(stdout.read(scratch)) {
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

/// Free the child's stdout pipe handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_stdout_free(stdout: *mut DtactChildStdout) {
    if !stdout.is_null() {
        drop(unsafe { Box::from_raw(stdout) });
    }
}

/// Read up to `len` bytes from the child's stderr into `buf`. Returns the
/// byte count read (0 = EOF) or -1 on error.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_stderr_read(
    stderr: *mut DtactChildStderr,
    buf: *mut u8,
    len: usize,
) -> isize {
    clear_last_error();
    if stderr.is_null() || buf.is_null() {
        set_last_error("null stderr handle or buffer");
        return -1;
    }
    let stderr = unsafe { &mut *stderr };
    let scratch = vec![0u8; len];
    match block_on(stderr.read(scratch)) {
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

/// Free the child's stderr pipe handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_process_stderr_free(stderr: *mut DtactChildStderr) {
    if !stderr.is_null() {
        drop(unsafe { Box::from_raw(stderr) });
    }
}
