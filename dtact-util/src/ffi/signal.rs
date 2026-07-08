//! C FFI for [`crate::signal`].
//!
//! The exposed surface is deliberately per-platform, matching what the
//! native backend actually compiles (see `signal/mod.rs`'s `cfg` gates):
//!
//! - **Windows**: [`dtact_util_signal_ctrl_c`] / [`dtact_util_signal_ctrl_break`]
//!   (there is no general POSIX-signal delivery on Windows).
//! - **Unix**: [`dtact_util_signal_register`] for an arbitrary signal number
//!   (`SIGINT`, `SIGTERM`, `SIGUSR1`, ...).
//!
//! Both platforms share [`dtact_util_signal_recv`] (blocking wait for the
//! next delivery) and [`dtact_util_signal_free`].

use crate::ffi::{block_on, clear_last_error, set_last_error};
use crate::signal::DtactSignalStream;

/// Register a listener for a raw signal number (`libc::SIGINT` etc.),
/// returning an owning handle. Free with [`dtact_util_signal_free`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_signal_register(
    signum: std::ffi::c_int,
) -> *mut DtactSignalStream {
    clear_last_error();
    Box::into_raw(Box::new(DtactSignalStream::new(signum)))
}

/// Register a listener for Ctrl+C (`CTRL_C_EVENT`), returning an owning
/// handle. Free with [`dtact_util_signal_free`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[cfg(windows)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_signal_ctrl_c() -> *mut DtactSignalStream {
    clear_last_error();
    Box::into_raw(Box::new(crate::signal::ctrl_c()))
}

/// Register a listener for Ctrl+Break (`CTRL_BREAK_EVENT`), returning an
/// owning handle. Free with [`dtact_util_signal_free`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[cfg(windows)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_signal_ctrl_break() -> *mut DtactSignalStream {
    clear_last_error();
    Box::into_raw(Box::new(crate::signal::ctrl_break()))
}

/// Block until this signal is next delivered.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `stream` must be a
/// live handle from one of the signal constructors above.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_signal_recv(stream: *mut DtactSignalStream) {
    clear_last_error();
    if stream.is_null() {
        set_last_error("null signal handle");
        return;
    }
    let stream = unsafe { &*stream };
    block_on(stream.recv());
}

/// Free a signal listener handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_signal_free(stream: *mut DtactSignalStream) {
    if !stream.is_null() {
        drop(unsafe { Box::from_raw(stream) });
    }
}
