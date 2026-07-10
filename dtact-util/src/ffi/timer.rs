//! C FFI for [`crate::timer`]: blocking sleep and a blocking interval.
//!
//! `timeout` from the Rust API is intentionally **not** exposed here: it
//! wraps an arbitrary `Future`, which has no representation in a synchronous
//! C ABI (there is no C "future" to time out). Callers who need a deadline
//! on a C-side blocking operation should use their own platform timeout on
//! the blocking call, or poll an interval.

use crate::ffi::{block_on, clear_last_error};
use crate::timer::{DtactInterval, sleep};
use std::time::Duration;

/// Block the calling thread for `millis` milliseconds using the native
/// timer wheel.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_timer_sleep_ms(millis: u64) {
    clear_last_error();
    block_on(sleep(Duration::from_millis(millis)));
}

/// Create a repeating interval timer with the given period in milliseconds.
///
/// Returns an owning handle, or null if `period_millis` is 0 (an error is
/// recorded). Free with [`dtact_util_timer_interval_free`].
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. Takes no pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_timer_interval_create(
    period_millis: u64,
) -> *mut DtactInterval {
    clear_last_error();
    if period_millis == 0 {
        crate::ffi::set_last_error("interval period must be > 0");
        return std::ptr::null_mut();
    }
    Box::into_raw(Box::new(DtactInterval::new(Duration::from_millis(
        period_millis,
    ))))
}

/// Block until this interval's next tick fires.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `interval` must be a
/// live handle from [`dtact_util_timer_interval_create`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_timer_interval_tick(interval: *mut DtactInterval) {
    clear_last_error();
    if interval.is_null() {
        crate::ffi::set_last_error("null interval handle");
        return;
    }
    let interval = unsafe { &mut *interval };
    block_on(interval.tick());
}

/// Free an interval handle. Passing null is a no-op.
///
/// # Safety
///
/// See the [`crate::ffi`] module-level Safety contract. `interval` must have
/// come from [`dtact_util_timer_interval_create`] and must not be used
/// afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtact_util_timer_interval_free(interval: *mut DtactInterval) {
    if !interval.is_null() {
        drop(unsafe { Box::from_raw(interval) });
    }
}
