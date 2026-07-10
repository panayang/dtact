//! Latency-breakdown tracing (`DTACT_IO_TRACE=1`), shared between the
//! Unix and Windows native reactors — previously an identical copy
//! pasted into both `native.rs` and `windows.rs` (the latter via
//! `include!("windows_primitives.rs")`).
//!
//! `perf` SIGSEGVs on this runtime's stackful coroutines and `strace -T`'s
//! own ptrace overhead dominates the very microsecond-scale timings we're
//! trying to measure (every syscall reads as ~1-2ms regardless of
//! backend). This gives three in-process, monotonic-clock checkpoints per
//! op so the total latency can be split into "submit -> kernel completion
//! observed by io-worker" (`io_uring/IOCP/kernel` round trip) vs "kernel
//! completion -> fiber re-polls and observes it" (wake propagation /
//! scheduler rescheduling), without any external tracer in the loop.

use std::sync::OnceLock;

#[inline]
pub fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("DTACT_IO_TRACE").is_some())
}

#[inline]
pub fn trace_now_us() -> u128 {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_micros()
}

macro_rules! io_trace {
    ($($arg:tt)*) => {
        if $crate::io::trace::trace_enabled() {
            eprintln!($($arg)*);
        }
    };
}
pub(crate) use io_trace;
