#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::undocumented_unsafe_blocks)]

#[cfg(feature = "native")]
pub use dtact_macros::dtact_io_init as init;
#[cfg(feature = "native")]
pub use dtact_macros::fs_init;
#[cfg(feature = "native")]
pub use dtact_macros::process_init;

// The Unix backend (io_uring on Linux, kqueue/mio elsewhere) is fd-based and
// cannot compile on Windows; the Windows backend (`io::windows`, below) is
// IOCP-based and cannot compile on Unix. Anything that's neither (e.g. wasm)
// gets a clear build-time error instead of failing deep inside fd/handle code.
#[cfg(all(feature = "native", not(any(unix, windows))))]
compile_error!(
    "dtact-util's `native` feature supports Unix (io_uring/kqueue) and \
     Windows (IOCP) only. On other platforms, use the default `tokio` feature instead."
);

#[cfg(feature = "native")]
pub mod lockfree;

pub mod io;

pub mod fs;

pub mod timer;

pub mod stream;

pub mod signal;

pub mod process;
