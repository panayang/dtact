//! Async filesystem primitives.
//!
//! Two backends, selected the same way as [`crate::io`]:
//! - `native` (default): a small dedicated blocking-thread pool that bridges
//!   `std::fs`/platform positional-I/O syscalls into futures. On Linux this
//!   is a reasonable place to grow real io_uring opcodes (Openat/Read/Write/
//!   Fsync/Close/Statx) later — see the module doc on `native` for exactly
//!   what's deferred and why.
//! - `tokio` (when `native` is off): a thin wrapper over `tokio::fs`.
//!
//! Unlike [`crate::io`] (which re-exports flat at the crate root to preserve
//! the pre-split `dtact-io` public API), `fs` items live under
//! `dtact_util::fs::*` to keep the growing set of new modules
//! (`fs`/`process`/`signal`/`stream`/`timer`) from colliding on names like
//! `init`.

#[cfg(all(feature = "native", windows))]
mod iocp_windows;
#[cfg(all(feature = "native", windows))]
pub use iocp_windows::*;

#[cfg(all(feature = "native", target_os = "linux"))]
mod uring_linux;
#[cfg(all(feature = "native", target_os = "linux"))]
pub use uring_linux::*;

#[cfg(all(feature = "native", not(windows), not(target_os = "linux")))]
mod native;
#[cfg(all(feature = "native", not(windows), not(target_os = "linux")))]
pub use native::*;

// NOTE: named `tokio_backend`, not `tokio` - see io/mod.rs for why a local
// module literally named `tokio` shadows the extern crate of the same name.
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;
