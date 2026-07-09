//! I/O primitives: TCP listener/stream, UDP socket, native and `tokio`-backed.
//!
//! The native backend is lock-free (`io_uring` on Linux, IOCP on Windows,
//! kqueue via mio on macOS/BSD) plus a thin tokio-backed alternative.
//! Split out of what used to be a single monolithic `lib.rs` — module
//! boundaries mirror the original `native_impl` / `windows_impl` /
//! `tokio_impl` blocks.

use std::future::Future;
use std::pin::Pin;
#[cfg(all(feature = "tokio", not(feature = "native")))]
use std::task::{Context, Poll};

#[cfg(all(feature = "native", any(unix, windows)))]
pub(crate) mod trace;

#[cfg(all(feature = "native", unix))]
mod native;
#[cfg(all(feature = "native", unix))]
pub use native::*;

#[cfg(all(feature = "native", windows))]
mod windows;
#[cfg(all(feature = "native", windows))]
pub use windows::*;

// NOTE: named `tokio_backend`, not `tokio` - a local module named `tokio`
// shadows the extern crate `tokio` for path resolution within this file,
// which breaks every `tokio::...` path below. This bit us once already
// (see dtact-util/src/fs/mod.rs's identical note).
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;
