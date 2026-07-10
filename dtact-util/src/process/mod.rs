//! Async child-process primitives.
//!
//! Two backends, selected the same way as [`crate::io`]/[`crate::fs`]:
//! - `native` (default): synchronous spawn (matches `tokio::process`'s
//!   own choice), `wait`/stdio I/O dispatched to a dedicated blocking-
//!   thread pool, completion signaled via
//!   [`crate::lockfree::OnceSlot`] — no `Mutex`-guarded completion state.
//!   Child handles are never shared: `wait`/`wait_with_output` consume
//!   `self` outright, and stdio handles are exclusively owned by whoever
//!   holds them. See `native`'s module doc for the full rationale.
//! - `tokio` (when `native` is off): a thin wrapper over
//!   `tokio::process`.

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use native::*;

// NOTE: named `tokio_backend`, not `tokio` — see `io::mod`'s doc for why a
// local module literally named `tokio` shadows the extern crate.
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;
