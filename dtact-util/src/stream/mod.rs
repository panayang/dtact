//! In-process duplex byte-stream pipes.
//!
//! Two backends, selected the same way as [`crate::io`] and [`crate::fs`]:
//! - `native` (default): a lock-free duplex pipe built directly on
//!   [`crate::lockfree::SpscQueue`] (one ring buffer per direction, single
//!   producer/single consumer per ring — exactly what a two-endpoint pipe
//!   needs) plus [`crate::lockfree::AtomicWakerSlot`] for backpressure/data-
//!   availability notification. No `Mutex`, no per-call heap allocation.
//! - `tokio` (when `native` is off): a thin wrapper over
//!   `tokio::io::duplex`.

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use native::*;

// NOTE: named `tokio_backend`, not `tokio` — see `io::mod`/`fs::mod` for
// why a local module literally named `tokio` shadows the extern crate.
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;
