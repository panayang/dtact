//! Async timer primitives: sleep, interval, timeout.
//!
//! Two backends, selected the same way as [`crate::fs`]:
//! - `native` (default): a single dedicated background thread that maintains
//!   a sorted list of pending deadlines and parks until the next one is due
//!   (see the module doc on `native` for why this — rather than a hashed
//!   timer wheel — was chosen for this pass).
//! - `tokio` (when `native` is off): a thin wrapper over `tokio::time`.

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use native::*;

// NOTE: named `tokio_backend`, not `tokio` - see fs/mod.rs / io/mod.rs for
// why a local module literally named `tokio` shadows the extern crate.
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;
