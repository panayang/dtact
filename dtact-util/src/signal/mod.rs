//! Async signal delivery.
//!
//! Two backends, selected the same way as [`crate::io`]/[`crate::fs`]/
//! [`crate::stream`]:
//! - `native` (default): Unix uses a self-pipe fed by a minimal async-
//!   signal-safe `sigaction` handler, drained by a dedicated reader
//!   thread that broadcasts to every registered listener (see
//!   [`registry::ListenerRegistry`]) — no `Mutex` anywhere in the
//!   delivery path. Windows uses `SetConsoleCtrlHandler` for Ctrl+C /
//!   Ctrl+Break (there's no POSIX-signal equivalent on Windows, so this
//!   backend's surface is intentionally narrower there — see
//!   `signal::windows`'s module doc).
//! - `tokio` (when `native` is off): thin wrappers over
//!   `tokio::signal::unix`/`tokio::signal::windows`.
//!
//! No `DtactCompat` layer here — signals aren't a byte stream, there's
//! nothing for `AsyncRead`/`AsyncWrite` to bridge.

#[cfg(feature = "native")]
mod registry;

#[cfg(all(feature = "native", unix))]
mod unix;
#[cfg(all(feature = "native", unix))]
pub use unix::*;

#[cfg(all(feature = "native", windows))]
mod windows;
#[cfg(all(feature = "native", windows))]
pub use windows::*;

// NOTE: named `tokio_backend`, not `tokio` — see `io::mod`'s doc for why a
// local module literally named `tokio` shadows the extern crate.
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;
