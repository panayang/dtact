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

#[cfg(all(feature = "native", windows))]
mod named_pipe_windows;
#[cfg(all(feature = "native", windows))]
pub use named_pipe_windows::{DtactNamedPipeClient, DtactNamedPipeHandle, DtactNamedPipeServer};

// NOTE: named `tokio_backend`, not `tokio` - a local module named `tokio`
// shadows the extern crate `tokio` for path resolution within this file,
// which breaks every `tokio::...` path below. This bit us once already
// (see dtact-util/src/fs/mod.rs's identical note).
#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_backend;
#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_backend::*;

/// Resolve `host` (a `"host:port"` string, exactly what
/// `std::net::ToSocketAddrs` accepts) to one or more `SocketAddr`s,
/// without blocking the calling task's thread on the DNS round-trip.
///
/// Unlike every other `native`-backend primitive in this crate, this
/// doesn't need `io_uring`/IOCP/mio at all — `getaddrinfo(3)` (what
/// `std::net::ToSocketAddrs` calls under the hood) has no async variant
/// on any of our target platforms, so the only way to make it not block
/// the caller is to run it on a throwaway thread and hand the result back
/// through a [`crate::sync::oneshot`] channel. One thread per call rather
/// than a persistent pool: DNS lookups are neither hot-path nor
/// typically concurrent enough to justify the extra bookkeeping a pool
/// (like `fs`'s or `process`'s) needs, and callers who *do* look up
/// hosts at high volume should be caching the results anyway, not
/// re-resolving on every connection.
///
/// # Errors
/// Returns whatever `std::net::ToSocketAddrs::to_socket_addrs` returns
/// for `host` (e.g. `host:port` doesn't parse, or the name doesn't
/// resolve), or an I/O error if the resolver thread itself panicked.
///
/// # Panics
/// Panics if the OS refuses to spawn the resolver thread (fatal resource
/// exhaustion) — the same class of failure every other native backend in
/// this crate treats as unrecoverable at thread-spawn time.
#[cfg(feature = "native")]
pub async fn lookup_host(
    host: impl Into<String>,
) -> std::io::Result<impl Iterator<Item = std::net::SocketAddr>> {
    let host = host.into();
    let (tx, rx) = crate::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("dtact-io-lookup-host".into())
        .spawn(move || {
            use std::net::ToSocketAddrs;
            let result = host.to_socket_addrs().map(Iterator::collect::<Vec<_>>);
            let _ = tx.send(result);
        })
        .expect("failed to spawn dtact-io lookup_host thread");
    rx.await
        .unwrap_or_else(|_| {
            Err(std::io::Error::other(
                "dtact-io: lookup_host resolver thread panicked before sending a result",
            ))
        })
        .map(Vec::into_iter)
}

// =========================================================================
// SHARED STREAM TRAITS + COMBINATORS (`BufReader`/`BufWriter`/`copy`)
// =========================================================================
//
// `tokio::io` has a much larger combinator zoo (`Chain`/`Take`/`Lines`/
// `split`/`empty`/`repeat`/`sink`/`stdin`/`stdout`/`stderr`/...) than what
// follows here — this covers the handful actually worth having given
// this crate's own shape, not a 1:1 port:
//
// - No `split()`/`OwnedReadHalf`/`OwnedWriteHalf`. Every stream type this
//   trait is implemented for already takes `&self` (not `&mut self`) for
//   both `read` and `write` — the reason `tokio::net::TcpStream` needs a
//   split at all is that `std`'s I/O traits require `&mut self`, forcing
//   one owner. Here, just wrap the stream in an `Arc` and call
//   `.read()`/`.write()` from as many tasks as you like; there's nothing
//   a split would add.
// - No `Chain`/`Take`/`Lines`/`empty`/`repeat`/`sink`/`stdin`/`stdout`/
//   `stderr`: genuinely useful but secondary — add them if/when a real
//   use case shows up rather than speculatively.
#[cfg(any(feature = "native", feature = "tokio"))]
mod combinators;
#[cfg(any(feature = "native", feature = "tokio"))]
pub use combinators::{AsyncRead, AsyncWrite, BufReader, BufWriter, copy};
