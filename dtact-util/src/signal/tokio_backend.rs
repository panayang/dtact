//! `tokio::signal`-backed signal primitives, for callers who'd rather
//! share tokio's own signal-driver reactor than dtact-signal's self-
//! pipe/console-handler backend.

#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

#[cfg(unix)]
mod unix {
    use std::io;
    use tokio::signal::unix::{Signal, SignalKind, signal};

    /// A stream of occurrences of one Unix signal, backed by tokio's
    /// signal-fd/self-pipe reactor integration instead of dtact-signal's
    /// own registry. Unlike the native backend, each instance owns its own
    /// OS-level registration rather than sharing a broadcast registry.
    pub struct DtactSignalStream(Signal);

    impl DtactSignalStream {
        /// Wait for the next delivery of this signal. Multiple deliveries
        /// that arrive before this is polled are coalesced into a single
        /// notification (standard Unix signal-coalescing behavior).
        pub async fn recv(&mut self) {
            self.0.recv().await;
        }
    }

    fn make(kind: SignalKind) -> io::Result<DtactSignalStream> {
        signal(kind).map(DtactSignalStream)
    }

    /// Register a listener for `SIGINT`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler (e.g. signal
    /// masking not available, or resource exhaustion registering the
    /// underlying self-pipe).
    pub fn sigint() -> DtactSignalStream {
        make(SignalKind::interrupt()).expect("dtact-signal: failed to register SIGINT handler")
    }
    /// Register a listener for `SIGTERM`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    pub fn sigterm() -> DtactSignalStream {
        make(SignalKind::terminate()).expect("dtact-signal: failed to register SIGTERM handler")
    }
    /// Register a listener for `SIGHUP`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    pub fn sighup() -> DtactSignalStream {
        make(SignalKind::hangup()).expect("dtact-signal: failed to register SIGHUP handler")
    }
    /// Register a listener for `SIGUSR1`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    pub fn sigusr1() -> DtactSignalStream {
        make(SignalKind::user_defined1()).expect("dtact-signal: failed to register SIGUSR1 handler")
    }
    /// Register a listener for `SIGUSR2`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    pub fn sigusr2() -> DtactSignalStream {
        make(SignalKind::user_defined2()).expect("dtact-signal: failed to register SIGUSR2 handler")
    }
    /// Register a listener for `SIGCHLD`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    pub fn sigchld() -> DtactSignalStream {
        make(SignalKind::child()).expect("dtact-signal: failed to register SIGCHLD handler")
    }
}

#[cfg(windows)]
mod windows {
    use tokio::signal::windows::{CtrlBreak, CtrlC};

    /// A stream of occurrences of one Windows console-control event
    /// (`Ctrl+C` or `Ctrl+Break`), backed by tokio's console-handler
    /// reactor integration instead of dtact-signal's own registry.
    pub struct DtactSignalStream(CtrlWrapper);

    enum CtrlWrapper {
        C(CtrlC),
        Break(CtrlBreak),
    }

    impl DtactSignalStream {
        /// Wait for the next delivery of this console-control event.
        pub async fn recv(&mut self) {
            match &mut self.0 {
                CtrlWrapper::C(s) => {
                    s.recv().await;
                }
                CtrlWrapper::Break(s) => {
                    s.recv().await;
                }
            }
        }
    }

    /// Register a listener for the `Ctrl+C` console-control event.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the console-control handler
    /// (e.g. the process has no console attached, or a handler is already
    /// registered in a way tokio cannot share).
    #[must_use]
    pub fn ctrl_c() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::C(
            tokio::signal::windows::ctrl_c()
                .expect("dtact-signal: failed to register Ctrl+C handler"),
        ))
    }

    /// Register a listener for the `Ctrl+Break` console-control event.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the console-control handler.
    #[must_use]
    pub fn ctrl_break() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::Break(
            tokio::signal::windows::ctrl_break()
                .expect("dtact-signal: failed to register Ctrl+Break handler"),
        ))
    }
}
