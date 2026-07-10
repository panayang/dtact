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
    pub use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::{Signal, signal};

    /// A stream of occurrences of one Unix signal.
    ///
    /// Backed by tokio's signal-fd/self-pipe reactor integration instead
    /// of dtact-signal's own registry. Unlike the native backend, each
    /// instance owns its own OS-level registration rather than sharing a
    /// broadcast registry.
    pub struct DtactSignalStream(Signal);

    impl DtactSignalStream {
        /// Wait for the next delivery of this signal. Multiple deliveries
        /// that arrive before this is polled are coalesced into a single
        /// notification (standard Unix signal-coalescing behavior).
        pub async fn recv(&mut self) {
            self.0.recv().await;
        }
    }

    /// Register a listener for an arbitrary [`SignalKind`], not just the
    /// six convenience wrappers below.
    ///
    /// Mirrors the native backend's `DtactSignalStream::new(sig:
    /// libc::c_int)`, which accepts any raw signal number for the same
    /// reason (e.g. `SIGQUIT`, `SIGWINCH`, `SIGALRM`, or any platform-
    /// specific signal this module doesn't have a named wrapper for).
    ///
    /// # Errors
    /// Returns whatever `tokio::signal::unix::signal` returns (e.g. an
    /// invalid signal number, or the OS refusing to install the handler).
    pub fn register(kind: SignalKind) -> io::Result<DtactSignalStream> {
        make(kind)
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
    #[must_use]
    pub fn sigint() -> DtactSignalStream {
        make(SignalKind::interrupt()).expect("dtact-signal: failed to register SIGINT handler")
    }
    /// Register a listener for `SIGTERM`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    #[must_use]
    pub fn sigterm() -> DtactSignalStream {
        make(SignalKind::terminate()).expect("dtact-signal: failed to register SIGTERM handler")
    }
    /// Register a listener for `SIGHUP`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    #[must_use]
    pub fn sighup() -> DtactSignalStream {
        make(SignalKind::hangup()).expect("dtact-signal: failed to register SIGHUP handler")
    }
    /// Register a listener for `SIGUSR1`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    #[must_use]
    pub fn sigusr1() -> DtactSignalStream {
        make(SignalKind::user_defined1()).expect("dtact-signal: failed to register SIGUSR1 handler")
    }
    /// Register a listener for `SIGUSR2`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    #[must_use]
    pub fn sigusr2() -> DtactSignalStream {
        make(SignalKind::user_defined2()).expect("dtact-signal: failed to register SIGUSR2 handler")
    }
    /// Register a listener for `SIGCHLD`.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the signal handler.
    #[must_use]
    pub fn sigchld() -> DtactSignalStream {
        make(SignalKind::child()).expect("dtact-signal: failed to register SIGCHLD handler")
    }
}

#[cfg(windows)]
mod windows {
    use tokio::signal::windows::{CtrlBreak, CtrlC, CtrlClose, CtrlLogoff, CtrlShutdown};

    /// A stream of occurrences of one Windows console-control event.
    ///
    /// Covers `Ctrl+C`, `Ctrl+Break`, window-close, logoff, and shutdown,
    /// backed by tokio's console-handler reactor integration instead of
    /// dtact-signal's own registry. Mirrors the native backend's
    /// `DtactSignalStream` — same five events, see its module doc for
    /// what each one means and the grace-period caveat that applies to
    /// the latter three.
    pub struct DtactSignalStream(CtrlWrapper);

    enum CtrlWrapper {
        C(CtrlC),
        Break(CtrlBreak),
        Close(CtrlClose),
        Logoff(CtrlLogoff),
        Shutdown(CtrlShutdown),
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
                CtrlWrapper::Close(s) => {
                    s.recv().await;
                }
                CtrlWrapper::Logoff(s) => {
                    s.recv().await;
                }
                CtrlWrapper::Shutdown(s) => {
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

    /// Register a listener for the console-window-close
    /// (`CTRL_CLOSE_EVENT`) event.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the console-control handler.
    #[must_use]
    pub fn ctrl_close() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::Close(
            tokio::signal::windows::ctrl_close()
                .expect("dtact-signal: failed to register Ctrl+Close handler"),
        ))
    }

    /// Register a listener for the user-logoff (`CTRL_LOGOFF_EVENT`)
    /// event. Not delivered to services — see `tokio::signal::windows`'s
    /// documentation of the same restriction.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the console-control handler.
    #[must_use]
    pub fn ctrl_logoff() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::Logoff(
            tokio::signal::windows::ctrl_logoff()
                .expect("dtact-signal: failed to register Ctrl+Logoff handler"),
        ))
    }

    /// Register a listener for the system-shutdown
    /// (`CTRL_SHUTDOWN_EVENT`) event.
    ///
    /// # Panics
    /// Panics if the OS refuses to install the console-control handler.
    #[must_use]
    pub fn ctrl_shutdown() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::Shutdown(
            tokio::signal::windows::ctrl_shutdown()
                .expect("dtact-signal: failed to register Ctrl+Shutdown handler"),
        ))
    }
}
