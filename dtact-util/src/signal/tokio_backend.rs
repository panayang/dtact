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

    pub struct DtactSignalStream(Signal);

    impl DtactSignalStream {
        pub async fn recv(&mut self) {
            self.0.recv().await;
        }
    }

    fn make(kind: SignalKind) -> io::Result<DtactSignalStream> {
        signal(kind).map(DtactSignalStream)
    }

    pub fn sigint() -> DtactSignalStream {
        make(SignalKind::interrupt()).expect("dtact-signal: failed to register SIGINT handler")
    }
    pub fn sigterm() -> DtactSignalStream {
        make(SignalKind::terminate()).expect("dtact-signal: failed to register SIGTERM handler")
    }
    pub fn sighup() -> DtactSignalStream {
        make(SignalKind::hangup()).expect("dtact-signal: failed to register SIGHUP handler")
    }
    pub fn sigusr1() -> DtactSignalStream {
        make(SignalKind::user_defined1()).expect("dtact-signal: failed to register SIGUSR1 handler")
    }
    pub fn sigusr2() -> DtactSignalStream {
        make(SignalKind::user_defined2()).expect("dtact-signal: failed to register SIGUSR2 handler")
    }
    pub fn sigchld() -> DtactSignalStream {
        make(SignalKind::child()).expect("dtact-signal: failed to register SIGCHLD handler")
    }
}

#[cfg(windows)]
mod windows {
    use tokio::signal::windows::{CtrlBreak, CtrlC};

    pub struct DtactSignalStream(CtrlWrapper);

    enum CtrlWrapper {
        C(CtrlC),
        Break(CtrlBreak),
    }

    impl DtactSignalStream {
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

    pub fn ctrl_c() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::C(
            tokio::signal::windows::ctrl_c()
                .expect("dtact-signal: failed to register Ctrl+C handler"),
        ))
    }

    pub fn ctrl_break() -> DtactSignalStream {
        DtactSignalStream(CtrlWrapper::Break(
            tokio::signal::windows::ctrl_break()
                .expect("dtact-signal: failed to register Ctrl+Break handler"),
        ))
    }
}
