//! Windows native signal backend: `SetConsoleCtrlHandler`, broadcasting
//! console-control events to every registered listener via
//! [`super::registry::ListenerRegistry`].
//!
//! Covers all five events `tokio::signal::windows` does: `CTRL_C_EVENT`,
//! `CTRL_BREAK_EVENT`, `CTRL_CLOSE_EVENT`, `CTRL_LOGOFF_EVENT`, and
//! `CTRL_SHUTDOWN_EVENT`. For the latter three the OS gives the process
//! very little time to react once the handler returns — returning `TRUE`
//! (as this handler does for every event it recognizes) tells Windows
//! "handled, don't run your own default action", the same contract
//! `tokio::signal::windows::CtrlClose`/`CtrlLogoff`/`CtrlShutdown` rely on
//! to let application code observe the event before the process is torn
//! down. Callers that register for these three must still act promptly —
//! this backend only relays the notification, it doesn't extend Windows'
//! shutdown grace period.
//!
//! Unlike a Unix signal handler, a console control handler runs on an
//! OS-created thread in normal (non-restricted) context, so the handler
//! here can call straight into `ListenerRegistry::broadcast` — no self-
//! pipe indirection needed.

use super::registry::{DeadOnDrop, ListenerRegistry};
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::{FALSE, TRUE};
use windows_sys::Win32::System::Console::{
    CTRL_BREAK_EVENT, CTRL_C_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT,
    SetConsoleCtrlHandler,
};

// Named `Bool` (not `BOOL`) purely to satisfy `clippy::upper_case_acronyms`
// — this is still the Win32 `BOOL` (`i32`) ABI type, just spelled with
// Rust naming conventions rather than the Win32 SDK's.
type Bool = i32;

static CTRLC: OnceLock<ListenerRegistry> = OnceLock::new();
static CTRLBREAK: OnceLock<ListenerRegistry> = OnceLock::new();
static CTRLCLOSE: OnceLock<ListenerRegistry> = OnceLock::new();
static CTRLLOGOFF: OnceLock<ListenerRegistry> = OnceLock::new();
static CTRLSHUTDOWN: OnceLock<ListenerRegistry> = OnceLock::new();
static HANDLER_INSTALLED: OnceLock<()> = OnceLock::new();

fn ctrlc_registry() -> &'static ListenerRegistry {
    CTRLC.get_or_init(ListenerRegistry::new)
}

fn ctrlbreak_registry() -> &'static ListenerRegistry {
    CTRLBREAK.get_or_init(ListenerRegistry::new)
}

fn ctrlclose_registry() -> &'static ListenerRegistry {
    CTRLCLOSE.get_or_init(ListenerRegistry::new)
}

fn ctrllogoff_registry() -> &'static ListenerRegistry {
    CTRLLOGOFF.get_or_init(ListenerRegistry::new)
}

fn ctrlshutdown_registry() -> &'static ListenerRegistry {
    CTRLSHUTDOWN.get_or_init(ListenerRegistry::new)
}

unsafe extern "system" fn handler(ctrl_type: u32) -> Bool {
    match ctrl_type {
        CTRL_C_EVENT => {
            ctrlc_registry().broadcast();
            TRUE
        }
        CTRL_BREAK_EVENT => {
            ctrlbreak_registry().broadcast();
            TRUE
        }
        CTRL_CLOSE_EVENT => {
            ctrlclose_registry().broadcast();
            TRUE
        }
        CTRL_LOGOFF_EVENT => {
            ctrllogoff_registry().broadcast();
            TRUE
        }
        CTRL_SHUTDOWN_EVENT => {
            ctrlshutdown_registry().broadcast();
            TRUE
        }
        _ => FALSE,
    }
}

fn ensure_handler_installed() {
    HANDLER_INSTALLED.get_or_init(|| {
        let ok = unsafe { SetConsoleCtrlHandler(Some(handler), TRUE) };
        assert!(ok != 0, "dtact-signal: SetConsoleCtrlHandler failed");
    });
}

/// A stream of a specific console-control event's deliveries — call
/// `.recv().await` repeatedly, mirroring `tokio::signal::windows`.
pub struct DtactSignalStream {
    state: std::sync::Arc<super::registry::ListenerState>,
    _dead_on_drop: DeadOnDrop,
}

impl DtactSignalStream {
    fn from_registry(reg: &'static ListenerRegistry) -> Self {
        ensure_handler_installed();
        let state = reg.register();
        Self {
            state: std::sync::Arc::clone(&state),
            _dead_on_drop: DeadOnDrop(state),
        }
    }

    /// Wait for the next delivery of this control event.
    pub async fn recv(&self) {
        std::future::poll_fn(|cx| self.state.poll_recv(cx)).await;
    }
}

/// Subscribe to Ctrl+C (`CTRL_C_EVENT`) deliveries.
#[must_use]
pub fn ctrl_c() -> DtactSignalStream {
    DtactSignalStream::from_registry(ctrlc_registry())
}

/// Subscribe to Ctrl+Break (`CTRL_BREAK_EVENT`) deliveries.
#[must_use]
pub fn ctrl_break() -> DtactSignalStream {
    DtactSignalStream::from_registry(ctrlbreak_registry())
}

/// Subscribe to console-window-close (`CTRL_CLOSE_EVENT`) deliveries.
///
/// Fires when the user closes the console window or the parent console
/// process exits. Windows allows only a short grace period (a few
/// seconds) after this before force-terminating the process.
#[must_use]
pub fn ctrl_close() -> DtactSignalStream {
    DtactSignalStream::from_registry(ctrlclose_registry())
}

/// Subscribe to user-logoff (`CTRL_LOGOFF_EVENT`) deliveries. Not sent to
/// services, which is the same restriction `tokio::signal::windows`
/// documents for its equivalent.
#[must_use]
pub fn ctrl_logoff() -> DtactSignalStream {
    DtactSignalStream::from_registry(ctrllogoff_registry())
}

/// Subscribe to system-shutdown (`CTRL_SHUTDOWN_EVENT`) deliveries.
#[must_use]
pub fn ctrl_shutdown() -> DtactSignalStream {
    DtactSignalStream::from_registry(ctrlshutdown_registry())
}
