//! Windows native signal backend: `SetConsoleCtrlHandler`, broadcasting
//! Ctrl+C / Ctrl+Break to every registered listener via
//! [`super::registry::ListenerRegistry`].
//!
//! Narrower than the Unix backend by necessity — Windows has no general
//! POSIX-style signal delivery. `CTRL_CLOSE_EVENT`/`CTRL_LOGOFF_EVENT`/
//! `CTRL_SHUTDOWN_EVENT` are deliberately left unhandled (the handler
//! returns `FALSE` for them, so the OS's default handling still applies —
//! this backend doesn't try to intercept process teardown, only the two
//! signals that have a real Unix-signal analogue).
//!
//! Unlike a Unix signal handler, a console control handler runs on an
//! OS-created thread in normal (non-restricted) context, so the handler
//! here can call straight into `ListenerRegistry::broadcast` — no self-
//! pipe indirection needed.

use super::registry::{DeadOnDrop, ListenerRegistry};
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::{FALSE, TRUE};
use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, CTRL_C_EVENT, SetConsoleCtrlHandler};

// Named `Bool` (not `BOOL`) purely to satisfy `clippy::upper_case_acronyms`
// — this is still the Win32 `BOOL` (`i32`) ABI type, just spelled with
// Rust naming conventions rather than the Win32 SDK's.
type Bool = i32;

static CTRLC: OnceLock<ListenerRegistry> = OnceLock::new();
static CTRLBREAK: OnceLock<ListenerRegistry> = OnceLock::new();
static HANDLER_INSTALLED: OnceLock<()> = OnceLock::new();

fn ctrlc_registry() -> &'static ListenerRegistry {
    CTRLC.get_or_init(ListenerRegistry::new)
}

fn ctrlbreak_registry() -> &'static ListenerRegistry {
    CTRLBREAK.get_or_init(ListenerRegistry::new)
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
