//! Unix native signal backend: a self-pipe fed by an async-signal-safe
//! `sigaction` handler, drained by a dedicated reader thread that
//! broadcasts each delivery to every registered listener via
//! [`super::registry::ListenerRegistry`].
//!
//! The handler itself does the *minimum* async-signal-safe work possible:
//! a single `write(2)` of one byte (the signal number) to a pre-opened
//! pipe fd. Everything else — bucketing by signal number, waking
//! listeners, growing any data structure — happens on the reader thread,
//! well outside signal-handler context, where normal Rust code
//! (allocation, atomics beyond the bare minimum, `libc::read`) is safe to
//! run.

use super::registry::{DeadOnDrop, ListenerRegistry};
use std::os::unix::io::RawFd;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, Ordering};

const NSIG: usize = 65; // enough for every standard POSIX signal number

static WRITE_FD: AtomicI32 = AtomicI32::new(-1);
static REGISTRY: OnceLock<Box<[ListenerRegistry]>> = OnceLock::new();
static INSTALLED: OnceLock<Box<[std::sync::atomic::AtomicBool]>> = OnceLock::new();

fn registry() -> &'static [ListenerRegistry] {
    REGISTRY.get_or_init(|| {
        let mut v = Vec::with_capacity(NSIG);
        for _ in 0..NSIG {
            v.push(ListenerRegistry::new());
        }
        v.into_boxed_slice()
    })
}

fn installed_flags() -> &'static [std::sync::atomic::AtomicBool] {
    INSTALLED.get_or_init(|| {
        let mut v = Vec::with_capacity(NSIG);
        for _ in 0..NSIG {
            v.push(std::sync::atomic::AtomicBool::new(false));
        }
        v.into_boxed_slice()
    })
}

extern "C" fn handler(sig: libc::c_int) {
    // Async-signal-safe: only `write(2)` on an already-open fd, no
    // allocation, no locking. A full pipe silently drops the byte —
    // acceptable, since ListenerState::pending already coalesces bursts
    // and this is just the wakeup nudge, not the payload.
    let fd = WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = sig as u8;
        unsafe {
            libc::write(fd, &byte as *const u8 as *const libc::c_void, 1);
        }
    }
}

fn ensure_pipe_and_reader() {
    if WRITE_FD.load(Ordering::Acquire) >= 0 {
        return;
    }
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "dtact-signal: pipe(2) failed");
    let (read_fd, write_fd): (RawFd, RawFd) = (fds[0], fds[1]);

    // CAS so only one thread's pipe/reader actually gets installed even
    // if two registrations race to initialize.
    if WRITE_FD
        .compare_exchange(-1, write_fd, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        return;
    }

    std::thread::Builder::new()
        .name("dtact-signal-reader".into())
        .spawn(move || reader_loop(read_fd))
        .expect("failed to spawn dtact-signal reader thread");
}

fn reader_loop(read_fd: RawFd) {
    let regs = registry();
    let mut byte = [0u8; 1];
    loop {
        let n = unsafe { libc::read(read_fd, byte.as_mut_ptr() as *mut libc::c_void, 1) };
        if n == 1 {
            let sig = byte[0] as usize;
            if sig < regs.len() {
                regs[sig].broadcast();
            }
        }
        // n <= 0 (interrupted or transient error): just retry the read.
    }
}

fn ensure_handler_installed(sig: libc::c_int) {
    ensure_pipe_and_reader();
    let flags = installed_flags();
    let idx = sig as usize;
    if idx >= flags.len() {
        return;
    }
    if flags[idx]
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return; // already installed by an earlier registration
    }
    unsafe {
        let mut act: libc::sigaction = std::mem::zeroed();
        act.sa_sigaction = handler as *const () as usize;
        libc::sigemptyset(&mut act.sa_mask);
        act.sa_flags = libc::SA_RESTART;
        libc::sigaction(sig, &act, std::ptr::null_mut());
    }
}

/// A stream of a specific signal's deliveries — call `.recv().await`
/// repeatedly, once per delivery, mirroring `tokio::signal::unix::Signal`.
pub struct DtactSignalStream {
    state: std::sync::Arc<super::registry::ListenerState>,
    _dead_on_drop: DeadOnDrop,
}

impl DtactSignalStream {
    /// Register a listener for raw Unix signal number `sig` (e.g.
    /// `libc::SIGINT`, `libc::SIGTERM`, `libc::SIGUSR1`).
    pub fn new(sig: libc::c_int) -> Self {
        ensure_handler_installed(sig);
        let state = registry()[sig as usize].register();
        Self {
            state: std::sync::Arc::clone(&state),
            _dead_on_drop: DeadOnDrop(state),
        }
    }

    /// Wait for the next delivery of this signal.
    pub async fn recv(&self) {
        std::future::poll_fn(|cx| self.state.poll_recv(cx)).await
    }
}

pub fn sigint() -> DtactSignalStream {
    DtactSignalStream::new(libc::SIGINT)
}

pub fn sigterm() -> DtactSignalStream {
    DtactSignalStream::new(libc::SIGTERM)
}

pub fn sighup() -> DtactSignalStream {
    DtactSignalStream::new(libc::SIGHUP)
}

pub fn sigusr1() -> DtactSignalStream {
    DtactSignalStream::new(libc::SIGUSR1)
}

pub fn sigusr2() -> DtactSignalStream {
    DtactSignalStream::new(libc::SIGUSR2)
}

pub fn sigchld() -> DtactSignalStream {
    DtactSignalStream::new(libc::SIGCHLD)
}
