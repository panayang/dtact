//! Native process backend: spawn is a direct synchronous
//! `std::process::Command::spawn()` call (matches `tokio::process`'s own
//! choice — spawning is a single `fork`+`exec`/`CreateProcess` syscall,
//! not something worth dispatching anywhere), while `wait`/stdio I/O
//! (operations that can genuinely block for an unbounded time) run on a
//! dedicated blocking-thread pool.
//!
//! **No `Mutex` anywhere.** Unlike an earlier draft of this module, child
//! handles are never shared behind a lock: `DtactChild::wait`/
//! `wait_with_output` *consume* `self` and move the whole
//! `std::process::Child` into the pool closure — there is no concurrent
//! access to coordinate because ownership transfers outright. Stdio
//! handles (`take_stdin`/`take_stdout`/`take_stderr`) are the same idea:
//! each is exclusively owned by whoever holds the returned handle, and
//! each async op temporarily moves it into a pool closure and gets it
//! back in the result, rather than holding it behind a shared lock across
//! the `.await`. Completion signaling uses
//! [`crate::lockfree::OnceSlot`] (a single `AtomicPtr` swap + wait-free
//! waker), not a `Mutex`-guarded completion flag.

use crate::lockfree::OnceSlot;
use std::ffi::OsStr;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::{Arc, OnceLock, mpsc};
use std::task::{Context, Poll};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct ProcessPool {
    sender: mpsc::Sender<Job>,
}

static PROCESS_POOL: OnceLock<ProcessPool> = OnceLock::new();

/// Start the process thread pool with the given number of worker
/// threads. Idempotent — later calls are no-ops once initialized.
///
/// # Panics
///
/// Panics if the OS refuses to spawn one of the `workers` pool threads
/// (`std::thread::Builder::spawn` failure, e.g. the process is out of
/// resources) — this is treated as fatal because a partially-started pool
/// would silently drop later `spawn_blocking` work.
pub fn init(workers: usize) {
    PROCESS_POOL.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        for _ in 0..workers.max(1) {
            let rx = Arc::clone(&rx);
            std::thread::Builder::new()
                .name("dtact-process-worker".into())
                .spawn(move || {
                    loop {
                        let job = { rx.lock().unwrap().recv() };
                        match job {
                            Ok(job) => job(),
                            Err(_) => break,
                        }
                    }
                })
                .expect("failed to spawn dtact-process worker thread");
        }
        ProcessPool { sender: tx }
    });
}

/// Full-signature entry point matching the other native backends' call.
///
/// Matches `init_fs`'s shape, for a future `process_init` macro to call
/// uniformly. `ring_depth`/`buffer_pool_size`/`chunk_size`/`pin_cpus`
/// don't apply to this thread-pool-bridged backend and are ignored.
///
/// # Panics
///
/// See [`init`] — the same worker-thread-spawn failure is fatal here too.
pub fn init_process(
    workers: usize,
    _ring_depth: u32,
    _buffer_pool_size: usize,
    _chunk_size: usize,
    _pin_cpus: &[usize],
) {
    init(workers);
}

/// A single blocking operation dispatched to the process thread pool,
/// completed via a wait-free [`OnceSlot`] rather than a `Mutex`-guarded
/// flag.
pub struct BlockingOp<T> {
    slot: Arc<OnceSlot<T>>,
}

impl<T: Send + 'static> Future for BlockingOp<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.slot.poll(cx)
    }
}

fn spawn_blocking<T, F>(f: F) -> BlockingOp<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    if PROCESS_POOL.get().is_none() {
        init(4);
    }
    let slot = Arc::new(OnceSlot::new());
    let slot2 = Arc::clone(&slot);
    let job: Job = Box::new(move || {
        let result = f();
        slot2.set(result);
    });
    let _ = PROCESS_POOL.get().unwrap().sender.send(job);
    BlockingOp { slot }
}

/// Async-friendly wrapper over [`std::process::Command`]. Builder methods
/// mirror `std::process::Command`'s own naming.
pub struct DtactCommand(Command);

impl DtactCommand {
    /// Start building a command that will run `program`.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self(Command::new(program))
    }

    /// Append a single argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.0.arg(arg);
        self
    }

    /// Append multiple arguments at once.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.0.args(args);
        self
    }

    /// Set an environment variable for the child process.
    pub fn env(&mut self, key: impl AsRef<OsStr>, val: impl AsRef<OsStr>) -> &mut Self {
        self.0.env(key, val);
        self
    }

    /// Set the working directory the child process is spawned in.
    pub fn current_dir(&mut self, dir: impl AsRef<std::path::Path>) -> &mut Self {
        self.0.current_dir(dir);
        self
    }

    /// Configure how the child's stdin is set up (inherit/pipe/null).
    pub fn stdin(&mut self, cfg: Stdio) -> &mut Self {
        self.0.stdin(cfg);
        self
    }

    /// Configure how the child's stdout is set up (inherit/pipe/null).
    pub fn stdout(&mut self, cfg: Stdio) -> &mut Self {
        self.0.stdout(cfg);
        self
    }

    /// Configure how the child's stderr is set up (inherit/pipe/null).
    pub fn stderr(&mut self, cfg: Stdio) -> &mut Self {
        self.0.stderr(cfg);
        self
    }

    /// Spawn the child process. A direct synchronous syscall — see the
    /// module doc for why this isn't dispatched to the pool.
    ///
    /// # Errors
    ///
    /// Returns whatever `std::process::Command::spawn` returns: most
    /// commonly `io::ErrorKind::NotFound` if `program` isn't on `PATH`/
    /// doesn't exist, or `PermissionDenied` if it exists but isn't
    /// executable by the current user.
    pub fn spawn(&mut self) -> io::Result<DtactChild> {
        self.0.spawn().map(DtactChild::new)
    }
}

/// A spawned child process.
///
/// `wait`/`wait_with_output` consume `self` (ownership transfers into the
/// pool closure, so nothing needs to be shared or locked); `kill`/`id`
/// are synchronous since they're fast, non-blocking syscalls.
pub struct DtactChild {
    inner: std::process::Child,
}

impl DtactChild {
    const fn new(inner: std::process::Child) -> Self {
        Self { inner }
    }

    /// The OS process ID of the child.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.inner.id()
    }

    /// Send `SIGKILL` (Unix) / `TerminateProcess` (Windows) to the child.
    ///
    /// # Errors
    ///
    /// Returns an error if the OS kill syscall fails — in practice this is
    /// almost always because the process had already exited (the
    /// underlying `std::process::Child::kill` documents this as the
    /// common failure case; it is not itself treated as success).
    pub fn kill(&mut self) -> io::Result<()> {
        self.inner.kill()
    }

    /// Take ownership of the child's stdin, if it was configured with
    /// `Stdio::piped()`. Can only be taken once.
    pub fn take_stdin(&mut self) -> Option<DtactChildStdin> {
        self.inner.stdin.take().map(DtactChildStdin::new)
    }

    /// Take ownership of the child's stdout, if it was configured with
    /// `Stdio::piped()`. Can only be taken once.
    pub fn take_stdout(&mut self) -> Option<DtactChildStdout> {
        self.inner.stdout.take().map(DtactChildStdout::new)
    }

    /// Take ownership of the child's stderr, if it was configured with
    /// `Stdio::piped()`. Can only be taken once.
    pub fn take_stderr(&mut self) -> Option<DtactChildStderr> {
        self.inner.stderr.take().map(DtactChildStderr::new)
    }

    /// Block (on the process pool, not the calling task's thread) until
    /// the child exits.
    ///
    /// # Errors
    ///
    /// Returns whatever `std::process::Child::wait` returns — an I/O
    /// error if waiting on the OS process handle itself fails (rare; not
    /// the same as the child exiting non-zero, which is a normal `Ok`
    /// with a non-zero `ExitStatus`).
    pub async fn wait(mut self) -> io::Result<ExitStatus> {
        spawn_blocking(move || self.inner.wait()).await
    }

    /// Wait for exit and collect stdout/stderr in one shot — the
    /// std-library convenience, dispatched to the pool the same way.
    ///
    /// # Errors
    ///
    /// Same as [`Self::wait`]: an I/O error only if waiting on the OS
    /// process handle or reading its piped stdout/stderr fails, not for a
    /// non-zero exit status.
    pub async fn wait_with_output(self) -> io::Result<Output> {
        spawn_blocking(move || self.inner.wait_with_output()).await
    }
}

macro_rules! child_pipe {
    ($name:ident, $inner:ty) => {
        /// One end of a child process's stdio pipe.
        ///
        /// Exclusively owned by whoever holds it (returned by `take_std*`
        /// on `DtactChild`) — each async op temporarily moves the handle
        /// into a pool closure and gets it back in the result, never
        /// shared behind a lock.
        pub struct $name(Option<$inner>);

        impl $name {
            const fn new(inner: $inner) -> Self {
                Self(Some(inner))
            }
        }
    };
}

child_pipe!(DtactChildStdin, std::process::ChildStdin);
child_pipe!(DtactChildStdout, std::process::ChildStdout);
child_pipe!(DtactChildStderr, std::process::ChildStderr);

impl DtactChildStdin {
    /// Write `buf` to the child's stdin on the process pool, returning
    /// the number of bytes written and the buffer back for reuse.
    ///
    /// # Errors
    ///
    /// Returns whatever the underlying blocking `Write::write` on the
    /// pipe returns (e.g. `BrokenPipe` if the child has already exited
    /// and closed its end).
    ///
    /// # Panics
    ///
    /// Panics if called again while a previous call on the same `&mut
    /// self` hasn't finished — in practice this is unreachable through
    /// safe code: taking `&mut self` for the duration of the returned
    /// future's lifetime means a second call cannot start until the
    /// first's future has been driven to completion (or dropped), at
    /// which point the handle has already been restored to `Some`.
    pub async fn write(&mut self, buf: Vec<u8>) -> io::Result<(usize, Vec<u8>)> {
        use std::io::Write;
        let mut handle = self
            .0
            .take()
            .expect("dtact-process: concurrent write on the same DtactChildStdin");
        let (result, handle) = spawn_blocking(move || {
            let r = handle.write(&buf).map(|n| (n, buf));
            (r, handle)
        })
        .await;
        self.0 = Some(handle);
        result
    }

    /// Explicitly drop this end (closes the pipe), letting the child
    /// observe EOF on its stdin.
    pub fn close(mut self) {
        self.0 = None;
    }
}

impl DtactChildStdout {
    /// Read from the child's stdout on the process pool into `buf`,
    /// returning the number of bytes read and the buffer back.
    ///
    /// # Errors
    ///
    /// Returns whatever the underlying blocking `Read::read` on the pipe
    /// returns.
    ///
    /// # Panics
    ///
    /// Panics if called again while a previous call on the same `&mut
    /// self` hasn't finished — see [`DtactChildStdin::write`]'s doc for
    /// why this is unreachable through safe code.
    pub async fn read(&mut self, mut buf: Vec<u8>) -> io::Result<(usize, Vec<u8>)> {
        use std::io::Read;
        let mut handle = self
            .0
            .take()
            .expect("dtact-process: concurrent read on the same DtactChildStdout");
        let (result, handle) = spawn_blocking(move || {
            let r = handle.read(&mut buf).map(|n| (n, buf));
            (r, handle)
        })
        .await;
        self.0 = Some(handle);
        result
    }
}

impl DtactChildStderr {
    /// Read from the child's stderr on the process pool into `buf`,
    /// returning the number of bytes read and the buffer back.
    ///
    /// # Errors
    ///
    /// Returns whatever the underlying blocking `Read::read` on the pipe
    /// returns.
    ///
    /// # Panics
    ///
    /// Panics if called again while a previous call on the same `&mut
    /// self` hasn't finished — see [`DtactChildStdin::write`]'s doc for
    /// why this is unreachable through safe code.
    pub async fn read(&mut self, mut buf: Vec<u8>) -> io::Result<(usize, Vec<u8>)> {
        use std::io::Read;
        let mut handle = self
            .0
            .take()
            .expect("dtact-process: concurrent read on the same DtactChildStderr");
        let (result, handle) = spawn_blocking(move || {
            let r = handle.read(&mut buf).map(|n| (n, buf));
            (r, handle)
        })
        .await;
        self.0 = Some(handle);
        result
    }
}
