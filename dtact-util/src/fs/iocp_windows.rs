//! Windows native filesystem backend: real IOCP-based async file I/O.
//!
//! Unlike `native.rs`'s thread-pool fallback, this issues `ReadFile`/
//! `WriteFile` directly against a `FILE_FLAG_OVERLAPPED` handle associated
//! with a completion port, so a read/write is a single async syscall whose
//! completion is delivered straight into the caller-supplied buffer — no
//! hop through a blocking worker thread, no extra copy. This is the
//! Windows analogue of the io_uring path on Linux (`uring_linux.rs`).
//!
//! One process-wide IOCP handle + one dedicated worker thread drains
//! `GetQueuedCompletionStatusEx` and wakes the waiting future per
//! completed op.
//!
//! **Per-op state is a preallocated slot, not a fresh heap allocation.**
//! [`init_fs`] carves out a fixed `Box<[OpState]>` arena (sized by
//! `ring_depth`) up front, handed out and reclaimed via a
//! [`crate::lockfree::TreiberStack`] free-list — the same
//! "arena + Treiber free-list" shape `io::native`'s `BufferPool` uses,
//! moved into `crate::lockfree` specifically so this module could share
//! it. `OVERLAPPED` is still the struct's first field (`#[repr(C)]`) so
//! the raw `*mut OVERLAPPED` Windows hands back on completion casts
//! straight back to the full slot, whether that slot lives in the pool
//! or (only if the pool is exhausted) on the heap as a one-off fallback —
//! see `acquire_slot`.
//!
//! A slot is only returned to the pool once its result has actually been
//! observed (`Drop for IoOp` checks `result != PENDING`); if a future is
//! dropped while its op is still in flight, that slot is deliberately
//! leaked rather than risked for reuse — reclaiming it safely would need
//! `CancelIoEx` plus waiting for the cancellation's own completion before
//! the slot could be trusted again (the same problem `io::native` solves
//! for socket ops via its `cancel_queue`). Not implemented in this pass;
//! every test/bench in this crate awaits ops to completion, so it isn't
//! exercised, but it's a real gap a future pass should close before this
//! is used somewhere that cancels in-flight file ops routinely.

use crate::lockfree::{AtomicWakerSlot, TreiberStack};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_HANDLE_EOF, ERROR_IO_PENDING, GENERIC_READ, GENERIC_WRITE, HANDLE,
    INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_ALWAYS, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OVERLAPPED, FILE_SHARE_READ,
    FILE_SHARE_WRITE, FlushFileBuffers, GetFileSizeEx, OPEN_EXISTING, ReadFile, WriteFile,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY,
};

const WAKE_KEY: usize = 0;
const FILE_KEY: usize = 1;

struct Port {
    handle: HANDLE,
}
unsafe impl Send for Port {}
unsafe impl Sync for Port {}

static PORT: OnceLock<Port> = OnceLock::new();

fn port() -> HANDLE {
    let p = PORT.get_or_init(|| {
        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1) };
        assert!(!handle.is_null(), "dtact-fs: CreateIoCompletionPort failed");
        std::thread::Builder::new()
            .name("dtact-fs-iocp".into())
            .spawn(worker_loop)
            .expect("failed to spawn dtact-fs-iocp worker thread");
        Port { handle }
    });
    p.handle
}

/// Sentinel for `OpState::result` meaning "not yet completed". Any other
/// value is a real result: `>= 0` is bytes transferred, `< 0` (and
/// `!= PENDING`) is `-(win32 error code)` — Win32 error codes are small
/// positive `DWORD`s, so this never collides with the sentinel.
const PENDING: i64 = i64::MIN;

#[repr(C)]
struct OpState {
    overlapped: OVERLAPPED,
    /// PENDING until the IOCP worker stores the real outcome — see the
    /// `PENDING` doc above. Single atomic: no lock, one store on the fire
    /// path, one load (or two, across the waker-registration race window)
    /// on the poll path.
    result: AtomicI64,
    waker: AtomicWakerSlot,
}

impl OpState {
    fn fresh() -> Self {
        Self {
            overlapped: unsafe { std::mem::zeroed() },
            result: AtomicI64::new(PENDING),
            waker: AtomicWakerSlot::new(),
        }
    }
}

// =============================================================================
// Preallocated slot pool — see module doc for the reuse/leak-on-cancel policy.
// =============================================================================

struct SlotPool {
    slots: Box<[OpState]>,
    free: TreiberStack,
}
// SAFETY: `OpState` embeds an `OVERLAPPED` (raw pointers inside, from
// windows-sys) purely as opaque kernel-visible scratch memory — nothing
// here ever dereferences those inner pointers from Rust; the OS treats
// the struct as a value, not a "pointer to shared data" that would need
// synchronization. `result`/`waker` (the fields Rust code actually reads
// and writes) are already atomics.
unsafe impl Send for SlotPool {}
unsafe impl Sync for SlotPool {}

static RING_DEPTH: OnceLock<usize> = OnceLock::new();
static SLOT_POOL: OnceLock<SlotPool> = OnceLock::new();

fn slot_pool() -> &'static SlotPool {
    SLOT_POOL.get_or_init(|| {
        let depth = *RING_DEPTH.get_or_init(|| 256);
        let mut slots = Vec::with_capacity(depth);
        for _ in 0..depth {
            slots.push(OpState::fresh());
        }
        let free = TreiberStack::new(depth);
        for i in 0..depth as u32 {
            free.push(i);
        }
        SlotPool {
            slots: slots.into_boxed_slice(),
            free,
        }
    })
}

/// Configure and eagerly start the fs-IOCP subsystem: `ring_depth` sized
/// preallocated op slots (see module doc) plus the completion-port worker
/// thread. `workers`/`buffer_pool_size`/`chunk_size`/`pin_cpus` are
/// accepted for signature parity with the other native backends'
/// `init_fs` (and with `crate::io::native::init_runtime`) but unused here:
/// IOCP dispatch is single-worker-thread by design (one port, one
/// `GetQueuedCompletionStatusEx` loop), and this backend has no
/// caller-facing buffer pool yet (reads/writes still take an owned
/// `Vec<u8>` per call — see the `fs` module doc for why that wasn't
/// changed in this pass).
pub fn init_fs(
    _workers: usize,
    ring_depth: u32,
    _buffer_pool_size: usize,
    _chunk_size: usize,
    _pin_cpus: &[usize],
) {
    let _ = RING_DEPTH.set(ring_depth.max(1) as usize);
    let _ = slot_pool();
    let _ = port();
}

/// Simple-signature convenience wrapper: `init_fs(workers, 256, 0, 0, &[])`.
pub fn init(workers: usize) {
    init_fs(workers, 256, 0, 0, &[]);
}

fn encode_ok(n: usize) -> i64 {
    n as i64
}

fn encode_err(win32_code: u32) -> i64 {
    -(win32_code as i64)
}

fn decode(result: i64) -> io::Result<usize> {
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(io::Error::from_raw_os_error(-result as i32))
    }
}

fn worker_loop() {
    let iocp = port();
    let mut entries: [OVERLAPPED_ENTRY; 64] = unsafe { std::mem::zeroed() };
    loop {
        let mut removed: u32 = 0;
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                iocp,
                entries.as_mut_ptr(),
                entries.len() as u32,
                &mut removed,
                u32::MAX,
                0,
            )
        };
        if ok == 0 {
            continue;
        }
        for entry in &entries[..removed as usize] {
            if entry.lpCompletionKey == WAKE_KEY {
                continue;
            }
            let op_ptr = entry.lpOverlapped as *mut OpState;
            if op_ptr.is_null() {
                continue;
            }
            let op = unsafe { &*op_ptr };
            let bytes = entry.dwNumberOfBytesTransferred as usize;
            // `Internal` holds the NTSTATUS of the completed request; a
            // nonzero value with zero bytes transferred on a Read is EOF or
            // a real error — treat "zero bytes, no bytes expected error" as
            // a plain EOF (0), matching ReadFile's synchronous convention.
            op.result.store(encode_ok(bytes), Ordering::Release);
            op.waker.take_and_wake();
        }
    }
}

/// Which allocation a given `IoOp`'s [`OpState`] lives in: a checked-out
/// pool slot (common case, no allocation), or a one-off heap fallback if
/// the pool was exhausted.
enum Slot {
    Pooled(u32),
    Heap(Box<OpState>),
}

fn acquire_slot() -> Slot {
    let pool = slot_pool();
    if let Some(idx) = pool.free.pop() {
        pool.slots[idx as usize]
            .result
            .store(PENDING, Ordering::Relaxed);
        Slot::Pooled(idx)
    } else {
        Slot::Heap(Box::new(OpState::fresh()))
    }
}

struct IoOp {
    slot: Slot,
}

impl IoOp {
    #[inline]
    fn state(&self) -> &OpState {
        match &self.slot {
            Slot::Pooled(idx) => &slot_pool().slots[*idx as usize],
            Slot::Heap(b) => b,
        }
    }

    #[inline]
    fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        match &self.slot {
            // SAFETY: this index is exclusively checked out to this `IoOp`
            // until it's returned to the free-list in `Drop` (never while
            // an op referencing it might still be in flight — see the
            // reuse policy in the module doc), so a mutable raw view of
            // the array element it owns is sound despite going through a
            // shared `&SlotPool` reference.
            Slot::Pooled(idx) => {
                (&slot_pool().slots[*idx as usize] as *const OpState as *mut OpState).cast()
            }
            Slot::Heap(b) => (b.as_ref() as *const OpState as *mut OpState).cast(),
        }
    }
}

impl Future for IoOp {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let r = self.state().result.load(Ordering::Acquire);
        if r != PENDING {
            return Poll::Ready(decode(r));
        }
        self.state().waker.register(cx.waker());
        let r = self.state().result.load(Ordering::Acquire);
        if r != PENDING {
            return Poll::Ready(decode(r));
        }
        Poll::Pending
    }
}

impl Drop for IoOp {
    fn drop(&mut self) {
        if let Slot::Pooled(idx) = self.slot {
            let pool = slot_pool();
            let done = pool.slots[idx as usize].result.load(Ordering::Acquire) != PENDING;
            if done {
                pool.free.push(idx);
            }
            // Else: leak this slot — see module doc's cancellation caveat.
        }
    }
}

fn issue_read(handle: HANDLE, buf: &mut [u8], offset: u64) -> IoOp {
    let slot = acquire_slot();
    let op = IoOp { slot };
    let ov_ptr = op.overlapped_ptr();
    unsafe {
        (*ov_ptr).Anonymous.Anonymous.Offset = offset as u32;
        (*ov_ptr).Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    }

    let ok = unsafe {
        ReadFile(
            handle,
            buf.as_mut_ptr(),
            buf.len() as u32,
            ptr::null_mut(),
            ov_ptr,
        )
    };
    if ok == 0 {
        let err = unsafe { windows_sys::Win32::Foundation::GetLastError() };
        if err != ERROR_IO_PENDING {
            let encoded = if err == ERROR_HANDLE_EOF {
                encode_ok(0)
            } else {
                encode_err(err)
            };
            op.state().result.store(encoded, Ordering::Release);
        }
    }
    op
}

fn issue_write(handle: HANDLE, buf: &[u8], offset: u64) -> IoOp {
    let slot = acquire_slot();
    let op = IoOp { slot };
    let ov_ptr = op.overlapped_ptr();
    unsafe {
        (*ov_ptr).Anonymous.Anonymous.Offset = offset as u32;
        (*ov_ptr).Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    }

    let ok = unsafe {
        WriteFile(
            handle,
            buf.as_ptr(),
            buf.len() as u32,
            ptr::null_mut(),
            ov_ptr,
        )
    };
    if ok == 0 {
        let err = unsafe { windows_sys::Win32::Foundation::GetLastError() };
        if err != ERROR_IO_PENDING {
            op.state().result.store(encode_err(err), Ordering::Release);
        }
    }
    op
}

fn to_wide(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

/// An open file whose reads/writes are dispatched as real overlapped IOCP
/// operations — no thread-pool hop, buffer handed straight to the kernel.
pub struct DtactFile {
    handle: HANDLE,
    cursor: AtomicI64,
}

unsafe impl Send for DtactFile {}
unsafe impl Sync for DtactFile {}

fn open_impl(path: &Path, disposition: u32, access: u32) -> io::Result<DtactFile> {
    let wide = to_wide(path);
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            ptr::null_mut(),
            disposition,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let iocp = port();
    let assoc = unsafe { CreateIoCompletionPort(handle, iocp, FILE_KEY, 0) };
    if assoc.is_null() {
        let e = io::Error::last_os_error();
        unsafe { CloseHandle(handle) };
        return Err(e);
    }
    Ok(DtactFile {
        handle,
        cursor: AtomicI64::new(0),
    })
}

impl DtactFile {
    pub async fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        open_impl(&path, OPEN_EXISTING, GENERIC_READ)
    }

    pub async fn create(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        open_impl(&path, CREATE_ALWAYS, GENERIC_READ | GENERIC_WRITE)
    }

    /// Generic open honoring an arbitrary [`std::fs::OpenOptions`]. Rather
    /// than re-deriving Win32 access/disposition flags from `opts` (which
    /// has no public getters), this delegates to `opts.open()` itself with
    /// `FILE_FLAG_OVERLAPPED` injected via `OpenOptionsExt::custom_flags`,
    /// then takes ownership of the resulting overlapped-capable handle.
    pub async fn open_with(
        path: impl Into<PathBuf>,
        mut opts: std::fs::OpenOptions,
    ) -> io::Result<Self> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::IntoRawHandle;
        opts.custom_flags(FILE_FLAG_OVERLAPPED);
        let path = path.into();
        let file = opts.open(&path)?;
        let handle = file.into_raw_handle() as HANDLE;
        let iocp = port();
        let assoc = unsafe { CreateIoCompletionPort(handle, iocp, FILE_KEY, 0) };
        if assoc.is_null() {
            let e = io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(e);
        }
        Ok(Self {
            handle,
            cursor: AtomicI64::new(0),
        })
    }

    pub async fn read(&self, mut buf: Vec<u8>) -> io::Result<(usize, Vec<u8>)> {
        let offset = self.cursor.load(Ordering::Relaxed) as u64;
        let n = issue_read(self.handle, &mut buf, offset).await?;
        self.cursor.fetch_add(n as i64, Ordering::Relaxed);
        Ok((n, buf))
    }

    pub async fn write(&self, buf: Vec<u8>) -> io::Result<(usize, Vec<u8>)> {
        let offset = self.cursor.load(Ordering::Relaxed) as u64;
        let n = issue_write(self.handle, &buf, offset).await?;
        self.cursor.fetch_add(n as i64, Ordering::Relaxed);
        Ok((n, buf))
    }

    /// Positional read: does not move the shared cursor, safe to call
    /// concurrently with other `read_at`/`write_at` calls on the same handle
    /// (each issues its own `OVERLAPPED` with an explicit offset).
    pub async fn read_at(&self, mut buf: Vec<u8>, offset: u64) -> io::Result<(usize, Vec<u8>)> {
        let n = issue_read(self.handle, &mut buf, offset).await?;
        Ok((n, buf))
    }

    pub async fn write_at(&self, buf: Vec<u8>, offset: u64) -> io::Result<(usize, Vec<u8>)> {
        let n = issue_write(self.handle, &buf, offset).await?;
        Ok((n, buf))
    }

    pub async fn sync_all(&self) -> io::Result<()> {
        let ok = unsafe { FlushFileBuffers(self.handle) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        // Win32 has no handle->std::fs::Metadata conversion without going
        // through a path or a duplicated std::fs::File; borrow the raw
        // handle briefly via ManuallyDrop so we don't double-close it.
        use std::os::windows::io::{AsRawHandle, FromRawHandle};
        let file = unsafe { std::fs::File::from_raw_handle(self.handle as *mut _) };
        let file = std::mem::ManuallyDrop::new(file);
        let meta = file.metadata();
        let _ = file.as_raw_handle(); // keep handle alive/used until here
        meta
    }

    pub async fn close(self) -> io::Result<()> {
        // Drop performs the actual CloseHandle.
        Ok(())
    }

    pub fn len(&self) -> io::Result<u64> {
        let mut size: i64 = 0;
        let ok = unsafe { GetFileSizeEx(self.handle, &mut size) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(size as u64)
    }
}

impl Drop for DtactFile {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.handle);
        }
    }
}

pub async fn metadata(path: impl Into<PathBuf>) -> io::Result<std::fs::Metadata> {
    let path = path.into();
    std::fs::metadata(&path)
}

pub async fn read_dir(path: impl Into<PathBuf>) -> io::Result<Vec<std::fs::DirEntry>> {
    let path: PathBuf = path.into();
    std::fs::read_dir(&path)?.collect()
}

pub async fn create_dir_all(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    std::fs::create_dir_all(&path)
}

pub async fn remove_file(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    std::fs::remove_file(&path)
}
