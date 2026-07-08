use super::*;
use std::net::SocketAddr;
use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicUsize, Ordering, fence};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

// Latency-breakdown tracing (DTACT_IO_TRACE=1) — shared with the Unix
// backend, see `crate::io::trace`'s module doc.
use crate::io::trace::{io_trace, trace_now_us};

// Lock-free free-list/ring-buffer primitives — shared with every other
// native backend in this crate via `crate::lockfree` (this module used
// to `include!("windows_primitives.rs")`, a byte-for-byte duplicate of
// the Unix backend's own private copies of these same two types).
use crate::lockfree::{SpscQueue, TreiberStack};

use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Networking::WinSock::*;
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};

// =========================================================================
// Windows-specific lock-free primitives that aren't shared with Unix
// (different field shapes: `WakerSlot` here tracks the *socket* an op
// was issued against as a `usize`, vs. the Unix backend's `AtomicU32`
// fd — not worth forcing into one generic type for this pass).
// =========================================================================
// WAKER SLOTS
// =========================================================================
#[repr(align(64))]
struct WakerSlot {
    waker_data: AtomicPtr<()>,
    waker_vtable: AtomicPtr<RawWakerVTable>,
    waker_lock: AtomicBool,
    result: AtomicI32,
    completed: AtomicBool,
    dropped: AtomicBool,
    /// The SOCKET (as usize) this op was issued against, so
    /// `cancel_queue` draining can find/clean up the right side of the
    /// op without the dropping thread touching IOCP-associated state
    /// directly.
    origin_socket: AtomicUsize,
}

impl WakerSlot {
    #[inline(always)]
    fn lock_waker(&self) {
        while self
            .waker_lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
    }

    #[inline(always)]
    fn unlock_waker(&self) {
        self.waker_lock.store(false, Ordering::Release);
    }
}

#[repr(align(64))]
struct WaitSlot {
    waker_data: AtomicPtr<()>,
    waker_vtable: AtomicPtr<RawWakerVTable>,
}

#[inline(always)]
fn wake_next_waiting_fiber(state: &WorkerState) {
    if let Some(wait_idx) = state.waiting_queue.pop() {
        let wait_slot = &state.wait_slots[wait_idx as usize];
        let data = wait_slot
            .waker_data
            .swap(std::ptr::null_mut(), Ordering::Relaxed);
        let vtable = wait_slot
            .waker_vtable
            .swap(std::ptr::null_mut(), Ordering::Relaxed);
        state.free_wait_slots.push(wait_idx);

        if !data.is_null() && !vtable.is_null() {
            let raw = RawWaker::new(data as *const (), unsafe { &*vtable });
            let w = unsafe { Waker::from_raw(raw) };
            w.wake();
        }
    }
}

// =========================================================================
// THREAD-LOCAL WORKER ASSIGNMENT
// =========================================================================
thread_local! {
    static THREAD_ID: usize = {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    };
}

fn get_local_thread_id() -> usize {
    THREAD_ID.with(|id| *id)
}

struct GlobalConfig {
    workers: usize,
}

static GLOBAL_CONFIG: OnceLock<GlobalConfig> = OnceLock::new();

// =========================================================================
// IOCP-specific types
// =========================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpCode {
    Read,
    Write,
    Accept,
    Connect,
}

/// Completion key used to distinguish a real op completion from a
/// `PostQueuedCompletionStatus` ping used purely to wake
/// `GetQueuedCompletionStatusEx` out of an infinite wait (analogous to
/// the Unix backend's eventfd).
const WAKE_KEY: usize = usize::MAX;

/// Per-op state, heap-allocated (`Box::into_raw`) for the lifetime of the
/// async op and reclaimed (`Box::from_raw`) by the worker when the
/// completion is dequeued. `overlapped` MUST be the first field: Windows
/// hands back a raw `*mut OVERLAPPED` on completion, which we cast back
/// to `*mut IoOverlapped` to recover the rest.
#[repr(C)]
struct IoOverlapped {
    overlapped: OVERLAPPED,
    slot_idx: usize,
    /// The socket the op was actually issued on (listening socket for
    /// Accept, the stream's own socket otherwise) — needed to call
    /// `WSAGetOverlappedResult` to decode the real result/error.
    issuing_socket: usize,
    /// Accept only: the pre-created socket AcceptEx will fill in. On
    /// success this is what gets reported as the op's result (the new
    /// connection), mirroring the Unix backend returning a new fd.
    accept_socket: usize,
    /// Scratch output buffer for AcceptEx's local+remote address pair —
    /// must outlive the op, hence living inside this heap allocation
    /// rather than on any stack. `2 * (sizeof(SOCKADDR_STORAGE) + 16)`.
    accept_addr_buf: [u8; 288],
}

enum IoRequest {
    Read {
        socket: usize,
        buf_ptr: *mut u8,
        len: usize,
        slot_idx: usize,
    },
    Write {
        socket: usize,
        buf_ptr: *const u8,
        len: usize,
        slot_idx: usize,
    },
    Accept {
        listen_socket: usize,
        accept_socket: usize,
        slot_idx: usize,
    },
    Connect {
        socket: usize,
        addr: SOCKADDR_STORAGE,
        addr_len: i32,
        slot_idx: usize,
    },
    /// See `Drop for DtactIoFuture` — cancellation is handed off to the
    /// owning worker thread rather than acted on directly, since the
    /// IOCP handle and any per-op state must only be touched by it.
    Cancel { slot_idx: usize },
}

unsafe impl Send for IoRequest {}

// =========================================================================
// Winsock extension functions (AcceptEx / ConnectEx)
// =========================================================================
// These aren't ordinary exported symbols — they must be queried per
// socket via `WSAIoctl(SIO_GET_EXTENSION_FUNCTION_POINTER)`. The pointer
// is stable for all sockets of the same address family/provider, so a
// single successful query is cached process-wide.

type AcceptExFn = unsafe extern "system" fn(
    SOCKET,
    SOCKET,
    *mut core::ffi::c_void,
    u32,
    u32,
    u32,
    *mut u32,
    *mut OVERLAPPED,
) -> i32;

type ConnectExFn = unsafe extern "system" fn(
    SOCKET,
    *const SOCKADDR,
    i32,
    *mut core::ffi::c_void,
    u32,
    *mut u32,
    *mut OVERLAPPED,
) -> i32;

const SIO_GET_EXTENSION_FUNCTION_POINTER: u32 = 0xC800_0006;
// {b5367df1-cbac-11cf-95ca-00805f48a192}
const WSAID_ACCEPTEX: windows_sys::core::GUID = windows_sys::core::GUID {
    data1: 0xb5367df1,
    data2: 0xcbac,
    data3: 0x11cf,
    data4: [0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92],
};
// {25a207b9-ddf3-4660-8ee9-76e58c74063e}
const WSAID_CONNECTEX: windows_sys::core::GUID = windows_sys::core::GUID {
    data1: 0x25a207b9,
    data2: 0xddf3,
    data3: 0x4660,
    data4: [0x8e, 0xe9, 0x76, 0xe5, 0x8c, 0x74, 0x06, 0x3e],
};

fn get_extension_fn<F: Copy>(socket: SOCKET, guid: &windows_sys::core::GUID) -> std::io::Result<F> {
    assert_eq!(std::mem::size_of::<F>(), std::mem::size_of::<usize>());
    let mut fn_ptr: usize = 0;
    let mut bytes_returned: u32 = 0;
    let res = unsafe {
        WSAIoctl(
            socket,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            guid as *const _ as *const core::ffi::c_void,
            std::mem::size_of::<windows_sys::core::GUID>() as u32,
            &mut fn_ptr as *mut usize as *mut core::ffi::c_void,
            std::mem::size_of::<usize>() as u32,
            &mut bytes_returned,
            std::ptr::null_mut(),
            None,
        )
    };
    if res != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::mem::transmute_copy::<usize, F>(&fn_ptr) })
}

fn get_accept_ex(socket: SOCKET) -> std::io::Result<AcceptExFn> {
    get_extension_fn(socket, &WSAID_ACCEPTEX)
}

fn get_connect_ex(socket: SOCKET) -> std::io::Result<ConnectExFn> {
    get_extension_fn(socket, &WSAID_CONNECTEX)
}

// =========================================================================
// WORKER STATE & RUNTIME INITIALISATION
// =========================================================================
struct WorkerState {
    iocp: HANDLE,
    queues: Box<[SpscQueue<IoRequest>]>,
    slots: Box<[WakerSlot]>,
    free_slots: TreiberStack,
    wait_slots: Box<[WaitSlot]>,
    free_wait_slots: TreiberStack,
    waiting_queue: TreiberStack,
    is_sleeping: AtomicBool,
    cancel_queue: TreiberStack,
}

unsafe impl Send for WorkerState {}
unsafe impl Sync for WorkerState {}

static WORKERS: OnceLock<Box<[WorkerState]>> = OnceLock::new();
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub fn init_runtime(
    workers: usize,
    _buffer_pool_size: usize,
    _chunk_size: usize,
    _pin_cpus: &[usize],
    ring_depth: u32,
) {
    let config = GlobalConfig { workers };
    if GLOBAL_CONFIG.set(config).is_err() {
        return;
    }

    let mut worker_states = Vec::with_capacity(workers);
    for _ in 0..workers {
        let mut queues = Vec::with_capacity(512);
        for _ in 0..512 {
            queues.push(SpscQueue::new(256));
        }
        let queues = queues.into_boxed_slice();

        let mut slots = Vec::with_capacity(ring_depth as usize);
        for _ in 0..ring_depth {
            slots.push(WakerSlot {
                waker_data: AtomicPtr::new(std::ptr::null_mut()),
                waker_vtable: AtomicPtr::new(std::ptr::null_mut()),
                waker_lock: AtomicBool::new(false),
                result: AtomicI32::new(0),
                completed: AtomicBool::new(false),
                dropped: AtomicBool::new(false),
                origin_socket: AtomicUsize::new(usize::MAX),
            });
        }
        let slots = slots.into_boxed_slice();
        let free_slots = TreiberStack::new(ring_depth as usize);
        for i in 0..ring_depth {
            free_slots.push(i);
        }
        let cancel_queue = TreiberStack::new(ring_depth as usize);

        let wait_slots_depth = 65536;
        let mut wait_slots = Vec::with_capacity(wait_slots_depth);
        for _ in 0..wait_slots_depth {
            wait_slots.push(WaitSlot {
                waker_data: AtomicPtr::new(std::ptr::null_mut()),
                waker_vtable: AtomicPtr::new(std::ptr::null_mut()),
            });
        }
        let wait_slots = wait_slots.into_boxed_slice();
        let free_wait_slots = TreiberStack::new(wait_slots_depth);
        for i in 0..wait_slots_depth {
            free_wait_slots.push(i as u32);
        }
        let waiting_queue = TreiberStack::new(wait_slots_depth);

        let iocp =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        if iocp.is_null() {
            panic!("Failed to create IOCP handle");
        }

        worker_states.push(WorkerState {
            iocp,
            queues,
            slots,
            free_slots,
            wait_slots,
            free_wait_slots,
            waiting_queue,
            is_sleeping: AtomicBool::new(false),
            cancel_queue,
        });
    }

    let worker_states = worker_states.into_boxed_slice();
    let _ = WORKERS.set(worker_states);

    for worker_idx in 0..workers {
        std::thread::Builder::new()
            .name(format!("dtact-io-worker-{worker_idx}"))
            .spawn(move || {
                let state = &WORKERS.get().unwrap()[worker_idx];
                run_windows_worker_loop(state);
            })
            .expect("Failed to spawn dtact-io worker thread");
    }
}

/// Shorthand initialiser matching the Unix backend's `init(workers)`.
pub fn init(workers: usize) {
    init_runtime(workers, 0, 0, &[], 1024);
}

pub fn shutdown_runtime() {
    SHUTDOWN.store(true, Ordering::Release);
    if let Some(workers) = WORKERS.get() {
        for state in workers.iter() {
            unsafe {
                PostQueuedCompletionStatus(state.iocp, 0, WAKE_KEY, std::ptr::null_mut());
            }
        }
    }
}

// =========================================================================
// IOCP WORKER LOOP
// =========================================================================
fn run_windows_worker_loop(state: &WorkerState) {
    let iocp = state.iocp;
    let mut entries: [OVERLAPPED_ENTRY; 64] = unsafe { std::mem::zeroed() };

    loop {
        if SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }

        let mut pushed = false;
        for q in state.queues.iter() {
            while let Some(req) = q.pop() {
                pushed = true;
                submit_windows_request(state, req);
            }
        }

        while let Some(slot_idx) = state.cancel_queue.pop() {
            pushed = true;
            cancel_windows_slot(state, slot_idx as usize);
        }

        let any_pending = state.queues.iter().any(|q| !q.is_empty());
        let timeout_ms = if pushed || any_pending { 0 } else { u32::MAX };

        state.is_sleeping.store(true, Ordering::SeqCst);
        // Dekker-style re-check (mirrors the Linux io_uring worker loop's
        // fix for the same class of bug): `any_pending` above was read
        // before we published `is_sleeping`. A producer that pushed to a
        // queue and observed `is_sleeping == false` right before our
        // store (a StoreLoad reorder is legal even on x86) would skip
        // its `PostQueuedCompletionStatus` wakeup, leaving us to block
        // on `GetQueuedCompletionStatusEx` with `timeout_ms = INFINITE`
        // forever with a request nobody drained. Re-scan now that
        // `is_sleeping` is published and fall back to a non-blocking
        // poll if anything landed.
        fence(Ordering::SeqCst);
        let missed = state.queues.iter().any(|q| !q.is_empty());
        let effective_timeout = if missed { 0 } else { timeout_ms };
        let mut removed: u32 = 0;
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                iocp,
                entries.as_mut_ptr(),
                entries.len() as u32,
                &mut removed,
                effective_timeout,
                0,
            )
        };
        state.is_sleeping.store(false, Ordering::Release);

        if ok == 0 {
            continue;
        }

        for entry in entries.iter().take(removed as usize) {
            if entry.lpCompletionKey as usize == WAKE_KEY {
                continue;
            }
            let ov_ptr = entry.lpOverlapped as *mut IoOverlapped;
            if ov_ptr.is_null() {
                continue;
            }
            process_windows_completion(state, ov_ptr);
        }
    }
}

fn submit_windows_request(state: &WorkerState, req: IoRequest) {
    match req {
        IoRequest::Cancel { slot_idx } => {
            // The op was cancelled before or racing with completion —
            // nothing to do on the IOCP side (Windows has no portable
            // cross-provider CancelIoEx equivalent to io_uring's
            // AsyncCancel that's worth the complexity here); the slot
            // just gets recycled once its (already in-flight) completion
            // arrives, same as the Unix `dropped` flag handles it.
            let _ = slot_idx;
        }
        IoRequest::Read {
            socket,
            buf_ptr,
            len,
            slot_idx,
        } => {
            state.slots[slot_idx]
                .origin_socket
                .store(socket, Ordering::Relaxed);
            let ov = Box::new(IoOverlapped {
                overlapped: unsafe { std::mem::zeroed() },
                slot_idx,
                issuing_socket: socket,
                accept_socket: 0,
                accept_addr_buf: [0u8; 288],
            });
            let ov_ptr = Box::into_raw(ov);
            let mut wsabuf = WSABUF {
                len: len as u32,
                buf: buf_ptr,
            };
            let mut flags: u32 = 0;
            let res = unsafe {
                WSARecv(
                    socket,
                    &mut wsabuf,
                    1,
                    std::ptr::null_mut(),
                    &mut flags,
                    ov_ptr as *mut OVERLAPPED,
                    None,
                )
            };
            handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
        }
        IoRequest::Write {
            socket,
            buf_ptr,
            len,
            slot_idx,
        } => {
            state.slots[slot_idx]
                .origin_socket
                .store(socket, Ordering::Relaxed);
            let ov = Box::new(IoOverlapped {
                overlapped: unsafe { std::mem::zeroed() },
                slot_idx,
                issuing_socket: socket,
                accept_socket: 0,
                accept_addr_buf: [0u8; 288],
            });
            let ov_ptr = Box::into_raw(ov);
            let wsabuf = WSABUF {
                len: len as u32,
                buf: buf_ptr as *mut u8,
            };
            let res = unsafe {
                WSASend(
                    socket,
                    &wsabuf,
                    1,
                    std::ptr::null_mut(),
                    0,
                    ov_ptr as *mut OVERLAPPED,
                    None,
                )
            };
            handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
        }
        IoRequest::Accept {
            listen_socket,
            accept_socket,
            slot_idx,
        } => {
            state.slots[slot_idx]
                .origin_socket
                .store(listen_socket, Ordering::Relaxed);
            let accept_fn = match get_accept_ex(listen_socket) {
                Ok(f) => f,
                Err(e) => {
                    complete_with_error(state, slot_idx, e);
                    return;
                }
            };
            let mut ov = Box::new(IoOverlapped {
                overlapped: unsafe { std::mem::zeroed() },
                slot_idx,
                issuing_socket: listen_socket,
                accept_socket,
                accept_addr_buf: [0u8; 288],
            });
            let buf_ptr = ov.accept_addr_buf.as_mut_ptr();
            let ov_ptr = Box::into_raw(ov);
            let mut bytes_received: u32 = 0;
            let res = unsafe {
                accept_fn(
                    listen_socket,
                    accept_socket,
                    buf_ptr as *mut core::ffi::c_void,
                    0,
                    144,
                    144,
                    &mut bytes_received,
                    ov_ptr as *mut OVERLAPPED,
                )
            };
            // AcceptEx returns BOOL directly (TRUE = immediate success),
            // not the WSA "0 or SOCKET_ERROR" convention WSARecv/WSASend use.
            let res = if res != 0 { 0 } else { -1 };
            handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
        }
        IoRequest::Connect {
            socket,
            addr,
            addr_len,
            slot_idx,
        } => {
            state.slots[slot_idx]
                .origin_socket
                .store(socket, Ordering::Relaxed);
            let connect_fn = match get_connect_ex(socket) {
                Ok(f) => f,
                Err(e) => {
                    complete_with_error(state, slot_idx, e);
                    return;
                }
            };
            let ov = Box::new(IoOverlapped {
                overlapped: unsafe { std::mem::zeroed() },
                slot_idx,
                issuing_socket: socket,
                accept_socket: 0,
                accept_addr_buf: [0u8; 288],
            });
            let ov_ptr = Box::into_raw(ov);
            let mut bytes_sent: u32 = 0;
            let res = unsafe {
                connect_fn(
                    socket,
                    &addr as *const SOCKADDR_STORAGE as *const SOCKADDR,
                    addr_len,
                    std::ptr::null_mut(),
                    0,
                    &mut bytes_sent,
                    ov_ptr as *mut OVERLAPPED,
                )
            };
            let res = if res != 0 { 0 } else { -1 };
            handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
        }
    }
}

/// WSARecv/WSASend/AcceptEx/ConnectEx can *return* success immediately —
/// but on a socket associated with an IOCP, a completion packet is
/// queued for it regardless (Windows only skips that if
/// `SetFileCompletionNotificationModes(..., FILE_SKIP_COMPLETION_PORT_ON_SUCCESS)`
/// was called, which we don't do). So an immediate success must be left
/// alone here — processing it now *and* again when
/// `GetQueuedCompletionStatusEx` reports the same completion later would
/// double-free `ov_ptr`. Only a genuine synchronous *failure* (anything
/// other than `WSA_IO_PENDING`) means no completion will ever be queued,
/// and must be handled right here instead.
fn handle_immediate_or_pending(
    state: &WorkerState,
    slot_idx: usize,
    ov_ptr: *mut IoOverlapped,
    res: i32,
) {
    if res == 0 {
        // Will still complete via IOCP — nothing to do now.
        return;
    }
    let err = unsafe { WSAGetLastError() };
    if err == WSA_IO_PENDING {
        // Genuinely async — the IOCP will deliver a completion later.
        return;
    }
    // Synchronous failure — no completion packet will ever arrive for it.
    unsafe {
        drop(Box::from_raw(ov_ptr));
    }
    complete_with_error(state, slot_idx, std::io::Error::from_raw_os_error(err));
}

fn complete_with_error(state: &WorkerState, slot_idx: usize, err: std::io::Error) {
    let res = -err.raw_os_error().unwrap_or(WSAEINVAL);
    finish_slot(state, slot_idx, res);
}

fn process_windows_completion(state: &WorkerState, ov_ptr: *mut IoOverlapped) {
    let ov = unsafe { Box::from_raw(ov_ptr) };
    let slot_idx = ov.slot_idx;
    let socket = ov.issuing_socket as SOCKET;

    let mut transferred: u32 = 0;
    let mut flags: u32 = 0;
    let ok = unsafe {
        WSAGetOverlappedResult(
            socket,
            &ov.overlapped as *const OVERLAPPED as *mut OVERLAPPED,
            &mut transferred,
            0,
            &mut flags,
        )
    };

    let res: i32 = if ok == 0 {
        let err = unsafe { WSAGetLastError() };
        -err
    } else if ov.accept_socket != 0 {
        // Accept: report the new connection's socket handle, not a byte count.
        ov.accept_socket as i32
    } else {
        transferred as i32
    };

    finish_slot(state, slot_idx, res);
}

fn finish_slot(state: &WorkerState, slot_idx: usize, res: i32) {
    let slot = &state.slots[slot_idx];

    io_trace!(
        "[dtact-io] t={} slot={} res={} B_kernel_complete",
        trace_now_us(),
        slot_idx,
        res
    );

    slot.result.store(res, Ordering::Release);

    // See the matching comment in the Unix backend's
    // `process_linux_completion`: extract the waker before publishing
    // `completed`, so a slot reused immediately after another thread
    // observes `completed` can never have its freshly installed waker
    // clobbered by this call.
    slot.lock_waker();
    let data = slot
        .waker_data
        .swap(std::ptr::null_mut(), Ordering::Relaxed);
    let vtable = slot
        .waker_vtable
        .swap(std::ptr::null_mut(), Ordering::Relaxed);
    slot.unlock_waker();

    slot.completed.store(true, Ordering::Release);

    if slot.dropped.load(Ordering::Acquire) {
        state.free_slots.push(slot_idx as u32);
        wake_next_waiting_fiber(state);
    } else if !data.is_null() && !vtable.is_null() {
        let raw = RawWaker::new(data as *const (), unsafe { &*vtable });
        let w = unsafe { Waker::from_raw(raw) };
        w.wake();
    }
}

fn cancel_windows_slot(state: &WorkerState, slot_idx: usize) {
    // The corresponding op is still genuinely in flight with the OS (we
    // have no cheap portable way to cancel it early); just let
    // `process_windows_completion`/`finish_slot`'s `dropped` check free
    // the slot once it actually completes. This mirrors how the slot
    // was already marked in `Drop for DtactIoFuture` before this was
    // queued — nothing further to do here except make sure the slot
    // isn't reachable from anywhere else in the meantime, which it
    // isn't (only `state.slots[slot_idx]` refers to it by index).
    let _ = (state, slot_idx);
}

// =========================================================================
// DtactIoFuture
// =========================================================================
pub struct DtactIoFuture {
    pub worker_idx: usize,
    pub fd: u32,
    pub direct_fd_idx: u32,
    pub op: OpCode,
    pub buf_ptr: *mut u8,
    pub len: usize,
    pub offset: i64,
    pub addr: Option<SOCKADDR_STORAGE>,
    pub addr_len: i32,
    pub slot_idx: Option<usize>,
    /// Accept only: a pre-created socket for AcceptEx to fill in, created
    /// lazily on first poll so `new()` stays a plain constructor.
    accept_socket: std::cell::Cell<usize>,
}

unsafe impl Send for DtactIoFuture {}
unsafe impl Sync for DtactIoFuture {}

impl DtactIoFuture {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_idx: usize,
        fd: u32,
        direct_fd_idx: u32,
        op: OpCode,
        buf_ptr: *mut u8,
        len: usize,
        offset: i64,
        addr: Option<SOCKADDR_STORAGE>,
        addr_len: i32,
        slot_idx: Option<usize>,
    ) -> Self {
        Self {
            worker_idx,
            fd,
            direct_fd_idx,
            op,
            buf_ptr,
            len,
            offset,
            addr,
            addr_len,
            slot_idx,
            accept_socket: std::cell::Cell::new(0),
        }
    }

    fn create_io_request(&self, slot_idx: usize) -> IoRequest {
        match self.op {
            OpCode::Read => IoRequest::Read {
                socket: self.fd as usize,
                buf_ptr: self.buf_ptr,
                len: self.len,
                slot_idx,
            },
            OpCode::Write => IoRequest::Write {
                socket: self.fd as usize,
                buf_ptr: self.buf_ptr,
                len: self.len,
                slot_idx,
            },
            OpCode::Accept => {
                if self.accept_socket.get() == 0 {
                    let s = unsafe {
                        WSASocketW(
                            AF_INET as i32,
                            SOCK_STREAM as i32,
                            0,
                            std::ptr::null(),
                            0,
                            WSA_FLAG_OVERLAPPED,
                        )
                    };
                    self.accept_socket.set(s as usize);
                }
                IoRequest::Accept {
                    listen_socket: self.fd as usize,
                    accept_socket: self.accept_socket.get(),
                    slot_idx,
                }
            }
            OpCode::Connect => IoRequest::Connect {
                socket: self.fd as usize,
                addr: self.addr.unwrap(),
                addr_len: self.addr_len,
                slot_idx,
            },
        }
    }
}

impl Future for DtactIoFuture {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot_idx = match self.slot_idx {
            Some(idx) => idx,
            None => {
                let state = &WORKERS.get().unwrap()[self.worker_idx];
                let idx = match state.free_slots.pop() {
                    Some(i) => i as usize,
                    None => {
                        if let Some(wait_idx) = state.free_wait_slots.pop() {
                            let wait_slot = &state.wait_slots[wait_idx as usize];
                            wait_slot
                                .waker_data
                                .store(cx.waker().data() as *mut (), Ordering::Relaxed);
                            wait_slot.waker_vtable.store(
                                cx.waker().vtable() as *const RawWakerVTable as *mut _,
                                Ordering::Relaxed,
                            );
                            state.waiting_queue.push(wait_idx);

                            if let Some(i) = state.free_slots.pop() {
                                wait_slot
                                    .waker_data
                                    .store(std::ptr::null_mut(), Ordering::Relaxed);
                                wait_slot
                                    .waker_vtable
                                    .store(std::ptr::null_mut(), Ordering::Relaxed);
                                i as usize
                            } else {
                                return Poll::Pending;
                            }
                        } else {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                };

                let slot = &state.slots[idx];
                slot.completed.store(false, Ordering::Relaxed);
                slot.dropped.store(false, Ordering::Relaxed);
                slot.lock_waker();
                slot.waker_data
                    .store(cx.waker().data() as *mut (), Ordering::Relaxed);
                slot.waker_vtable.store(
                    cx.waker().vtable() as *const RawWakerVTable as *mut _,
                    Ordering::Relaxed,
                );
                slot.unlock_waker();

                let req = self.create_io_request(idx);
                let q_idx = get_local_thread_id() % state.queues.len();
                let queue = &state.queues[q_idx];

                io_trace!(
                    "[dtact-io] t={} slot={} fd={} op={:?} A_submit",
                    trace_now_us(),
                    idx,
                    self.fd,
                    self.op
                );

                if queue.push(req).is_err() {
                    slot.lock_waker();
                    slot.waker_data
                        .store(std::ptr::null_mut(), Ordering::Relaxed);
                    slot.waker_vtable
                        .store(std::ptr::null_mut(), Ordering::Relaxed);
                    slot.unlock_waker();
                    state.free_slots.push(idx as u32);
                    wake_next_waiting_fiber(state);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                // Paired with the Dekker-style re-check in
                // `run_windows_worker_loop` — must be SeqCst with a fence
                // after the queue push above so this load can't be
                // reordered ahead of it, or we could miss waking a
                // worker that's about to block on
                // `GetQueuedCompletionStatusEx` with an infinite timeout.
                fence(Ordering::SeqCst);
                if state.is_sleeping.load(Ordering::SeqCst) {
                    unsafe {
                        PostQueuedCompletionStatus(state.iocp, 0, WAKE_KEY, std::ptr::null_mut());
                    }
                }

                self.slot_idx = Some(idx);
                idx
            }
        };

        let state = &WORKERS.get().unwrap()[self.worker_idx];
        let slot = &state.slots[slot_idx];

        if slot.completed.load(Ordering::Acquire) {
            let res = slot.result.load(Ordering::Acquire);
            io_trace!(
                "[dtact-io] t={} slot={} res={} C_fiber_resumed",
                trace_now_us(),
                slot_idx,
                res
            );
            slot.lock_waker();
            slot.waker_data
                .store(std::ptr::null_mut(), Ordering::Relaxed);
            slot.waker_vtable
                .store(std::ptr::null_mut(), Ordering::Relaxed);
            slot.unlock_waker();
            state.free_slots.push(slot_idx as u32);
            self.slot_idx = None;

            wake_next_waiting_fiber(state);

            if res < 0 {
                Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
            } else {
                Poll::Ready(Ok(res as usize))
            }
        } else {
            let new_data = cx.waker().data() as *mut ();
            let new_vtable = cx.waker().vtable() as *const RawWakerVTable as *mut _;

            slot.lock_waker();
            let old_data = slot.waker_data.load(Ordering::Relaxed);
            let old_vtable = slot.waker_vtable.load(Ordering::Relaxed);
            if old_data != new_data || old_vtable != new_vtable {
                slot.waker_data.store(new_data, Ordering::Relaxed);
                slot.waker_vtable.store(new_vtable, Ordering::Relaxed);
            }
            slot.unlock_waker();
            Poll::Pending
        }
    }
}

impl Drop for DtactIoFuture {
    fn drop(&mut self) {
        let Some(idx) = self.slot_idx else { return };
        let Some(state) = WORKERS.get().and_then(|w| w.get(self.worker_idx)) else {
            return;
        };

        let slot = &state.slots[idx];
        slot.lock_waker();
        slot.waker_data
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        slot.waker_vtable
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        slot.unlock_waker();

        if slot.completed.load(Ordering::Acquire) {
            state.free_slots.push(idx as u32);
            wake_next_waiting_fiber(state);
            return;
        }

        slot.dropped.store(true, Ordering::Release);
        let q_idx = get_local_thread_id() % state.queues.len();
        let _ = state.queues[q_idx].push(IoRequest::Cancel { slot_idx: idx });
        state.cancel_queue.push(idx as u32);

        fence(Ordering::SeqCst);
        if state.is_sleeping.load(Ordering::SeqCst) {
            unsafe {
                PostQueuedCompletionStatus(state.iocp, 0, WAKE_KEY, std::ptr::null_mut());
            }
        }
    }
}

// =========================================================================
// HIGH-LEVEL API: DtactTcpStream / DtactTcpListener
// =========================================================================
pub struct DtactTcpStream {
    inner: std::net::TcpStream,
    worker_idx: usize,
}

fn pick_worker(socket: usize) -> usize {
    let num_workers = GLOBAL_CONFIG.get().map_or(1, |c| c.workers);
    socket % num_workers
}

impl DtactTcpStream {
    pub fn from_std(stream: std::net::TcpStream) -> std::io::Result<Self> {
        stream.set_nonblocking(true)?;
        // See the equivalent comment on the Unix backend's `from_std` —
        // Nagle + delayed ACK stalls small request/response traffic.
        stream.set_nodelay(true)?;
        let socket = stream.as_raw_socket() as usize;
        let worker_idx = pick_worker(socket);
        let state = &WORKERS.get().unwrap()[worker_idx];
        let res = unsafe { CreateIoCompletionPort(socket as HANDLE, state.iocp, socket, 0) };
        if res.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            inner: stream,
            worker_idx,
        })
    }

    pub async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        DtactIoFuture::new(
            self.worker_idx,
            self.inner.as_raw_socket() as u32,
            u32::MAX,
            OpCode::Read,
            buf.as_mut_ptr(),
            buf.len(),
            0,
            None,
            0,
            None,
        )
        .await
    }

    pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        DtactIoFuture::new(
            self.worker_idx,
            self.inner.as_raw_socket() as u32,
            u32::MAX,
            OpCode::Write,
            buf.as_ptr() as *mut u8,
            buf.len(),
            0,
            None,
            0,
            None,
        )
        .await
    }

    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        let raw_socket = unsafe {
            WSASocketW(
                domain as i32,
                SOCK_STREAM as i32,
                0,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED,
            )
        };
        if raw_socket == INVALID_SOCKET {
            return Err(std::io::Error::last_os_error());
        }

        // ConnectEx requires the socket to already be bound.
        let any_addr = match addr {
            SocketAddr::V4(_) => socket_addr_to_win(&"0.0.0.0:0".parse().unwrap()),
            SocketAddr::V6(_) => socket_addr_to_win(&"[::]:0".parse().unwrap()),
        };
        let (bind_addr, bind_len) = any_addr;
        let bind_res = unsafe {
            bind(
                raw_socket,
                &bind_addr as *const SOCKADDR_STORAGE as *const SOCKADDR,
                bind_len,
            )
        };
        if bind_res != 0 {
            let e = std::io::Error::last_os_error();
            unsafe { closesocket(raw_socket) };
            return Err(e);
        }

        let stream = unsafe { std::net::TcpStream::from_raw_socket(raw_socket as RawSocket) };
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        let worker_idx = pick_worker(raw_socket as usize);
        let state = &WORKERS.get().unwrap()[worker_idx];
        let assoc = unsafe {
            CreateIoCompletionPort(raw_socket as HANDLE, state.iocp, raw_socket as usize, 0)
        };
        if assoc.is_null() {
            return Err(std::io::Error::last_os_error());
        }

        let (win_addr, win_len) = socket_addr_to_win(&addr);

        let res = DtactIoFuture::new(
            worker_idx,
            raw_socket as u32,
            u32::MAX,
            OpCode::Connect,
            std::ptr::null_mut(),
            0,
            0,
            Some(win_addr),
            win_len,
            None,
        )
        .await;

        match res {
            Ok(_) => {
                // Required after ConnectEx before the socket behaves like
                // a normal connected socket (getpeername, etc.).
                unsafe {
                    setsockopt(
                        raw_socket,
                        SOL_SOCKET,
                        SO_UPDATE_CONNECT_CONTEXT,
                        std::ptr::null(),
                        0,
                    );
                }
                Ok(Self {
                    inner: stream,
                    worker_idx,
                })
            }
            Err(e) => Err(e),
        }
    }
}

pub struct DtactTcpListener {
    inner: std::net::TcpListener,
    worker_idx: usize,
}

impl DtactTcpListener {
    pub fn from_std(listener: std::net::TcpListener) -> std::io::Result<Self> {
        listener.set_nonblocking(true)?;
        let socket = listener.as_raw_socket() as usize;
        let worker_idx = pick_worker(socket);
        let state = &WORKERS.get().unwrap()[worker_idx];
        let res = unsafe { CreateIoCompletionPort(socket as HANDLE, state.iocp, socket, 0) };
        if res.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            inner: listener,
            worker_idx,
        })
    }

    pub async fn accept(&self) -> std::io::Result<(DtactTcpStream, SocketAddr)> {
        let listen_socket = self.inner.as_raw_socket() as usize;
        let fut = DtactIoFuture::new(
            self.worker_idx,
            listen_socket as u32,
            u32::MAX,
            OpCode::Accept,
            std::ptr::null_mut(),
            0,
            0,
            None,
            0,
            None,
        );
        let res = fut.await?;
        let accept_socket = res as usize;

        // Required after AcceptEx before the socket behaves like a
        // normal accepted socket (getpeername, inherited listen options).
        unsafe {
            setsockopt(
                accept_socket,
                SOL_SOCKET,
                SO_UPDATE_ACCEPT_CONTEXT,
                &listen_socket as *const usize as *const u8,
                std::mem::size_of::<usize>() as i32,
            );
        }

        let stream = unsafe { std::net::TcpStream::from_raw_socket(accept_socket as RawSocket) };
        stream.set_nonblocking(true)?;
        let peer_addr = stream.peer_addr()?;
        let client = DtactTcpStream::from_std(stream)?;
        Ok((client, peer_addr))
    }
}

fn socket_addr_to_win(addr: &SocketAddr) -> (SOCKADDR_STORAGE, i32) {
    let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = SOCKADDR_IN {
                sin_family: AF_INET as u16,
                sin_port: a.port().to_be(),
                sin_addr: IN_ADDR {
                    S_un: IN_ADDR_0 {
                        S_addr: u32::from_ne_bytes(a.ip().octets()),
                    },
                },
                sin_zero: [0; 8],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &sin as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of_val(&sin),
                );
            }
            std::mem::size_of_val(&sin) as i32
        }
        SocketAddr::V6(a) => {
            let sin6 = SOCKADDR_IN6 {
                sin6_family: AF_INET6 as u16,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: IN6_ADDR {
                    u: IN6_ADDR_0 {
                        Byte: a.ip().octets(),
                    },
                },
                Anonymous: SOCKADDR_IN6_0 {
                    sin6_scope_id: a.scope_id(),
                },
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &sin6 as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of_val(&sin6),
                );
            }
            std::mem::size_of_val(&sin6) as i32
        }
    };
    (storage, len)
}
