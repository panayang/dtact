use super::{Future, Pin};
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
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, IN_ADDR, IN_ADDR_0, IN6_ADDR, IN6_ADDR_0, INVALID_SOCKET,
    SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCK_STREAM, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_IN6_0, SOCKADDR_STORAGE, SOCKET, SOL_SOCKET, WSA_FLAG_OVERLAPPED,
    WSA_IO_PENDING, WSABUF, WSAEINVAL, WSAGetLastError, WSAGetOverlappedResult, WSAIoctl, WSARecv,
    WSARecvFrom, WSASend, WSASendTo, WSASocketW, bind, closesocket, setsockopt,
};
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
            let raw = RawWaker::new(data.cast_const(), unsafe { &*vtable });
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

/// Which kind of async op an [`IoRequest`]/completion refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpCode {
    /// A socket read.
    Read,
    /// A socket write.
    Write,
    /// Accepting an incoming connection on a listener.
    Accept,
    /// Connecting to a remote address.
    Connect,
    /// Connectionless UDP send to an explicit peer (`WSASendTo`).
    SendTo,
    /// Connectionless UDP receive, recording the peer address (`WSARecvFrom`).
    RecvFrom,
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
    /// Accept only: the pre-created socket `AcceptEx` will fill in. On
    /// success this is what gets reported as the op's result (the new
    /// connection), mirroring the Unix backend returning a new fd.
    accept_socket: usize,
    /// Scratch output buffer for `AcceptEx`'s local+remote address pair —
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
    SendTo {
        socket: usize,
        buf_ptr: *const u8,
        len: usize,
        addr: SOCKADDR_STORAGE,
        addr_len: i32,
        slot_idx: usize,
    },
    RecvFrom {
        socket: usize,
        buf_ptr: *mut u8,
        len: usize,
        /// Caller-owned (see `DtactUdpSocket::recv_from`) `SOCKADDR_STORAGE`
        /// the OS fills with the sender's address; must outlive the op.
        from_ptr: *mut SOCKADDR_STORAGE,
        /// Caller-owned in/out length for `from_ptr`.
        from_len_ptr: *mut i32,
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
    data1: 0xb536_7df1,
    data2: 0xcbac,
    data3: 0x11cf,
    data4: [0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92],
};
// {25a207b9-ddf3-4660-8ee9-76e58c74063e}
const WSAID_CONNECTEX: windows_sys::core::GUID = windows_sys::core::GUID {
    data1: 0x25a2_07b9,
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
            std::ptr::from_ref(guid).cast::<core::ffi::c_void>(),
            std::mem::size_of::<windows_sys::core::GUID>() as u32,
            (&raw mut fn_ptr).cast::<core::ffi::c_void>(),
            std::mem::size_of::<usize>() as u32,
            &raw mut bytes_returned,
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

/// Start the IOCP-backed native io reactor.
///
/// `workers` io-worker threads, each with a `ring_depth`-deep in-flight-op
/// slot table. `buffer_pool_size`/`chunk_size`/`pin_cpus` are accepted
/// only for signature parity with the Unix backend and are currently
/// unused. Idempotent: only the first call takes effect.
///
/// # Panics
///
/// Panics if `CreateIoCompletionPort` fails to create the per-worker IOCP
/// handle, or if the OS refuses to spawn a worker thread — both are
/// treated as fatal startup failures.
pub fn init_runtime(
    workers: usize,
    ring_depth: u32,
    _buffer_pool_size: usize,
    _chunk_size: usize,
    _pin_cpus: &[usize],
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
        assert!(!iocp.is_null(), "Failed to create IOCP handle");

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
    init_runtime(workers, 1024, 0, 0, &[]);
}

/// Signal every io-worker thread to stop via a wake packet. Does not join
/// the worker threads.
pub fn shutdown_runtime() {
    SHUTDOWN.store(true, Ordering::Release);
    if let Some(workers) = WORKERS.get() {
        for state in workers {
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
        for q in &state.queues {
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
                &raw mut removed,
                effective_timeout,
                0,
            )
        };
        state.is_sleeping.store(false, Ordering::Release);

        if ok == 0 {
            continue;
        }

        for entry in entries.iter().take(removed as usize) {
            if entry.lpCompletionKey == WAKE_KEY {
                continue;
            }
            let ov_ptr = entry.lpOverlapped.cast::<IoOverlapped>();
            if ov_ptr.is_null() {
                continue;
            }
            process_windows_completion(state, ov_ptr);
        }
    }
}

// `req` is taken by value (not `&IoRequest`) deliberately: it was just
// popped by value off the per-worker `SpscQueue<IoRequest>` (see the
// caller), and every field clippy would suggest borrowing instead is a
// `Copy` primitive (pointer/usize) matched out of the enum anyway, so a
// reference would only add lifetime noise with no allocation saved.
fn submit_read(state: &WorkerState, socket: usize, buf_ptr: *mut u8, len: usize, slot_idx: usize) {
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
        buf: buf_ptr,
    };
    let mut flags: u32 = 0;
    let res = unsafe {
        WSARecv(
            socket,
            &raw const wsabuf,
            1,
            std::ptr::null_mut(),
            &raw mut flags,
            ov_ptr.cast::<OVERLAPPED>(),
            None,
        )
    };
    handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
}

fn submit_write(
    state: &WorkerState,
    socket: usize,
    buf_ptr: *const u8,
    len: usize,
    slot_idx: usize,
) {
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
        buf: buf_ptr.cast_mut(),
    };
    let res = unsafe {
        WSASend(
            socket,
            &raw const wsabuf,
            1,
            std::ptr::null_mut(),
            0,
            ov_ptr.cast::<OVERLAPPED>(),
            None,
        )
    };
    handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
}

fn submit_accept(state: &WorkerState, listen_socket: usize, accept_socket: usize, slot_idx: usize) {
    state.slots[slot_idx]
        .origin_socket
        .store(listen_socket, Ordering::Relaxed);
    let accept_fn = match get_accept_ex(listen_socket) {
        Ok(f) => f,
        Err(e) => {
            complete_with_error(state, slot_idx, &e);
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
            buf_ptr.cast::<core::ffi::c_void>(),
            0,
            144,
            144,
            &raw mut bytes_received,
            ov_ptr.cast::<OVERLAPPED>(),
        )
    };
    // AcceptEx returns BOOL directly (TRUE = immediate success),
    // not the WSA "0 or SOCKET_ERROR" convention WSARecv/WSASend use.
    let res = if res != 0 { 0 } else { -1 };
    handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
}

fn submit_connect(
    state: &WorkerState,
    socket: usize,
    addr: SOCKADDR_STORAGE,
    addr_len: i32,
    slot_idx: usize,
) {
    state.slots[slot_idx]
        .origin_socket
        .store(socket, Ordering::Relaxed);
    let connect_fn = match get_connect_ex(socket) {
        Ok(f) => f,
        Err(e) => {
            complete_with_error(state, slot_idx, &e);
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
            (&raw const addr).cast::<SOCKADDR>(),
            addr_len,
            std::ptr::null_mut(),
            0,
            &raw mut bytes_sent,
            ov_ptr.cast::<OVERLAPPED>(),
        )
    };
    let res = if res != 0 { 0 } else { -1 };
    handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
}

// `req` is taken by value (not `&IoRequest`) deliberately: it was just
// popped by value off the per-worker `SpscQueue<IoRequest>` (see the
// caller), and every field clippy would suggest borrowing instead is a
// `Copy` primitive (pointer/usize) matched out of the enum anyway, so a
// reference would only add lifetime noise with no allocation saved.
#[allow(clippy::needless_pass_by_value)]
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
        } => submit_read(state, socket, buf_ptr, len, slot_idx),
        IoRequest::Write {
            socket,
            buf_ptr,
            len,
            slot_idx,
        } => submit_write(state, socket, buf_ptr, len, slot_idx),
        IoRequest::Accept {
            listen_socket,
            accept_socket,
            slot_idx,
        } => submit_accept(state, listen_socket, accept_socket, slot_idx),
        IoRequest::Connect {
            socket,
            addr,
            addr_len,
            slot_idx,
        } => submit_connect(state, socket, addr, addr_len, slot_idx),
        IoRequest::SendTo {
            socket,
            buf_ptr,
            len,
            addr,
            addr_len,
            slot_idx,
        } => submit_send_to(state, socket, buf_ptr, len, addr, addr_len, slot_idx),
        IoRequest::RecvFrom {
            socket,
            buf_ptr,
            len,
            from_ptr,
            from_len_ptr,
            slot_idx,
        } => submit_recv_from(
            state,
            socket,
            buf_ptr,
            len,
            from_ptr,
            from_len_ptr,
            slot_idx,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn submit_send_to(
    state: &WorkerState,
    socket: usize,
    buf_ptr: *const u8,
    len: usize,
    addr: SOCKADDR_STORAGE,
    addr_len: i32,
    slot_idx: usize,
) {
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
        buf: buf_ptr.cast_mut(),
    };
    let res = unsafe {
        WSASendTo(
            socket,
            &raw const wsabuf,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::addr_of!(addr).cast::<SOCKADDR>(),
            addr_len,
            ov_ptr.cast::<OVERLAPPED>(),
            None,
        )
    };
    handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
}

#[allow(clippy::too_many_arguments)]
fn submit_recv_from(
    state: &WorkerState,
    socket: usize,
    buf_ptr: *mut u8,
    len: usize,
    from_ptr: *mut SOCKADDR_STORAGE,
    from_len_ptr: *mut i32,
    slot_idx: usize,
) {
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
        buf: buf_ptr,
    };
    let mut flags: u32 = 0;
    let res = unsafe {
        WSARecvFrom(
            socket,
            &raw const wsabuf,
            1,
            std::ptr::null_mut(),
            &raw mut flags,
            from_ptr.cast::<SOCKADDR>(),
            from_len_ptr,
            ov_ptr.cast::<OVERLAPPED>(),
            None,
        )
    };
    handle_immediate_or_pending(state, slot_idx, ov_ptr, res);
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
    complete_with_error(state, slot_idx, &std::io::Error::from_raw_os_error(err));
}

fn complete_with_error(state: &WorkerState, slot_idx: usize, err: &std::io::Error) {
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
            (&raw const ov.overlapped).cast_mut(),
            &raw mut transferred,
            0,
            &raw mut flags,
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
        let raw = RawWaker::new(data.cast_const(), unsafe { &*vtable });
        let w = unsafe { Waker::from_raw(raw) };
        w.wake();
    }
}

const fn cancel_windows_slot(state: &WorkerState, slot_idx: usize) {
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
/// A single in-flight async socket op (read/write/accept/connect),
/// dispatched to the IOCP worker for `worker_idx` and polled to
/// completion.
pub struct DtactIoFuture {
    /// Index of the io-worker thread this op is (or will be) dispatched
    /// to.
    pub worker_idx: usize,
    /// The socket this op operates on, as a raw `usize` (cast from
    /// `SOCKET`).
    pub fd: u32,
    /// Unused on this backend (kept for signature parity with the Unix
    /// direct-descriptor-table backend); always `0`.
    pub direct_fd_idx: u32,
    /// Which kind of op this is.
    pub op: OpCode,
    /// Read/Write only: pointer to the caller-owned buffer.
    pub buf_ptr: *mut u8,
    /// Read/Write only: length of the buffer at `buf_ptr`.
    pub len: usize,
    /// Unused on this backend (no positional read/write here); always
    /// `0`.
    pub offset: i64,
    /// Connect only: the target address to connect to.
    pub addr: Option<SOCKADDR_STORAGE>,
    /// Connect only: length in bytes of the valid prefix of `addr`.
    pub addr_len: i32,
    /// `None` until the op has been submitted (assigned a pool slot);
    /// `Some(idx)` while in flight.
    pub slot_idx: Option<usize>,
    /// Accept only: a pre-created socket for `AcceptEx` to fill in, created
    /// lazily on first poll so `new()` stays a plain constructor.
    accept_socket: std::cell::Cell<usize>,
    /// `RecvFrom` only: caller-owned output buffers for the peer address
    /// (see `DtactUdpSocket::recv_from`), null for every other op.
    from_ptr: *mut SOCKADDR_STORAGE,
    from_len_ptr: *mut i32,
}

// SAFETY: every field is either a `Copy` primitive/pointer treated as an
// opaque handle (never dereferenced through `&DtactIoFuture` from two
// threads at once — `buf_ptr`'s pointee is only touched by whichever
// worker thread is actively servicing this op's slot) or already
// thread-safe (`Cell<usize>`, which is `!Sync` on its own, but this type
// is only ever polled from one task at a time per the `Future` contract,
// so no two threads observe `accept_socket` concurrently).
unsafe impl Send for DtactIoFuture {}
// SAFETY: same reasoning as `Send` — the `Future::poll` contract already
// guarantees exclusive access to `&mut self` (and by extension its
// `Cell`) from one thread at a time.
unsafe impl Sync for DtactIoFuture {}

impl DtactIoFuture {
    /// Build a not-yet-submitted future for the given op. Submission
    /// (queueing to the target worker) happens lazily on first
    /// [`Future::poll`].
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
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
            from_ptr: std::ptr::null_mut(),
            from_len_ptr: std::ptr::null_mut(),
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
                            i32::from(AF_INET),
                            SOCK_STREAM,
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
            OpCode::SendTo => IoRequest::SendTo {
                socket: self.fd as usize,
                buf_ptr: self.buf_ptr,
                len: self.len,
                addr: self.addr.unwrap(),
                addr_len: self.addr_len,
                slot_idx,
            },
            OpCode::RecvFrom => IoRequest::RecvFrom {
                socket: self.fd as usize,
                buf_ptr: self.buf_ptr,
                len: self.len,
                from_ptr: self.from_ptr,
                from_len_ptr: self.from_len_ptr,
                slot_idx,
            },
        }
    }
}

impl DtactIoFuture {
    /// Acquire a free op slot for `worker_idx`, registering `cx`'s waker
    /// in a waiting-list slot instead and returning `None` if the pool is
    /// currently exhausted (the caller must then return `Poll::Pending`).
    fn acquire_op_slot(state: &WorkerState, cx: &Context<'_>) -> Option<usize> {
        if let Some(i) = state.free_slots.pop() {
            return Some(i as usize);
        }
        let wait_idx = state.free_wait_slots.pop()?;
        let wait_slot = &state.wait_slots[wait_idx as usize];
        wait_slot
            .waker_data
            .store(cx.waker().data().cast_mut(), Ordering::Relaxed);
        wait_slot.waker_vtable.store(
            std::ptr::from_ref::<RawWakerVTable>(cx.waker().vtable()).cast_mut(),
            Ordering::Relaxed,
        );
        state.waiting_queue.push(wait_idx);

        let idx = state.free_slots.pop()?;
        wait_slot
            .waker_data
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        wait_slot
            .waker_vtable
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        Some(idx as usize)
    }

    /// Submit this op (first poll only): acquire a slot, register the
    /// waker on it, enqueue the [`IoRequest`] to a worker queue, and wake
    /// the worker if it's parked. Returns the acquired slot index, or
    /// `None` if the caller should return `Poll::Pending` right away
    /// (slot pool exhausted, or the target queue rejected the push).
    fn submit(self: Pin<&mut Self>, cx: &Context<'_>) -> Option<usize> {
        let this = self.get_mut();
        let state = &WORKERS.get().unwrap()[this.worker_idx];
        let Some(idx) = Self::acquire_op_slot(state, cx) else {
            cx.waker().wake_by_ref();
            return None;
        };

        let slot = &state.slots[idx];
        slot.completed.store(false, Ordering::Relaxed);
        slot.dropped.store(false, Ordering::Relaxed);
        slot.lock_waker();
        slot.waker_data
            .store(cx.waker().data().cast_mut(), Ordering::Relaxed);
        slot.waker_vtable.store(
            std::ptr::from_ref::<RawWakerVTable>(cx.waker().vtable()).cast_mut(),
            Ordering::Relaxed,
        );
        slot.unlock_waker();

        let req = this.create_io_request(idx);
        let q_idx = get_local_thread_id() % state.queues.len();
        let queue = &state.queues[q_idx];

        io_trace!(
            "[dtact-io] t={} slot={} fd={} op={:?} A_submit",
            trace_now_us(),
            idx,
            this.fd,
            this.op
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
            return None;
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

        this.slot_idx = Some(idx);
        Some(idx)
    }
}

impl Future for DtactIoFuture {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot_idx = if let Some(idx) = self.slot_idx {
            idx
        } else {
            let Some(idx) = self.as_mut().submit(cx) else {
                return Poll::Pending;
            };
            idx
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
            let new_data = cx.waker().data().cast_mut();
            let new_vtable = std::ptr::from_ref::<RawWakerVTable>(cx.waker().vtable()).cast_mut();

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
/// An async TCP stream backed by IOCP-issued `WSARecv`/`WSASend`.
pub struct DtactTcpStream {
    inner: std::net::TcpStream,
    worker_idx: usize,
}

fn pick_worker(socket: usize) -> usize {
    let num_workers = GLOBAL_CONFIG.get().map_or(1, |c| c.workers);
    socket % num_workers
}

impl DtactTcpStream {
    /// Wrap an existing `std::net::TcpStream`, switching it to
    /// non-blocking + `TCP_NODELAY` and associating it with this worker's
    /// IOCP.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if `set_nonblocking`/`set_nodelay` fail, or
    /// if `CreateIoCompletionPort` fails to associate the socket.
    ///
    /// # Panics
    ///
    /// Panics if called before [`init_runtime`]/[`init`] has been called
    /// (`WORKERS` not yet initialized).
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

    /// Read into `buf`, returning the number of bytes read (`0` = EOF).
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the underlying `WSARecv`/IOCP completion
    /// reports one (e.g. connection reset by peer).
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

    /// Write from `buf`, returning the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the underlying `WSASend`/IOCP completion
    /// reports one (e.g. connection reset by peer, broken pipe).
    pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        DtactIoFuture::new(
            self.worker_idx,
            self.inner.as_raw_socket() as u32,
            u32::MAX,
            OpCode::Write,
            buf.as_ptr().cast_mut(),
            buf.len(),
            0,
            None,
            0,
            None,
        )
        .await
    }

    /// Connect to `addr`, returning a ready-to-use stream.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if socket creation, binding the ephemeral
    /// local address `ConnectEx` requires, or the connect itself fails
    /// (e.g. `ConnectionRefused`, `TimedOut`).
    ///
    /// # Panics
    ///
    /// Panics if called before [`init_runtime`]/[`init`] has been called,
    /// same as [`Self::from_std`].
    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let domain = match addr {
            SocketAddr::V4(_) => AF_INET,
            SocketAddr::V6(_) => AF_INET6,
        };
        let raw_socket = unsafe {
            WSASocketW(
                i32::from(domain),
                SOCK_STREAM,
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
                (&raw const bind_addr).cast::<SOCKADDR>(),
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

/// An async TCP listener backed by IOCP-issued `AcceptEx`.
pub struct DtactTcpListener {
    inner: std::net::TcpListener,
    worker_idx: usize,
}

impl DtactTcpListener {
    /// Wrap an existing `std::net::TcpListener`, switching it to
    /// non-blocking and associating it with this worker's IOCP.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if `set_nonblocking` fails, or if
    /// `CreateIoCompletionPort` fails to associate the socket.
    ///
    /// # Panics
    ///
    /// Panics if called before [`init_runtime`]/[`init`] has been called
    /// (`WORKERS` not yet initialized).
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

    /// Accept an incoming connection, returning the new stream and the
    /// peer's address.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the underlying `AcceptEx`/IOCP
    /// completion reports one, or if a post-accept socket-option/address
    /// query fails.
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
                (&raw const listen_socket).cast::<u8>(),
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

// =========================================================================
// HIGH-LEVEL API: DtactUdpSocket  (IOCP backend)
// =========================================================================

/// Async UDP socket driven by the IOCP backend.
///
/// Supports the connectionless (`send_to`/`recv_from`) and connected
/// (`connect`/`send`/`recv`) patterns, mirroring `std::net::UdpSocket`'s and
/// `tokio::net::UdpSocket`'s API shape. `send_to`/`recv_from` issue overlapped
/// `WSASendTo`/`WSARecvFrom` ops; the connected `send`/`recv` reuse the same
/// `WSASend`/`WSARecv` machinery as [`DtactTcpStream`].
pub struct DtactUdpSocket {
    inner: std::net::UdpSocket,
    worker_idx: usize,
}

impl DtactUdpSocket {
    /// Bind a new UDP socket to `addr` and register it with the driver.
    ///
    /// # Errors
    /// Returns any error from binding the OS socket or associating it with
    /// the IOCP completion port.
    pub fn bind(addr: SocketAddr) -> impl Future<Output = std::io::Result<Self>> {
        std::future::ready(std::net::UdpSocket::bind(addr).and_then(Self::from_std))
    }

    /// Register an existing (already-bound) `std::net::UdpSocket`, taking
    /// ownership and associating it with the IOCP completion port.
    ///
    /// # Errors
    /// Returns any error from switching to non-blocking mode or the
    /// `CreateIoCompletionPort` association.
    ///
    /// # Panics
    /// Panics if called before [`init_runtime`]/[`init`] has been called
    /// (`WORKERS` not yet initialized).
    pub fn from_std(socket: std::net::UdpSocket) -> std::io::Result<Self> {
        socket.set_nonblocking(true)?;
        let raw = socket.as_raw_socket() as usize;
        let worker_idx = pick_worker(raw);
        let state = &WORKERS.get().unwrap()[worker_idx];
        let res = unsafe { CreateIoCompletionPort(raw as HANDLE, state.iocp, raw, 0) };
        if res.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            inner: socket,
            worker_idx,
        })
    }

    /// The local address this socket is bound to.
    ///
    /// # Errors
    /// Returns any error from the underlying `getsockname` call.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Send `buf` as a single datagram to `target`, returning the number of
    /// bytes sent.
    ///
    /// # Errors
    /// Returns any error from the underlying `WSASendTo`.
    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        let (win_addr, win_len) = socket_addr_to_win(&target);
        DtactIoFuture::new(
            self.worker_idx,
            self.inner.as_raw_socket() as u32,
            u32::MAX,
            OpCode::SendTo,
            buf.as_ptr().cast_mut(),
            buf.len(),
            0,
            Some(win_addr),
            win_len,
            None,
        )
        .await
    }

    /// Receive a single datagram into `buf`, returning the byte count and the
    /// peer address it came from.
    ///
    /// # Errors
    /// Returns any error from the underlying `WSARecvFrom`.
    pub async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        // These live in this async fn's frame, which stays pinned across the
        // await below, so the raw pointers handed to the op remain valid
        // until the OS fills them on completion.
        let mut from: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
        let mut from_len: i32 = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;

        let mut fut = DtactIoFuture::new(
            self.worker_idx,
            self.inner.as_raw_socket() as u32,
            u32::MAX,
            OpCode::RecvFrom,
            buf.as_mut_ptr(),
            buf.len(),
            0,
            None,
            0,
            None,
        );
        fut.from_ptr = &raw mut from;
        fut.from_len_ptr = &raw mut from_len;
        let n = fut.await?;
        Ok((n, win_to_socket_addr(&from)))
    }

    /// Connect this socket to `addr` so [`send`](Self::send)/[`recv`](Self::recv)
    /// can omit the peer address. UDP `connect` is a local operation (it just
    /// records the default peer), so this completes without a round trip.
    ///
    /// # Errors
    /// Returns any error from the underlying `connect`.
    pub fn connect(&self, addr: SocketAddr) -> impl Future<Output = std::io::Result<()>> {
        std::future::ready(self.inner.connect(addr))
    }

    /// Send `buf` to the connected peer (see [`connect`](Self::connect)),
    /// returning the number of bytes sent.
    ///
    /// # Errors
    /// Returns any error from the underlying `WSASend`.
    pub async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        DtactIoFuture::new(
            self.worker_idx,
            self.inner.as_raw_socket() as u32,
            u32::MAX,
            OpCode::Write,
            buf.as_ptr().cast_mut(),
            buf.len(),
            0,
            None,
            0,
            None,
        )
        .await
    }

    /// Receive a datagram from the connected peer into `buf`, returning the
    /// byte count.
    ///
    /// # Errors
    /// Returns any error from the underlying `WSARecv`.
    pub async fn recv(&self, buf: &mut [u8]) -> std::io::Result<usize> {
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
}

/// Parse a `SOCKADDR_STORAGE` (filled by `WSARecvFrom`) into a
/// `std::net::SocketAddr`.
fn win_to_socket_addr(storage: &SOCKADDR_STORAGE) -> SocketAddr {
    if storage.ss_family == AF_INET {
        // SAFETY: family checked to be AF_INET.
        let sin = unsafe { &*(std::ptr::from_ref(storage).cast::<SOCKADDR_IN>()) };
        // `S_addr` is stored in network byte order as a native `u32`; its
        // native-endian bytes are the address octets in order.
        let octets = unsafe { sin.sin_addr.S_un.S_addr }.to_ne_bytes();
        let ip = std::net::Ipv4Addr::from(octets);
        let port = u16::from_be(sin.sin_port);
        SocketAddr::V4(std::net::SocketAddrV4::new(ip, port))
    } else {
        // SAFETY: any non-AF_INET storage here is AF_INET6.
        let sin6 = unsafe { &*(std::ptr::from_ref(storage).cast::<SOCKADDR_IN6>()) };
        let ip = std::net::Ipv6Addr::from(unsafe { sin6.sin6_addr.u.Byte });
        let port = u16::from_be(sin6.sin6_port);
        let scope = unsafe { sin6.Anonymous.sin6_scope_id };
        SocketAddr::V6(std::net::SocketAddrV6::new(
            ip,
            port,
            sin6.sin6_flowinfo,
            scope,
        ))
    }
}

const fn socket_addr_to_win(addr: &SocketAddr) -> (SOCKADDR_STORAGE, i32) {
    let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = SOCKADDR_IN {
                sin_family: AF_INET,
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
                    (&raw const sin).cast::<u8>(),
                    (&raw mut storage).cast::<u8>(),
                    std::mem::size_of_val(&sin),
                );
            }
            std::mem::size_of_val(&sin) as i32
        }
        SocketAddr::V6(a) => {
            let sin6 = SOCKADDR_IN6 {
                sin6_family: AF_INET6,
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
                    (&raw const sin6).cast::<u8>(),
                    (&raw mut storage).cast::<u8>(),
                    std::mem::size_of_val(&sin6),
                );
            }
            std::mem::size_of_val(&sin6) as i32
        }
    };
    (storage, len)
}
