#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::undocumented_unsafe_blocks)]

use std::future::Future;
use std::pin::Pin;
#[cfg(all(feature = "tokio", not(feature = "native")))]
use std::task::{Context, Poll};

#[cfg(feature = "native")]
pub use dtact_macros::dtact_io_init as init;

// The Unix backend (io_uring on Linux, kqueue/mio elsewhere) is fd-based and
// cannot compile on Windows; the Windows backend (`windows_impl`, below) is
// IOCP-based and cannot compile on Unix. Anything that's neither (e.g. wasm)
// gets a clear build-time error instead of failing deep inside fd/handle code.
#[cfg(all(feature = "native", not(any(unix, windows))))]
compile_error!(
    "dtact-io's `native` feature supports Unix (io_uring/kqueue) and \
     Windows (IOCP) only. On other platforms, use the default `tokio` feature instead."
);

#[cfg(all(feature = "native", unix))]
mod native_impl {
    use super::*;
    use std::cell::RefCell;
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};
    use std::sync::OnceLock;
    use std::sync::atomic::{
        AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
    };
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    // =========================================================================
    // 0. LATENCY-BREAKDOWN TRACING (DTACT_IO_TRACE=1)
    // =========================================================================
    // `perf` SIGSEGVs on this runtime's stackful coroutines and `strace -T`'s
    // own ptrace overhead dominates the very microsecond-scale timings we're
    // trying to measure (every syscall reads as ~1-2ms regardless of backend).
    // This gives three in-process, monotonic-clock checkpoints per op so the
    // total latency can be split into "submit -> kernel completion observed
    // by io-worker" (io_uring/kernel round trip) vs "kernel completion ->
    // fiber re-polls and observes it" (wake propagation / scheduler
    // rescheduling), without any external tracer in the loop.
    #[inline]
    fn trace_enabled() -> bool {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| std::env::var_os("DTACT_IO_TRACE").is_some())
    }

    #[inline]
    fn trace_now_us() -> u128 {
        static START: OnceLock<std::time::Instant> = OnceLock::new();
        START
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_micros()
    }

    macro_rules! io_trace {
        ($($arg:tt)*) => {
            if trace_enabled() {
                eprintln!($($arg)*);
            }
        };
    }

    // =========================================================================
    // 1. HIGH-PERFORMANCE LOCK-FREE TREIBERSTACK (ABA-FREE)
    // =========================================================================
    #[repr(align(64))]
    #[doc(hidden)]
    pub struct TreiberStack {
        head: AtomicU64,
        next: Box<[AtomicU32]>,
    }

    impl TreiberStack {
        pub fn new(size: usize) -> Self {
            let mut next = Vec::with_capacity(size);
            for i in 0..size {
                next.push(AtomicU32::new((i + 1) as u32));
            }
            if size > 0 {
                next[size - 1].store(u32::MAX, Ordering::Relaxed);
            }
            Self {
                head: AtomicU64::new(u32::MAX as u64), // Initialize as empty index (u32::MAX) with 0 tag
                next: next.into_boxed_slice(),
            }
        }

        pub fn push(&self, idx: u32) {
            let mut head = self.head.load(Ordering::Acquire);
            loop {
                let head_idx = (head & 0xFFFFFFFF) as u32;
                let tag = (head >> 32) as u32;
                self.next[idx as usize].store(head_idx, Ordering::Release);
                let new_head = ((tag.wrapping_add(1) as u64) << 32) | (idx as u64);
                match self.head.compare_exchange_weak(
                    head,
                    new_head,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => head = actual,
                }
            }
        }

        pub fn pop(&self) -> Option<u32> {
            let mut head = self.head.load(Ordering::Acquire);
            loop {
                let head_idx = (head & 0xFFFFFFFF) as u32;
                if head_idx == u32::MAX {
                    return None;
                }
                let tag = (head >> 32) as u32;
                let next = self.next[head_idx as usize].load(Ordering::Acquire);
                let new_head = ((tag.wrapping_add(1) as u64) << 32) | (next as u64);
                match self.head.compare_exchange_weak(
                    head,
                    new_head,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(head_idx),
                    Err(actual) => head = actual,
                }
            }
        }
    }

    // =========================================================================
    // 2. PAGE-ALIGNED DMA-FRIENDLY BUFFERPOOL
    // =========================================================================
    #[doc(hidden)]
    pub struct BufferPool {
        arena_ptr: *mut u8,
        layout: std::alloc::Layout,
        chunk_size: usize,
        total_chunks: usize,
        global_free: TreiberStack,
        chunk_owners: Box<[AtomicU32]>,
    }

    unsafe impl Send for BufferPool {}
    unsafe impl Sync for BufferPool {}

    impl BufferPool {
        pub fn new(total_chunks: usize, chunk_size: usize) -> Self {
            let layout = std::alloc::Layout::from_size_align(total_chunks * chunk_size, 4096)
                .expect("Invalid layout alignment for BufferPool");
            let arena_ptr = unsafe { std::alloc::alloc(layout) };
            if arena_ptr.is_null() {
                panic!("Failed to allocate BufferPool memory arena");
            }

            let mut owners = Vec::with_capacity(total_chunks);
            for _ in 0..total_chunks {
                owners.push(AtomicU32::new(u32::MAX));
            }

            Self {
                arena_ptr,
                layout,
                chunk_size,
                total_chunks,
                global_free: TreiberStack::new(total_chunks),
                chunk_owners: owners.into_boxed_slice(),
            }
        }

        pub fn get_ptr(&self, idx: u32) -> *mut u8 {
            unsafe { self.arena_ptr.add(idx as usize * self.chunk_size) }
        }
    }

    impl Drop for BufferPool {
        fn drop(&mut self) {
            unsafe {
                std::alloc::dealloc(self.arena_ptr, self.layout);
            }
        }
    }

    // =========================================================================
    // 3. THREAD-LOCAL SLAB ALLOCATOR & RETURN PATH
    // =========================================================================
    struct LocalAllocator {
        thread_idx: usize,
        local_chunks: Vec<u32>,
    }

    thread_local! {
        static LOCAL_ALLOCATOR: RefCell<Option<LocalAllocator>> = const { RefCell::new(None) };
        static THREAD_ID: usize = {
            static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        };
    }

    #[doc(hidden)]
    pub fn get_local_thread_id() -> usize {
        THREAD_ID.with(|id| *id)
    }

    static THREAD_RETURNED_STACKS: OnceLock<Box<[TreiberStack]>> = OnceLock::new();
    static GLOBAL_BUFFER_POOL: OnceLock<BufferPool> = OnceLock::new();

    fn get_or_init_local_allocator() -> Option<usize> {
        LOCAL_ALLOCATOR.with(|cell| {
            let mut borrow = cell.borrow_mut();
            if borrow.is_none() {
                let idx = get_local_thread_id();
                if idx < 512 {
                    *borrow = Some(LocalAllocator {
                        thread_idx: idx,
                        local_chunks: Vec::new(),
                    });
                }
            }
            borrow.as_ref().map(|alloc| alloc.thread_idx)
        })
    }

    #[doc(hidden)]
    pub fn allocate_buffer() -> Option<u32> {
        let t_idx_opt = get_or_init_local_allocator();
        if let Some(t_idx) = t_idx_opt {
            LOCAL_ALLOCATOR.with(|cell| {
                let mut borrow = cell.borrow_mut();
                let alloc = borrow.as_mut().unwrap();

                // 1. Try local cache
                if let Some(idx) = alloc.local_chunks.pop() {
                    return Some(idx);
                }
                // 2. Try thread-specific returned stack
                if let Some(stacks) = THREAD_RETURNED_STACKS.get()
                    && let Some(stack) = stacks.get(t_idx)
                {
                    while let Some(idx) = stack.pop() {
                        alloc.local_chunks.push(idx);
                    }
                    if let Some(idx) = alloc.local_chunks.pop() {
                        return Some(idx);
                    }
                }
                // 3. Fallback to global pool
                if let Some(pool) = GLOBAL_BUFFER_POOL.get()
                    && let Some(idx) = pool.global_free.pop()
                {
                    pool.chunk_owners[idx as usize].store(t_idx as u32, Ordering::Release);
                    return Some(idx);
                }
                None
            })
        } else if let Some(pool) = GLOBAL_BUFFER_POOL.get() {
            if let Some(idx) = pool.global_free.pop() {
                pool.chunk_owners[idx as usize].store(u32::MAX, Ordering::Release);
                return Some(idx);
            }
            None
        } else {
            None
        }
    }

    #[doc(hidden)]
    pub fn free_buffer(idx: u32) {
        if let Some(pool) = GLOBAL_BUFFER_POOL.get() {
            let owner = pool.chunk_owners[idx as usize].load(Ordering::Acquire);
            if owner == u32::MAX {
                pool.global_free.push(idx);
                return;
            }

            let current_thread_idx = get_or_init_local_allocator();
            if Some(owner as usize) == current_thread_idx {
                LOCAL_ALLOCATOR.with(|cell| {
                    if let Some(alloc) = cell.borrow_mut().as_mut() {
                        alloc.local_chunks.push(idx);
                    }
                });
            } else if let Some(stacks) = THREAD_RETURNED_STACKS.get() {
                if let Some(stack) = stacks.get(owner as usize) {
                    stack.push(idx);
                } else {
                    pool.global_free.push(idx);
                }
            } else {
                pool.global_free.push(idx);
            }
        }
    }

    #[doc(hidden)]
    pub struct BufferSlice {
        pub buf_idx: u32,
        pub read_pos: usize,
        pub write_pos: usize,
    }

    impl BufferSlice {
        pub const fn new(buf_idx: u32, len: usize) -> Self {
            Self {
                buf_idx,
                read_pos: 0,
                write_pos: len,
            }
        }

        pub fn data(&self) -> *mut u8 {
            GLOBAL_BUFFER_POOL.get().unwrap().get_ptr(self.buf_idx)
        }

        pub fn remaining(&self) -> usize {
            self.write_pos.saturating_sub(self.read_pos)
        }
    }

    impl Drop for BufferSlice {
        fn drop(&mut self) {
            free_buffer(self.buf_idx);
        }
    }

    // =========================================================================
    // 4. CACHE-ALIGNED LOCK-FREE SPSC RINGBUFFER
    // =========================================================================
    // NOTE: no outer `repr(align(64))` here — `head`/`tail` already each own a
    // cache line via `CacheAlignedUsize`, which is what actually matters for
    // avoiding false sharing between producer and consumer. Aligning the
    // container itself only pads the start of `buffer` for no benefit.
    #[doc(hidden)]
    pub struct SpscQueue<T> {
        head: CacheAlignedUsize,
        tail: CacheAlignedUsize,
        buffer: Box<[std::mem::MaybeUninit<T>]>,
        capacity: usize,
    }

    #[repr(align(64))]
    struct CacheAlignedUsize {
        value: AtomicUsize,
    }

    unsafe impl<T: Send> Send for SpscQueue<T> {}
    unsafe impl<T: Send> Sync for SpscQueue<T> {}

    impl<T> SpscQueue<T> {
        pub fn new(capacity: usize) -> Self {
            assert!(capacity.is_power_of_two());
            let mut buffer = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                buffer.push(std::mem::MaybeUninit::uninit());
            }
            Self {
                head: CacheAlignedUsize {
                    value: AtomicUsize::new(0),
                },
                tail: CacheAlignedUsize {
                    value: AtomicUsize::new(0),
                },
                buffer: buffer.into_boxed_slice(),
                capacity,
            }
        }

        pub fn push(&self, value: T) -> Result<(), T> {
            let tail = self.tail.value.load(Ordering::Relaxed);
            let head = self.head.value.load(Ordering::Acquire);
            if tail.wrapping_sub(head) == self.capacity {
                return Err(value);
            }
            let mask = self.capacity - 1;
            let idx = tail & mask;
            unsafe {
                let ptr = self.buffer[idx].as_ptr() as *mut T;
                ptr.write(value);
            }
            self.tail
                .value
                .store(tail.wrapping_add(1), Ordering::Release);
            Ok(())
        }

        pub fn pop(&self) -> Option<T> {
            let head = self.head.value.load(Ordering::Relaxed);
            let tail = self.tail.value.load(Ordering::Acquire);
            if head == tail {
                return None;
            }
            let mask = self.capacity - 1;
            let idx = head & mask;
            let value = unsafe {
                let ptr = self.buffer[idx].as_ptr();
                ptr.read()
            };
            self.head
                .value
                .store(head.wrapping_add(1), Ordering::Release);
            Some(value)
        }

        pub fn is_empty(&self) -> bool {
            let head = self.head.value.load(Ordering::Relaxed);
            let tail = self.tail.value.load(Ordering::Acquire);
            head == tail
        }
    }

    impl<T> Drop for SpscQueue<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }

    // =========================================================================
    // 5. IO ENGINE WORKERS AND EVENTS DEFINITIONS
    // =========================================================================
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum OpCode {
        Read,
        Write,
        Accept,
        Connect,
    }

    pub enum IoRequest {
        Read {
            fd: u32,
            direct_fd_idx: u32,
            buf_ptr: *mut u8,
            len: usize,
            offset: i64,
            slot_idx: usize,
        },
        Write {
            fd: u32,
            direct_fd_idx: u32,
            buf_ptr: *const u8,
            len: usize,
            offset: i64,
            slot_idx: usize,
        },
        Accept {
            fd: u32,
            direct_fd_idx: u32,
            slot_idx: usize,
        },
        Connect {
            fd: u32,
            direct_fd_idx: u32,
            addr: libc::sockaddr_storage,
            addr_len: libc::socklen_t,
            slot_idx: usize,
        },
        RegisterFile {
            fd: RawFd,
            slot_idx: usize,
        },
        UnregisterFile {
            direct_fd_idx: u32,
            slot_idx: usize,
        },
    }

    /// Lock-free waker slot.
    ///
    /// `waker` is written by the fiber (before the SPSC push) and read+cleared
    /// by the io-worker (after the SPSC pop, under the Acquire that observes the
    /// Release from the SPSC push).  Since only one fiber owns a slot at a time
    /// and the io-worker reads only after the ordering guarantee, there is no
    /// data race — no Mutex needed.
    // `#[repr(align(64))]` on the slot itself (not just the backing array) is
    // what actually prevents false sharing: with a bare struct, adjacent slots
    // in `Box<[WakerSlot]>` pack tightly and two io-worker threads completing
    // unrelated ops in neighbouring slots end up bouncing the same cache line.
    #[repr(align(64))]
    struct WakerSlot {
        /// Stores the raw `data` pointer of a fiber `Waker` (`*const FiberContext`).
        waker_data: AtomicPtr<()>,
        /// Stores the raw `vtable` pointer of a fiber `Waker` (`*const RawWakerVTable`).
        /// Combined, these allow zero-cost reconstruction of the `RawWaker` without clone/drop overhead.
        waker_vtable: AtomicPtr<RawWakerVTable>,
        waker_lock: AtomicBool,
        result: AtomicI32,
        completed: AtomicBool,
        dropped: AtomicBool,
        /// fd this op was issued against, recorded so the owning worker thread
        /// can cancel/clean up the op purely from `slot_idx` (see `cancel_queue`)
        /// without the dropping thread needing to touch backend state itself.
        origin_fd: AtomicU32,
    }

    #[repr(align(64))]
    struct WaitSlot {
        waker_data: AtomicPtr<()>,
        waker_vtable: AtomicPtr<RawWakerVTable>,
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

    pub struct WorkerState {
        #[cfg(target_os = "linux")]
        ring: std::cell::UnsafeCell<io_uring::IoUring>,
        #[cfg(not(target_os = "linux"))]
        poll: std::cell::UnsafeCell<mio::Poll>,

        queues: Box<[SpscQueue<IoRequest>]>,
        slots: Box<[WakerSlot]>,
        free_slots: TreiberStack,

        wait_slots: Box<[WaitSlot]>,
        free_wait_slots: TreiberStack,
        waiting_queue: TreiberStack,
        is_sleeping: AtomicBool,

        /// Slot indices whose owning `DtactIoFuture` was dropped before the op
        /// completed. Multiple arbitrary threads may push here (whichever
        /// thread drops the future), so this must be MP-safe — `TreiberStack`
        /// already is (CAS-based), unlike the per-thread SPSC `queues`, which
        /// must never receive a push from more than one producer thread.
        /// Only the owning io-worker thread pops from it.
        cancel_queue: TreiberStack,

        #[cfg(target_os = "linux")]
        wake_eventfd: RawFd,
        #[cfg(target_os = "linux")]
        sqpoll_enabled: bool,
        #[cfg(not(target_os = "linux"))]
        waker: std::sync::Arc<mio::Waker>,

        direct_fd_free: TreiberStack,
    }

    unsafe impl Send for WorkerState {}
    unsafe impl Sync for WorkerState {}

    struct GlobalConfig {
        workers: usize,
        buffer_pool_size: usize,
        chunk_size: usize,
        pin_cpus: Vec<usize>,
        ring_depth: u32,
    }

    static GLOBAL_CONFIG: OnceLock<GlobalConfig> = OnceLock::new();
    static WORKERS: OnceLock<Box<[WorkerState]>> = OnceLock::new();
    static SHUTDOWN: AtomicBool = AtomicBool::new(false);

    #[cfg(target_os = "linux")]
    fn pin_thread_to_cpu(cpu_id: usize) -> Result<(), &'static str> {
        unsafe {
            let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu_id, &mut cpuset);
            let thread = libc::pthread_self();
            let res = libc::pthread_setaffinity_np(
                thread,
                std::mem::size_of::<libc::cpu_set_t>(),
                &cpuset,
            );
            if res == 0 {
                Ok(())
            } else {
                Err("pthread_setaffinity_np failed")
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn pin_thread_to_cpu(_cpu_id: usize) -> Result<(), &'static str> {
        Ok(())
    }

    /// Registering `n` direct/fixed files with io_uring requires the
    /// process's `RLIMIT_NOFILE` soft limit to cover `n` (on top of whatever
    /// fds are already open) — unconditionally asking for 4096 slots panics
    /// with EMFILE on any environment with a lower default soft limit (1024
    /// is a common distro default). Raise our own soft limit to the hard
    /// limit first (always permitted for a non-privileged process to do to
    /// itself), then size the direct-fd table to what's actually available,
    /// capped at the desired maximum and leaving headroom for real sockets.
    #[cfg(target_os = "linux")]
    fn pick_direct_fd_count(desired_max: usize) -> usize {
        unsafe {
            let mut lim: libc::rlimit = std::mem::zeroed();
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
                if lim.rlim_cur < lim.rlim_max {
                    let raised = libc::rlimit {
                        rlim_cur: lim.rlim_max,
                        rlim_max: lim.rlim_max,
                    };
                    let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &raised);
                    let _ = libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim);
                }
                let headroom = 256usize;
                let available = (lim.rlim_cur as usize).saturating_sub(headroom);
                return available.clamp(64, desired_max);
            }
        }
        // getrlimit itself failed — fall back to a conservative count rather
        // than the original unconditional 4096.
        64
    }

    // =========================================================================
    // 6. RUNTIME INITIALIZATION
    // =========================================================================
    pub fn init_runtime(
        workers: usize,
        buffer_pool_size: usize,
        chunk_size: usize,
        pin_cpus: &[usize],
        ring_depth: u32,
    ) {
        let config = GlobalConfig {
            workers,
            buffer_pool_size,
            chunk_size,
            pin_cpus: pin_cpus.to_vec(),
            ring_depth,
        };
        if GLOBAL_CONFIG.set(config).is_err() {
            return;
        }

        let pool = BufferPool::new(buffer_pool_size, chunk_size);
        let _ = GLOBAL_BUFFER_POOL.set(pool);

        let mut returned_stacks = Vec::with_capacity(512);
        for _ in 0..512 {
            returned_stacks.push(TreiberStack::new(0));
        }
        let _ = THREAD_RETURNED_STACKS.set(returned_stacks.into_boxed_slice());

        let mut worker_states = Vec::with_capacity(workers);
        for _worker_idx in 0..workers {
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
                    origin_fd: AtomicU32::new(u32::MAX),
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
            let is_sleeping = AtomicBool::new(false);

            #[cfg(target_os = "linux")]
            let direct_fd_count = pick_direct_fd_count(4096);
            #[cfg(not(target_os = "linux"))]
            let direct_fd_count = 4096usize;

            let direct_fd_free = TreiberStack::new(direct_fd_count);
            for i in 0..direct_fd_count as u32 {
                direct_fd_free.push(i);
            }

            #[cfg(target_os = "linux")]
            {
                let wake_eventfd =
                    unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
                if wake_eventfd < 0 {
                    panic!("Failed to create eventfd");
                }

                let (ring, sqpoll_enabled) = match io_uring::IoUring::builder()
                    .setup_sqpoll(2000)
                    .build(ring_depth)
                {
                    Ok(r) => (r, true),
                    Err(_) => (
                        io_uring::IoUring::new(ring_depth)
                            .expect("Failed to initialize io_uring fallback"),
                        false,
                    ),
                };

                let initial_fds = vec![-1; direct_fd_count];
                ring.submitter()
                    .register_files(&initial_fds)
                    .expect("Failed to register direct FDs");

                worker_states.push(WorkerState {
                    ring: std::cell::UnsafeCell::new(ring),
                    queues,
                    slots,
                    free_slots,
                    wait_slots,
                    free_wait_slots,
                    waiting_queue,
                    is_sleeping,
                    cancel_queue,
                    wake_eventfd,
                    sqpoll_enabled,
                    direct_fd_free,
                });
            }

            #[cfg(not(target_os = "linux"))]
            {
                let poll = mio::Poll::new().expect("Failed to initialize mio Poll");
                let waker = std::sync::Arc::new(
                    mio::Waker::new(poll.registry(), mio::Token(0))
                        .expect("Failed to create mio waker"),
                );

                worker_states.push(WorkerState {
                    poll: std::cell::UnsafeCell::new(poll),
                    queues,
                    slots,
                    free_slots,
                    wait_slots,
                    free_wait_slots,
                    waiting_queue,
                    is_sleeping,
                    cancel_queue,
                    waker,
                    direct_fd_free,
                });
            }
        }

        let worker_states = worker_states.into_boxed_slice();
        let _ = WORKERS.set(worker_states);

        for worker_idx in 0..workers {
            std::thread::Builder::new()
                .name(format!("dtact-io-worker-{worker_idx}"))
                .spawn(move || {
                    LOCAL_ALLOCATOR.with(|cell| {
                        *cell.borrow_mut() = Some(LocalAllocator {
                            thread_idx: worker_idx,
                            local_chunks: Vec::new(),
                        });
                    });

                    let state = &WORKERS.get().unwrap()[worker_idx];

                    #[cfg(target_os = "linux")]
                    run_linux_worker_loop(worker_idx, state);

                    #[cfg(not(target_os = "linux"))]
                    run_mio_worker_loop(worker_idx, state);
                })
                .expect("Failed to spawn dtact-io worker thread");
        }
    }

    pub fn shutdown_runtime() {
        SHUTDOWN.store(true, Ordering::Release);
        if let Some(workers) = WORKERS.get() {
            for state in workers.iter() {
                #[cfg(target_os = "linux")]
                let _ = unsafe {
                    libc::write(
                        state.wake_eventfd,
                        &1u64 as *const u64 as *const libc::c_void,
                        8,
                    )
                };
                #[cfg(not(target_os = "linux"))]
                state.waker.wake();
            }
        }
    }

    // =========================================================================
    // 7. LINUX SYSTEM CALL DRIVER (io_uring)
    // =========================================================================
    #[cfg(target_os = "linux")]
    fn run_linux_worker_loop(worker_idx: usize, state: &WorkerState) {
        if let Some(config) = GLOBAL_CONFIG.get()
            && let Some(&cpu_id) = config.pin_cpus.get(worker_idx)
        {
            let _ = pin_thread_to_cpu(cpu_id);
        }

        let ring = unsafe { &mut *state.ring.get() };
        let mut eventfd_buf = 0u64;
        let mut eventfd_submitted = false;

        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }

            if !eventfd_submitted {
                let sqe = io_uring::opcode::Read::new(
                    io_uring::types::Fd(state.wake_eventfd),
                    &mut eventfd_buf as *mut u64 as *mut u8,
                    8,
                )
                .build()
                .user_data(u64::MAX);

                unsafe {
                    if ring.submission().push(&sqe).is_ok() {
                        eventfd_submitted = true;
                    }
                }
            }

            let mut pushed_sqe = false;
            for q in state.queues.iter() {
                while let Some(req) = q.pop() {
                    pushed_sqe = true;
                    let _ = submit_linux_request(state, req);
                }
            }

            while let Some(slot_idx) = state.cancel_queue.pop() {
                pushed_sqe = true;
                let sqe = io_uring::opcode::AsyncCancel::new(slot_idx as u64)
                    .build()
                    .user_data(u64::MAX - 1);
                unsafe {
                    let _ = push_sqe(ring, &sqe);
                }
            }

            // `push()` only writes into the *local* SQE array and tail — it is
            // not visible to the SQPOLL kernel thread (which polls the shared
            // mmap'd ring) until the tail is published. `submit()` publishes
            // it as a side effect of the io_uring_enter syscall, but that
            // syscall is exactly what SQPOLL exists to avoid. So: always
            // `sync()` (cheap, no syscall — just a store-release into shared
            // memory) so an actively-spinning kernel thread sees new entries
            // immediately, and only pay for `io_uring_enter` (via `submit()`)
            // when the kernel thread has actually gone to sleep and asked to
            // be woken (`need_wakeup`), or when SQPOLL isn't in use at all.
            let any_pending = state.queues.iter().any(|q| !q.is_empty());

            // When we just submitted new work and there is nothing else
            // queued behind it, we already know the very next thing this
            // loop will do is block waiting for that work to complete (the
            // `!pushed_sqe && !has_completions` branch below, one iteration
            // later) — that used to cost a *second* `io_uring_enter` syscall
            // (`submit()` now, `submit_and_wait(1)` next loop). Fold both
            // into a single blocking enter here instead.
            let mut folded_wait = false;
            if pushed_sqe || eventfd_submitted {
                ring.submission().sync();
                if pushed_sqe && !any_pending {
                    state.is_sleeping.store(true, Ordering::SeqCst);
                    // Dekker-style re-check: a producer that pushed to a queue
                    // and observed `is_sleeping == false` just before our store
                    // above (a StoreLoad reorder is otherwise legal even on
                    // x86-TSO) would skip the eventfd wakeup, leaving us to
                    // block forever on a request nobody drained. Re-scan the
                    // queues now that `is_sleeping` is published; if anything
                    // landed, bail out of the blocking wait and let the top of
                    // the loop drain it next iteration instead.
                    fence(Ordering::SeqCst);
                    let missed = state.queues.iter().any(|q| !q.is_empty());
                    if missed {
                        state.is_sleeping.store(false, Ordering::SeqCst);
                        let sr = ring.submit();
                        io_trace!(
                            "[dtact-io] t={} loop submit(folded-missed) result={:?}",
                            trace_now_us(),
                            sr
                        );
                    } else {
                        let sr = ring.submit_and_wait(1);
                        state.is_sleeping.store(false, Ordering::Release);
                        io_trace!(
                            "[dtact-io] t={} loop submit_and_wait(folded) result={:?}",
                            trace_now_us(),
                            sr
                        );
                    }
                    folded_wait = true;
                } else {
                    let should_enter = if state.sqpoll_enabled {
                        ring.submission().need_wakeup()
                    } else {
                        true
                    };
                    if should_enter {
                        let sr = ring.submit();
                        io_trace!(
                            "[dtact-io] t={} loop submit() result={:?}",
                            trace_now_us(),
                            sr
                        );
                    }
                }
            }

            let mut has_completions = false;
            let mut cq = ring.completion();
            cq.sync();
            let cq_len = cq.len();
            if cq_len > 0 {
                io_trace!("[dtact-io] t={} loop cq_len={}", trace_now_us(), cq_len);
            }
            for cqe in cq {
                has_completions = true;
                let user_data = cqe.user_data();
                let res = cqe.result();

                if user_data == u64::MAX {
                    eventfd_submitted = false;
                } else if user_data == u64::MAX - 1 {
                    // Cancel event completion, do nothing
                } else {
                    process_linux_completion(state, user_data as usize, res);
                }
            }

            if !folded_wait && !pushed_sqe && !has_completions {
                state.is_sleeping.store(true, Ordering::SeqCst);
                // Same Dekker-style re-check as the folded-wait path above:
                // `any_pending` was computed earlier in this iteration and may
                // be stale by now. Without this re-scan + fence, a producer's
                // push-then-check-is_sleeping (also StoreLoad-ordered) can
                // race with our store-then-block here and neither side sends
                // a wakeup — the classic lost-wakeup deadlock.
                fence(Ordering::SeqCst);
                let missed = state.queues.iter().any(|q| !q.is_empty());
                if !any_pending && !missed {
                    let sr = ring.submit_and_wait(1);
                    io_trace!(
                        "[dtact-io] t={} loop submit_and_wait(idle) result={:?}",
                        trace_now_us(),
                        sr
                    );
                } else if missed {
                    let sr = ring.submit();
                    io_trace!(
                        "[dtact-io] t={} loop submit(idle-missed) result={:?}",
                        trace_now_us(),
                        sr
                    );
                }
                state.is_sleeping.store(false, Ordering::Release);
            }
        }
    }

    #[cfg(target_os = "linux")]
    unsafe fn push_sqe(
        ring: &mut io_uring::IoUring,
        sqe: &io_uring::squeue::Entry,
    ) -> Result<(), &'static str> {
        loop {
            let res = unsafe { ring.submission().push(sqe) };
            match res {
                Ok(_) => return Ok(()),
                Err(_) => {
                    let _ = ring.submit();
                    core::hint::spin_loop();
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn submit_linux_request(state: &WorkerState, req: IoRequest) -> Result<(), &'static str> {
        let ring = unsafe { &mut *state.ring.get() };

        let sqe = match req {
            IoRequest::Read {
                fd,
                direct_fd_idx,
                buf_ptr,
                len,
                offset,
                slot_idx,
            } => {
                let use_fixed = direct_fd_idx != u32::MAX;
                let target_fd = if use_fixed {
                    direct_fd_idx as i32
                } else {
                    fd as i32
                };
                let mut s = io_uring::opcode::Read::new(
                    io_uring::types::Fd(target_fd),
                    buf_ptr,
                    len as u32,
                )
                .offset(offset as u64)
                .build()
                .user_data(slot_idx as u64);
                if use_fixed {
                    s = s.flags(io_uring::squeue::Flags::FIXED_FILE);
                }
                s
            }
            IoRequest::Write {
                fd,
                direct_fd_idx,
                buf_ptr,
                len,
                offset,
                slot_idx,
            } => {
                let use_fixed = direct_fd_idx != u32::MAX;
                let target_fd = if use_fixed {
                    direct_fd_idx as i32
                } else {
                    fd as i32
                };
                let mut s = io_uring::opcode::Write::new(
                    io_uring::types::Fd(target_fd),
                    buf_ptr,
                    len as u32,
                )
                .offset(offset as u64)
                .build()
                .user_data(slot_idx as u64);
                if use_fixed {
                    s = s.flags(io_uring::squeue::Flags::FIXED_FILE);
                }
                s
            }
            IoRequest::Accept {
                fd,
                direct_fd_idx,
                slot_idx,
            } => {
                let use_fixed = direct_fd_idx != u32::MAX;
                let target_fd = if use_fixed {
                    direct_fd_idx as i32
                } else {
                    fd as i32
                };
                let mut s = io_uring::opcode::Accept::new(
                    io_uring::types::Fd(target_fd),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
                .build()
                .user_data(slot_idx as u64);
                if use_fixed {
                    s = s.flags(io_uring::squeue::Flags::FIXED_FILE);
                }
                s
            }
            IoRequest::Connect {
                fd,
                direct_fd_idx,
                addr,
                addr_len,
                slot_idx,
            } => {
                // `addr` lives inside the IoRequest enum on the io-worker's stack.
                // io_uring copies the sockaddr into the kernel during push_sqe /
                // io_uring_enter, so a stack pointer is safe for the duration of
                // submit_linux_request.  No Mutex required.
                let addr_ptr = &addr as *const libc::sockaddr_storage as *const libc::sockaddr;

                let use_fixed = direct_fd_idx != u32::MAX;
                let target_fd = if use_fixed {
                    direct_fd_idx as i32
                } else {
                    fd as i32
                };
                let mut s = io_uring::opcode::Connect::new(
                    io_uring::types::Fd(target_fd),
                    addr_ptr,
                    addr_len,
                )
                .build()
                .user_data(slot_idx as u64);
                if use_fixed {
                    s = s.flags(io_uring::squeue::Flags::FIXED_FILE);
                }
                s
            }
            IoRequest::RegisterFile { fd, slot_idx } => {
                if let Some(direct_idx) = state.direct_fd_free.pop() {
                    let fds = [fd];
                    let res = ring.submitter().register_files_update(direct_idx, &fds);
                    let out_res = match res {
                        Ok(_) => direct_idx as i32,
                        Err(e) => -(e.raw_os_error().unwrap_or(libc::EINVAL)),
                    };
                    process_linux_completion(state, slot_idx, out_res);
                } else {
                    process_linux_completion(state, slot_idx, -libc::ENFILE);
                }
                return Ok(());
            }
            IoRequest::UnregisterFile {
                direct_fd_idx,
                slot_idx,
            } => {
                let fds = [-1];
                let res = ring.submitter().register_files_update(direct_fd_idx, &fds);
                state.direct_fd_free.push(direct_fd_idx);
                let out_res = match res {
                    Ok(_) => 0,
                    Err(e) => -(e.raw_os_error().unwrap_or(libc::EINVAL)),
                };
                process_linux_completion(state, slot_idx, out_res);
                return Ok(());
            }
        };

        let user_data = sqe.get_user_data();
        let r = unsafe { push_sqe(ring, &sqe) };
        io_trace!(
            "[dtact-io] t={} slot={} submit_linux_request pushed_local ok={}",
            trace_now_us(),
            user_data,
            r.is_ok()
        );
        r
    }

    #[cfg(target_os = "linux")]
    fn process_linux_completion(state: &WorkerState, slot_idx: usize, res: i32) {
        let slot = &state.slots[slot_idx];

        io_trace!(
            "[dtact-io] t={} slot={} res={} B_kernel_complete",
            trace_now_us(),
            slot_idx,
            res
        );

        slot.result.store(res, Ordering::Release);

        // Extract (and fully detach) the waker BEFORE publishing `completed`.
        // If `completed` were published first, a concurrently spin-polling
        // fiber (see `wait_pinned`'s adaptive spin) could observe it, free
        // this slot, and have a *brand new* op reuse the same slot index and
        // install a fresh waker into the very fields we're about to swap out
        // here — we'd then wake (or null out) the new op's waker instead of
        // the one this completion actually belongs to, permanently losing
        // the new op's wakeup. Waker extraction is a self-contained lock+swap,
        // so doing it first is safe regardless of publication order.
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

    // =========================================================================
    // 8. FALLBACK MULTIPLEXER (mio REACTOR) FOR OTHER PLATFORMS
    // =========================================================================
    #[cfg(not(target_os = "linux"))]
    struct FdState {
        reader_waker: Option<Waker>,
        writer_waker: Option<Waker>,
        /// Which `WakerSlot` each waker came from, so `cancel_queue` draining
        /// can find and clear the right side without an O(n) scan.
        reader_slot: Option<usize>,
        writer_slot: Option<usize>,
        /// Interest last handed to `reregister` for this fd, so callers can
        /// skip the syscall entirely when the newly-computed interest is
        /// identical (e.g. a Write request arriving while a Read is already
        /// registered doesn't need to touch epoll/kqueue at all).
        registered_interest: Option<mio::Interest>,
    }

    #[cfg(not(target_os = "linux"))]
    impl FdState {
        const fn new() -> Self {
            Self {
                reader_waker: None,
                writer_waker: None,
                reader_slot: None,
                writer_slot: None,
                registered_interest: None,
            }
        }
    }

    /// Grow `fd_states` on demand instead of preallocating a fixed-size table
    /// that silently drops events for any fd beyond it.
    #[cfg(not(target_os = "linux"))]
    fn ensure_fd_state(fd_states: &mut Vec<FdState>, fd: usize) {
        if fd_states.len() <= fd {
            fd_states.resize_with(fd + 1, FdState::new);
        }
    }

    /// Install `fd_state`'s currently-desired waker for `fd` (reader or
    /// writer side, matching `is_reader`) and reregister with the OS poller
    /// only when the resulting interest set actually changed. Returns
    /// `false` if `reregister` failed — in which case the just-installed
    /// waker has already been woken immediately (rather than left parked
    /// waiting for an event that, given the broken registration, may never
    /// arrive) and cleared back out of `fd_state`.
    #[cfg(not(target_os = "linux"))]
    fn install_interest(
        state: &WorkerState,
        fd_state: &mut FdState,
        fd: u32,
        slot_idx: usize,
        is_reader: bool,
    ) -> bool {
        let slot = &state.slots[slot_idx];
        slot.lock_waker();
        let data = slot
            .waker_data
            .swap(std::ptr::null_mut(), Ordering::Relaxed);
        let vtable = slot
            .waker_vtable
            .swap(std::ptr::null_mut(), Ordering::Relaxed);
        slot.unlock_waker();

        let waker = if !data.is_null() && !vtable.is_null() {
            let raw = RawWaker::new(data as *const (), unsafe { &*vtable });
            Some(unsafe { Waker::from_raw(raw) })
        } else {
            None
        };

        if is_reader {
            fd_state.reader_waker = waker;
            fd_state.reader_slot = if fd_state.reader_waker.is_some() {
                Some(slot_idx)
            } else {
                None
            };
        } else {
            fd_state.writer_waker = waker;
            fd_state.writer_slot = if fd_state.writer_waker.is_some() {
                Some(slot_idx)
            } else {
                None
            };
        }

        let interest = get_mio_interest(fd_state);
        if fd_state.registered_interest == Some(interest) {
            return true;
        }

        let res = unsafe {
            let poll = &mut *state.poll.get();
            poll.registry().reregister(
                &mut mio::unix::SourceFd(&(fd as i32)),
                mio::Token(fd as usize),
                interest,
            )
        };

        match res {
            Ok(()) => {
                fd_state.registered_interest = Some(interest);
                true
            }
            Err(e) => {
                io_trace!(
                    "[dtact-io] t={} fd={} reregister failed: {e}",
                    trace_now_us(),
                    fd
                );
                // Registration is broken for this fd — don't leave the fiber
                // parked waiting for an event that may never come; wake it
                // immediately so it retries the syscall directly and
                // surfaces a real error through the existing WouldBlock path.
                let woken = if is_reader {
                    fd_state.reader_waker.take()
                } else {
                    fd_state.writer_waker.take()
                };
                if is_reader {
                    fd_state.reader_slot = None;
                } else {
                    fd_state.writer_slot = None;
                }
                if let Some(w) = woken {
                    w.wake();
                }
                false
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn get_mio_interest(fd_state: &FdState) -> mio::Interest {
        let r = fd_state.reader_waker.is_some();
        let w = fd_state.writer_waker.is_some();
        if r && w {
            mio::Interest::READABLE | mio::Interest::WRITABLE
        } else if r {
            mio::Interest::READABLE
        } else if w {
            mio::Interest::WRITABLE
        } else {
            mio::Interest::READABLE
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn run_mio_worker_loop(worker_idx: usize, state: &WorkerState) {
        if let Some(config) = GLOBAL_CONFIG.get() {
            if let Some(&cpu_id) = config.pin_cpus.get(worker_idx) {
                let _ = pin_thread_to_cpu(cpu_id);
            }
        }

        let poll = unsafe { &mut *state.poll.get() };
        let mut events = mio::Events::with_capacity(256);
        // Starts small and grows on demand via `ensure_fd_state` — no fixed
        // upper bound on fd numbers, unlike the old preallocated 65536-entry
        // table (which silently dropped events for anything beyond it).
        let mut fd_states: Vec<FdState> = Vec::with_capacity(256);

        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
                break;
            }

            let mut processed_any = false;
            for q in state.queues.iter() {
                while let Some(req) = q.pop() {
                    processed_any = true;
                    process_mio_request(state, &mut fd_states, req);
                }
            }

            // Drained *after* the per-thread request queues above, so a
            // Cancel for a slot whose original request was still sitting in
            // its SPSC queue at the start of this iteration is guaranteed to
            // be processed after that request installs its waker — never
            // before, which would free/reuse the slot out from under it.
            while let Some(slot_idx) = state.cancel_queue.pop() {
                processed_any = true;
                cancel_mio_slot(state, &mut fd_states, slot_idx as usize);
            }

            state.is_sleeping.store(true, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            let mut any_pending = false;
            for q in state.queues.iter() {
                if !q.is_empty() {
                    any_pending = true;
                    break;
                }
            }

            let poll_res = if !any_pending {
                poll.poll(&mut events, Some(std::time::Duration::from_millis(10)))
            } else {
                poll.poll(&mut events, Some(std::time::Duration::from_millis(0)))
            };
            state.is_sleeping.store(false, Ordering::Release);

            if poll_res.is_err() {
                continue;
            }

            for event in events.iter() {
                let token = event.token();
                if token == mio::Token(0) {
                    continue;
                }
                let fd = token.0;
                process_mio_event(
                    state,
                    &mut fd_states,
                    fd,
                    event.is_readable(),
                    event.is_writable(),
                );
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn process_mio_request(state: &WorkerState, fd_states: &mut Vec<FdState>, req: IoRequest) {
        match req {
            IoRequest::Read { fd, slot_idx, .. } | IoRequest::Accept { fd, slot_idx, .. } => {
                ensure_fd_state(fd_states, fd as usize);
                install_interest(state, &mut fd_states[fd as usize], fd, slot_idx, true);
            }
            IoRequest::Write { fd, slot_idx, .. } | IoRequest::Connect { fd, slot_idx, .. } => {
                ensure_fd_state(fd_states, fd as usize);
                install_interest(state, &mut fd_states[fd as usize], fd, slot_idx, false);
            }
            IoRequest::RegisterFile { fd, slot_idx } => {
                let res = unsafe {
                    let poll = &mut *state.poll.get();
                    poll.registry().register(
                        &mut mio::unix::SourceFd(&fd),
                        mio::Token(fd as usize),
                        mio::Interest::READABLE | mio::Interest::WRITABLE,
                    )
                };
                match res {
                    Ok(()) => complete_mio_slot(state, slot_idx, fd),
                    Err(e) => {
                        let os_err = e.raw_os_error().unwrap_or(libc::EINVAL);
                        complete_mio_slot(state, slot_idx, -os_err);
                    }
                }
            }
            IoRequest::UnregisterFile {
                direct_fd_idx,
                slot_idx,
            } => {
                let _ = unsafe {
                    let poll = &mut *state.poll.get();
                    poll.registry()
                        .deregister(&mut mio::unix::SourceFd(&(direct_fd_idx as i32)))
                };
                if let Some(fd_state) = fd_states.get_mut(direct_fd_idx as usize) {
                    fd_state.reader_waker = None;
                    fd_state.writer_waker = None;
                    fd_state.reader_slot = None;
                    fd_state.writer_slot = None;
                    fd_state.registered_interest = None;
                }
                complete_mio_slot(state, slot_idx, 0);
            }
        }
    }

    /// Handle a slot whose owning `DtactIoFuture` was dropped before its op
    /// completed (see `Drop for DtactIoFuture`). Clears whichever side of the
    /// fd's interest this slot owns (if the request had already been
    /// processed into `fd_states`) and recycles the slot.
    #[cfg(not(target_os = "linux"))]
    fn cancel_mio_slot(state: &WorkerState, fd_states: &mut Vec<FdState>, slot_idx: usize) {
        let slot = &state.slots[slot_idx];
        let fd = slot.origin_fd.load(Ordering::Relaxed);

        if fd != u32::MAX {
            ensure_fd_state(fd_states, fd as usize);
            let fd_state = &mut fd_states[fd as usize];
            let mut touched = false;
            if fd_state.reader_slot == Some(slot_idx) {
                fd_state.reader_waker = None;
                fd_state.reader_slot = None;
                touched = true;
            }
            if fd_state.writer_slot == Some(slot_idx) {
                fd_state.writer_waker = None;
                fd_state.writer_slot = None;
                touched = true;
            }
            if touched {
                let interest = get_mio_interest(fd_state);
                if fd_state.registered_interest != Some(interest) {
                    let res = unsafe {
                        let poll = &mut *state.poll.get();
                        poll.registry().reregister(
                            &mut mio::unix::SourceFd(&(fd as i32)),
                            mio::Token(fd as usize),
                            interest,
                        )
                    };
                    if res.is_ok() {
                        fd_state.registered_interest = Some(interest);
                    }
                }
            }
        }

        state.free_slots.push(slot_idx as u32);
        wake_next_waiting_fiber(state);
    }

    #[cfg(not(target_os = "linux"))]
    fn process_mio_event(
        _state: &WorkerState,
        fd_states: &mut Vec<FdState>,
        fd: usize,
        readable: bool,
        writable: bool,
    ) {
        ensure_fd_state(fd_states, fd);
        let fd_state = &mut fd_states[fd];

        if readable {
            if let Some(w) = fd_state.reader_waker.take() {
                w.wake();
            }
            fd_state.reader_slot = None;
        }
        if writable {
            if let Some(w) = fd_state.writer_waker.take() {
                w.wake();
            }
            fd_state.writer_slot = None;
        }

        let interest = get_mio_interest(fd_state);
        if fd_state.registered_interest != Some(interest) {
            let res = unsafe {
                let poll = &mut *_state.poll.get();
                poll.registry().reregister(
                    &mut mio::unix::SourceFd(&(fd as i32)),
                    mio::Token(fd),
                    interest,
                )
            };
            if res.is_ok() {
                fd_state.registered_interest = Some(interest);
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn complete_mio_slot(state: &WorkerState, slot_idx: usize, res: i32) {
        let slot = &state.slots[slot_idx];
        slot.result.store(res, Ordering::Release);

        // See the matching comment in `process_linux_completion`: extract the
        // waker before publishing `completed`, so a slot reused immediately
        // after another thread observes `completed` can never have its freshly
        // installed waker clobbered by this call.
        slot.lock_waker();
        let data = slot
            .waker_data
            .swap(std::ptr::null_mut(), Ordering::Relaxed);
        let vtable = slot
            .waker_vtable
            .swap(std::ptr::null_mut(), Ordering::Relaxed);
        slot.unlock_waker();

        slot.completed.store(true, Ordering::Release);

        if !data.is_null() && !vtable.is_null() {
            let raw = RawWaker::new(data as *const (), unsafe { &*vtable });
            let w = unsafe { Waker::from_raw(raw) };
            w.wake();
        }
    }

    // =========================================================================
    // 9. DtactIoFuture INTERFACE
    // =========================================================================
    pub struct DtactIoFuture {
        pub worker_idx: usize,
        pub fd: u32,
        pub direct_fd_idx: u32,
        pub op: OpCode,
        pub buf_ptr: *mut u8,
        pub len: usize,
        pub offset: i64,
        pub addr: Option<libc::sockaddr_storage>,
        pub addr_len: libc::socklen_t,
        pub slot_idx: Option<usize>,
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
            addr: Option<libc::sockaddr_storage>,
            addr_len: libc::socklen_t,
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
            }
        }

        fn create_io_request(&self, slot_idx: usize) -> IoRequest {
            match self.op {
                OpCode::Read => IoRequest::Read {
                    fd: self.fd,
                    direct_fd_idx: self.direct_fd_idx,
                    buf_ptr: self.buf_ptr,
                    len: self.len,
                    offset: self.offset,
                    slot_idx,
                },
                OpCode::Write => IoRequest::Write {
                    fd: self.fd,
                    direct_fd_idx: self.direct_fd_idx,
                    buf_ptr: self.buf_ptr,
                    len: self.len,
                    offset: self.offset,
                    slot_idx,
                },
                OpCode::Accept => IoRequest::Accept {
                    fd: self.fd,
                    direct_fd_idx: self.direct_fd_idx,
                    slot_idx,
                },
                OpCode::Connect => IoRequest::Connect {
                    fd: self.fd,
                    direct_fd_idx: self.direct_fd_idx,
                    addr: self.addr.unwrap(),
                    addr_len: self.addr_len,
                    slot_idx,
                },
            }
        }

        #[cfg(not(target_os = "linux"))]
        fn execute_syscall(&self) -> std::io::Result<usize> {
            let res = match self.op {
                OpCode::Read => {
                    let buf_ptr = self.buf_ptr;
                    let len = self.len;
                    unsafe { libc::read(self.fd as i32, buf_ptr as *mut libc::c_void, len) }
                }
                OpCode::Write => {
                    let buf_ptr = self.buf_ptr;
                    let len = self.len;
                    unsafe { libc::write(self.fd as i32, buf_ptr as *const libc::c_void, len) }
                }
                OpCode::Accept => unsafe {
                    libc::accept(self.fd as i32, std::ptr::null_mut(), std::ptr::null_mut())
                        as isize
                },
                OpCode::Connect => {
                    let addr_ptr = &self.addr.unwrap() as *const libc::sockaddr_storage
                        as *const libc::sockaddr;
                    let res = unsafe { libc::connect(self.fd as i32, addr_ptr, self.addr_len) };
                    if res < 0 {
                        let err = std::io::Error::last_os_error();
                        if err.raw_os_error() == Some(libc::EISCONN) {
                            return Ok(0);
                        }
                        return Err(err);
                    }
                    res as isize
                }
            };

            if res < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(res as usize)
            }
        }
    }

    impl Future for DtactIoFuture {
        type Output = std::io::Result<usize>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            #[cfg(target_os = "linux")]
            {
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
                        slot.origin_fd.store(self.fd, Ordering::Relaxed);
                        // Store the raw waker details.
                        slot.lock_waker();
                        slot.waker_data
                            .store(cx.waker().data() as *mut (), Ordering::Relaxed);
                        slot.waker_vtable.store(
                            cx.waker().vtable() as *const RawWakerVTable as *mut _,
                            Ordering::Relaxed,
                        );
                        slot.unlock_waker();

                        let req = self.create_io_request(idx);
                        let q_idx = get_or_init_local_allocator().unwrap_or(0);
                        let queue = &state.queues[q_idx];

                        if queue.push(req).is_err() {
                            // Queue full — reset slot and retry next poll.
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

                        // Paired with the io-worker's Dekker-style re-check
                        // (see `run_linux_worker_loop`): this must be a
                        // SeqCst load with a fence between the queue push
                        // above and this load, otherwise the push and this
                        // load can be observed out of order (StoreLoad
                        // reorder) and we could skip the wakeup right as the
                        // io-worker is about to go to sleep without ever
                        // seeing the new queue entry — a permanent lost
                        // wakeup / deadlock.
                        fence(Ordering::SeqCst);
                        if state.is_sleeping.load(Ordering::SeqCst) {
                            unsafe {
                                let _ = libc::write(
                                    state.wake_eventfd,
                                    &1u64 as *const u64 as *const libc::c_void,
                                    8,
                                );
                            }
                        }

                        io_trace!(
                            "[dtact-io] t={} slot={} fd={} op={:?} A_submit",
                            trace_now_us(),
                            idx,
                            self.fd,
                            self.op
                        );

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
                    // Clear the waker
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
                    // Still pending — update the waker if the waker changed
                    // (e.g. the fiber migrated to a different scheduler core).
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

            #[cfg(not(target_os = "linux"))]
            {
                let res = self.execute_syscall();
                if self.slot_idx.is_some()
                    && !matches!(res, Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock)
                {
                    let state = &WORKERS.get().unwrap()[self.worker_idx];
                    state.free_slots.push(self.slot_idx.unwrap() as u32);
                    self.slot_idx = None;
                    wake_next_waiting_fiber(state);
                }

                match res {
                    Ok(n) => Poll::Ready(Ok(n)),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        let slot_idx = match self.slot_idx {
                            Some(idx) => idx,
                            None => {
                                let state = &WORKERS.get().unwrap()[self.worker_idx];
                                let idx = match state.free_slots.pop() {
                                    Some(i) => i as usize,
                                    None => {
                                        if let Some(wait_idx) = state.free_wait_slots.pop() {
                                            let wait_slot = &state.wait_slots[wait_idx as usize];
                                            wait_slot.waker_data.store(
                                                cx.waker().data() as *mut (),
                                                Ordering::Relaxed,
                                            );
                                            wait_slot.waker_vtable.store(
                                                cx.waker().vtable() as *const RawWakerVTable
                                                    as *mut _,
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
                                slot.origin_fd.store(self.fd, Ordering::Relaxed);
                                let raw = cx.waker().as_raw();
                                slot.lock_waker();
                                slot.waker_data
                                    .store(raw.data() as *mut (), Ordering::Relaxed);
                                slot.waker_vtable.store(
                                    raw.vtable() as *const RawWakerVTable as *mut _,
                                    Ordering::Relaxed,
                                );
                                slot.unlock_waker();

                                let req = self.create_io_request(idx);
                                let q_idx = get_or_init_local_allocator().unwrap_or(0);
                                let queue = &state.queues[q_idx];

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

                                fence(Ordering::SeqCst);
                                if state.is_sleeping.load(Ordering::SeqCst) {
                                    state.waker.wake();
                                }
                                self.slot_idx = Some(idx);
                                idx
                            }
                        };

                        let state = &WORKERS.get().unwrap()[self.worker_idx];
                        let slot = &state.slots[slot_idx];
                        let raw = cx.waker().as_raw();
                        let new_data = raw.data() as *mut ();
                        let new_vtable = raw.vtable() as *const RawWakerVTable as *mut _;

                        slot.lock_waker();
                        let old_data = slot.waker_data.load(Ordering::Relaxed);
                        let old_vtable = slot.waker_vtable.load(Ordering::Relaxed);
                        let mut changed = false;
                        if old_data != new_data || old_vtable != new_vtable {
                            slot.waker_data.store(new_data, Ordering::Relaxed);
                            slot.waker_vtable.store(new_vtable, Ordering::Relaxed);
                            changed = true;
                        }
                        slot.unlock_waker();

                        if changed {
                            let req = self.create_io_request(slot_idx);
                            let q_idx = get_or_init_local_allocator().unwrap_or(0);
                            let _ = state.queues[q_idx].push(req);
                            fence(Ordering::SeqCst);
                            if state.is_sleeping.load(Ordering::SeqCst) {
                                state.waker.wake();
                            }
                        }
                        Poll::Pending
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
        }
    }

    impl Drop for DtactIoFuture {
        fn drop(&mut self) {
            let Some(idx) = self.slot_idx else { return };
            let Some(state) = WORKERS.get().and_then(|w| w.get(self.worker_idx)) else {
                return;
            };

            // Clear the waker so the io-worker won't try to wake a fiber that
            // is no longer polling this future.
            let slot = &state.slots[idx];
            slot.lock_waker();
            slot.waker_data
                .store(std::ptr::null_mut(), Ordering::Relaxed);
            slot.waker_vtable
                .store(std::ptr::null_mut(), Ordering::Relaxed);
            slot.unlock_waker();

            if slot.completed.load(Ordering::Acquire) {
                // The op already finished (CQE/event observed) and nobody will
                // touch this slot again — safe to recycle right away, from any
                // thread, since it never gets accessed again after this point.
                state.free_slots.push(idx as u32);
                wake_next_waiting_fiber(state);
                return;
            }

            // The op may still be in flight (submitted to the kernel / queued
            // for the io-worker / registered with the OS reactor). We must NOT
            // touch backend state (the io_uring `ring` or the mio `poll`)
            // from here — `Drop::drop` can run on an arbitrary thread, not
            // necessarily the io-worker thread that owns that `UnsafeCell`.
            // Instead, mark the slot dropped and hand cancellation off to the
            // owning worker via `cancel_queue`, which — unlike the per-thread
            // SPSC `queues` — is safe to push to from any thread.
            slot.dropped.store(true, Ordering::Release);
            state.cancel_queue.push(idx as u32);
            fence(Ordering::SeqCst);

            #[cfg(target_os = "linux")]
            {
                if state.is_sleeping.load(Ordering::SeqCst) {
                    unsafe {
                        let _ = libc::write(
                            state.wake_eventfd,
                            &1u64 as *const u64 as *const libc::c_void,
                            8,
                        );
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                if state.is_sleeping.load(Ordering::SeqCst) {
                    state.waker.wake();
                }
            }
        }
    }

    // =========================================================================
    // 10. HIGH-LEVEL API: DtactTcpStream AND DtactTcpListener
    // =========================================================================
    pub struct DtactTcpStream {
        inner: std::net::TcpStream,
        direct_fd_idx: u32,
        worker_idx: usize,
    }

    impl DtactTcpStream {
        /// Register an existing non-blocking `TcpStream` with the dtact-io driver.
        ///
        /// Registration is **synchronous and lock-free on the hot path** — it calls
        /// `io_uring_register_files_update` directly under a per-worker mutex rather
        /// than going through the SPSC queue, which would require a spin-wait and
        /// could deadlock when called from within a dtact fiber.
        pub fn from_std(stream: std::net::TcpStream) -> std::io::Result<Self> {
            let fd = stream.as_raw_fd();
            stream.set_nonblocking(true)?;
            // Nagle's algorithm batches small writes waiting for the peer's
            // ACK; combined with the peer's delayed-ACK timer (~40-200ms on
            // Linux) this stalls exactly the small request/response traffic
            // this driver targets. Every consumer of this async driver wants
            // low latency, not bandwidth-optimised batching, so disable it
            // unconditionally rather than leaving it as a footgun.
            stream.set_nodelay(true)?;

            let num_workers = GLOBAL_CONFIG.get().map_or(1, |c| c.workers);
            let worker_idx = fd as usize % num_workers;
            let state = &WORKERS.get().unwrap()[worker_idx];

            let direct_fd_idx = register_fd_sync(state, fd)?;

            Ok(Self {
                inner: stream,
                direct_fd_idx,
                worker_idx,
            })
        }

        pub async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }

            // Try the syscall directly, exactly once, before paying for an
            // io_uring round trip — this only helps when data is already
            // available. A previous version of this function busy-spun the
            // OS thread for up to 4000 iterations (issuing the syscall every
            // 128 spins, ~31 raw syscalls) before falling back to async: on a
            // cooperative fiber scheduler that's actively harmful whenever
            // the data *isn't* ready yet (the common case for a server
            // request/response loop) — it blocks the OS thread from running
            // any other fiber and burns dozens of guaranteed-EAGAIN syscalls
            // per await instead of yielding immediately.
            let res = unsafe {
                let r = libc::read(
                    self.inner.as_raw_fd(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                );
                if r > 0 {
                    Ok(r as usize)
                } else if r == 0 {
                    Ok(0) // EOF
                } else {
                    Err(std::io::Error::last_os_error())
                }
            };

            match res {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WouldBlock {
                        return Err(e);
                    }
                }
            }

            // 100% Zerocopy, Lockless Direct path using DtactIoFuture
            DtactIoFuture {
                worker_idx: self.worker_idx,
                fd: self.inner.as_raw_fd() as u32,
                direct_fd_idx: self.direct_fd_idx,
                op: OpCode::Read,
                buf_ptr: buf.as_mut_ptr(),
                len: buf.len(),
                offset: 0,
                addr: None,
                addr_len: 0,
                slot_idx: None,
            }
            .await
            .map(|n| n.min(buf.len()))
        }

        pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }

            // One direct attempt before going async — see the comment in
            // `read()` above for why this is no longer a busy-spin loop.
            let res = unsafe {
                let r = libc::write(
                    self.inner.as_raw_fd(),
                    buf.as_ptr() as *const libc::c_void,
                    buf.len(),
                );
                if r >= 0 {
                    Ok(r as usize)
                } else {
                    Err(std::io::Error::last_os_error())
                }
            };

            match res {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WouldBlock {
                        return Err(e);
                    }
                }
            }

            DtactIoFuture {
                worker_idx: self.worker_idx,
                fd: self.inner.as_raw_fd() as u32,
                direct_fd_idx: self.direct_fd_idx,
                op: OpCode::Write,
                buf_ptr: buf.as_ptr() as *mut u8,
                len: buf.len(),
                offset: 0,
                addr: None,
                addr_len: 0,
                slot_idx: None,
            }
            .await
        }

        pub async fn connect(addr: std::net::SocketAddr) -> std::io::Result<Self> {
            let domain = match addr {
                std::net::SocketAddr::V4(_) => libc::AF_INET,
                std::net::SocketAddr::V6(_) => libc::AF_INET6,
            };
            let fd = unsafe {
                libc::socket(
                    domain,
                    libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                    0,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }

            // `from_raw_fd` takes ownership; the socket is closed on Drop.
            let stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
            // See the comment in `from_std` — Nagle + the peer's delayed ACK
            // otherwise stalls small request/response traffic by ~40-200ms.
            stream.set_nodelay(true)?;
            let num_workers = GLOBAL_CONFIG.get().map_or(1, |c| c.workers);
            let worker_idx = fd as usize % num_workers;
            let state = &WORKERS.get().unwrap()[worker_idx];

            // register_fd_sync returns u32::MAX (raw-fd mode) — no queue, no spin,
            // no deadlock risk when called from within a dtact fiber.
            let direct_fd_idx = register_fd_sync(state, fd)?;

            let (libc_addr, addr_len) = socket_addr_to_libc(addr);

            // Try direct connect first!
            let connect_res = unsafe {
                libc::connect(
                    fd,
                    &libc_addr as *const libc::sockaddr_storage as *const libc::sockaddr,
                    addr_len,
                )
            };
            if connect_res == 0 {
                return Ok(Self {
                    inner: stream,
                    direct_fd_idx,
                    worker_idx,
                });
            }
            let err = std::io::Error::last_os_error();
            #[cfg(target_os = "windows")]
            let is_in_progress = err.raw_os_error()
                == Some(windows_sys::Win32::Networking::WinSock::WSAEWOULDBLOCK as i32);
            #[cfg(not(target_os = "windows"))]
            let is_in_progress = err.raw_os_error() == Some(libc::EINPROGRESS);

            if !is_in_progress {
                return Err(err);
            }

            // One non-blocking `poll` check before going async — see the
            // comment in `read()` above for why this is no longer a
            // busy-spin loop (connect latency is dominated by the network
            // round trip anyway, so spinning here never helps).
            let mut pollfd = libc::pollfd {
                fd,
                events: libc::POLLOUT,
                revents: 0,
            };
            let poll_res = unsafe { libc::poll(&mut pollfd, 1, 0) };
            if poll_res > 0 {
                if (pollfd.revents & libc::POLLOUT) != 0 {
                    let mut err_code: libc::c_int = 0;
                    let mut err_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                    let sockopt_res = unsafe {
                        libc::getsockopt(
                            fd,
                            libc::SOL_SOCKET,
                            libc::SO_ERROR,
                            &mut err_code as *mut libc::c_int as *mut libc::c_void,
                            &mut err_len,
                        )
                    };
                    if sockopt_res == 0 && err_code == 0 {
                        return Ok(Self {
                            inner: stream,
                            direct_fd_idx,
                            worker_idx,
                        });
                    } else {
                        let os_err = if err_code != 0 {
                            err_code
                        } else {
                            libc::ECONNREFUSED
                        };
                        return Err(std::io::Error::from_raw_os_error(os_err));
                    }
                } else if (pollfd.revents & (libc::POLLERR | libc::POLLHUP)) != 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "connect failed",
                    ));
                }
            }

            let connect_res = DtactIoFuture {
                worker_idx,
                fd: fd as u32,
                direct_fd_idx,
                op: OpCode::Connect,
                buf_ptr: std::ptr::null_mut(),
                len: 0,
                offset: 0,
                addr: Some(libc_addr),
                addr_len,
                slot_idx: None,
            }
            .await;

            match connect_res {
                Ok(_) => Ok(Self {
                    inner: stream,
                    direct_fd_idx,
                    worker_idx,
                }),
                Err(e) => Err(e),
            }
        }
    }

    impl Drop for DtactTcpStream {
        fn drop(&mut self) {
            if let Some(workers) = WORKERS.get()
                && let Some(state) = workers.get(self.worker_idx)
            {
                unregister_fd_sync(state, self.direct_fd_idx);
            }
        }
    }

    pub struct DtactTcpListener {
        inner: std::net::TcpListener,
        direct_fd_idx: u32,
        worker_idx: usize,
    }

    impl DtactTcpListener {
        pub fn from_std(listener: std::net::TcpListener) -> std::io::Result<Self> {
            let fd = listener.as_raw_fd();
            listener.set_nonblocking(true)?;

            let num_workers = GLOBAL_CONFIG.get().map_or(1, |c| c.workers);
            let worker_idx = fd as usize % num_workers;
            let state = &WORKERS.get().unwrap()[worker_idx];

            let direct_fd_idx = register_fd_sync(state, fd)?;

            Ok(Self {
                inner: listener,
                direct_fd_idx,
                worker_idx,
            })
        }

        pub async fn accept(&self) -> std::io::Result<(DtactTcpStream, std::net::SocketAddr)> {
            // One direct attempt before going async — see the comment in
            // `read()` above for why this is no longer a busy-spin loop. An
            // accept() in particular has no reason to expect a pending
            // connection at any given instant, so spinning here was pure
            // waste on every call that didn't already have one queued.
            let res = unsafe {
                let mut addr: libc::sockaddr_storage = std::mem::zeroed();
                let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                let r = libc::accept(
                    self.inner.as_raw_fd(),
                    &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
                    &mut len,
                );
                if r >= 0 {
                    Ok((r, addr, len))
                } else {
                    Err(std::io::Error::last_os_error())
                }
            };

            match res {
                Ok((client_fd, addr, len)) => {
                    // Parse peer addr directly from the sockaddr we already have —
                    // no extra getpeername() syscall needed.
                    let peer_addr = sockaddr_storage_to_socketaddr(&addr, len);
                    // Set nonblocking on the client fd.
                    unsafe { libc::fcntl(client_fd, libc::F_SETFL, libc::O_NONBLOCK) };
                    let stream = unsafe { std::net::TcpStream::from_raw_fd(client_fd) };
                    let client_stream = DtactTcpStream::from_std(stream)?;
                    return Ok((client_stream, peer_addr));
                }
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WouldBlock {
                        return Err(e);
                    }
                }
            }

            let res = DtactIoFuture {
                worker_idx: self.worker_idx,
                fd: self.inner.as_raw_fd() as u32,
                direct_fd_idx: self.direct_fd_idx,
                op: OpCode::Accept,
                buf_ptr: std::ptr::null_mut(),
                len: 0,
                offset: 0,
                addr: None,
                addr_len: 0,
                slot_idx: None,
            }
            .await?;

            let client_fd = res as RawFd;
            // Set nonblocking on the accepted fd.
            unsafe { libc::fcntl(client_fd, libc::F_SETFL, libc::O_NONBLOCK) };
            let stream = unsafe { std::net::TcpStream::from_raw_fd(client_fd) };
            let peer_addr = stream.peer_addr()?;
            let client_stream = DtactTcpStream::from_std(stream)?;
            Ok((client_stream, peer_addr))
        }
    }

    impl Drop for DtactTcpListener {
        fn drop(&mut self) {
            if let Some(workers) = WORKERS.get()
                && let Some(state) = workers.get(self.worker_idx)
            {
                unregister_fd_sync(state, self.direct_fd_idx);
            }
        }
    }

    // =========================================================================
    // 11. FILE-REGISTRATION HELPERS
    // =========================================================================

    /// Register `fd` with the dtact-io driver.
    ///
    /// We intentionally skip io_uring fixed-file registration here.
    /// `register_files_update` (io_uring_register) returns EBUSY under SQPOLL
    /// when called concurrently with the io worker's submit/wait loop, and
    /// serialising it with a mutex would either deadlock (if called from inside
    /// a fiber) or severely harm throughput.  Fixed files provide only ~5%
    /// throughput gain; correctness takes priority.
    ///
    /// `u32::MAX` is the sentinel the io-path already uses for "raw fd" mode.
    fn register_fd_sync(_state: &WorkerState, _fd: RawFd) -> std::io::Result<u32> {
        Ok(u32::MAX)
    }

    /// Nothing to release when we aren't using fixed files.
    fn unregister_fd_sync(_state: &WorkerState, _direct_fd_idx: u32) {}

    // =========================================================================
    // 12. HELPER CONVERTER FUNCTIONS
    // =========================================================================
    fn socket_addr_to_libc(
        addr: std::net::SocketAddr,
    ) -> (libc::sockaddr_storage, libc::socklen_t) {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let len = match addr {
            std::net::SocketAddr::V4(a) => {
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: a.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(a.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &sin as *const libc::sockaddr_in as *const u8,
                        &mut storage as *mut libc::sockaddr_storage as *mut u8,
                        std::mem::size_of::<libc::sockaddr_in>(),
                    );
                }
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            std::net::SocketAddr::V6(a) => {
                let sin6 = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: a.port().to_be(),
                    sin6_flowinfo: a.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: a.ip().octets(),
                    },
                    sin6_scope_id: a.scope_id(),
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &sin6 as *const libc::sockaddr_in6 as *const u8,
                        &mut storage as *mut libc::sockaddr_storage as *mut u8,
                        std::mem::size_of::<libc::sockaddr_in6>(),
                    );
                }
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        };
        (storage, len)
    }

    /// Parse a `libc::sockaddr_storage` (returned by `libc::accept`) into a
    /// `std::net::SocketAddr` without issuing an extra `getpeername` syscall.
    fn sockaddr_storage_to_socketaddr(
        storage: &libc::sockaddr_storage,
        _len: libc::socklen_t,
    ) -> std::net::SocketAddr {
        match storage.ss_family as libc::c_int {
            libc::AF_INET => {
                // Safety: ss_family confirmed to be AF_INET.
                let sin = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
                let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                let port = u16::from_be(sin.sin_port);
                std::net::SocketAddr::V4(std::net::SocketAddrV4::new(ip, port))
            }
            libc::AF_INET6 => {
                // Safety: ss_family confirmed to be AF_INET6.
                let sin6 = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
                let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                let port = u16::from_be(sin6.sin6_port);
                std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
                    ip,
                    port,
                    sin6.sin6_flowinfo,
                    sin6.sin6_scope_id,
                ))
            }
            _ => {
                panic!("Unsupported address family: {}", storage.ss_family);
            }
        }
    }
}

#[cfg(all(feature = "native", unix))]
pub use native_impl::*;

#[cfg(all(feature = "native", windows))]
mod windows_impl {
    use super::*;
    use std::net::SocketAddr;
    use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicUsize, Ordering, fence};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Networking::WinSock::{SOCKADDR, SOCKADDR_STORAGE, SOCKET, WSABUF};
    use windows_sys::Win32::System::IO::{
        CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY,
        PostQueuedCompletionStatus,
    };

    // =========================================================================
    // Shared lock-free primitives — deliberately duplicated from
    // `native_impl` rather than hoisted into a common module: this
    // keeps the already-verified Unix backend completely untouched while
    // this Windows backend is developed and iterated on independently.
    // =========================================================================
    include!("windows_primitives.rs");

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

    fn get_extension_fn<F: Copy>(
        socket: SOCKET,
        guid: &windows_sys::core::GUID,
    ) -> std::io::Result<F> {
        assert_eq!(std::mem::size_of::<F>(), std::mem::size_of::<usize>());
        let mut fn_ptr: usize = 0;
        let mut bytes_returned: u32 = 0;
        let res = unsafe {
            windows_sys::Win32::Networking::WinSock::WSAIoctl(
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

            let iocp = unsafe {
                CreateIoCompletionPort(
                    windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE,
                    std::ptr::null_mut(),
                    0,
                    1,
                )
            };
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
                    windows_sys::Win32::Networking::WinSock::WSARecv(
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
                    windows_sys::Win32::Networking::WinSock::WSASend(
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
        let err = unsafe { windows_sys::Win32::Networking::WinSock::WSAGetLastError() };
        if err == windows_sys::Win32::Networking::WinSock::WSA_IO_PENDING {
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
        let res = -err
            .raw_os_error()
            .unwrap_or(windows_sys::Win32::Networking::WinSock::WSAEINVAL);
        finish_slot(state, slot_idx, res);
    }

    fn process_windows_completion(state: &WorkerState, ov_ptr: *mut IoOverlapped) {
        let ov = unsafe { Box::from_raw(ov_ptr) };
        let slot_idx = ov.slot_idx;
        let socket = ov.issuing_socket as SOCKET;

        let mut transferred: u32 = 0;
        let mut flags: u32 = 0;
        let ok = unsafe {
            windows_sys::Win32::Networking::WinSock::WSAGetOverlappedResult(
                socket,
                &ov.overlapped as *const OVERLAPPED as *mut OVERLAPPED,
                &mut transferred,
                0,
                &mut flags,
            )
        };

        let res: i32 = if ok == 0 {
            let err = unsafe { windows_sys::Win32::Networking::WinSock::WSAGetLastError() };
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
                            windows_sys::Win32::Networking::WinSock::WSASocketW(
                                windows_sys::Win32::Networking::WinSock::AF_INET as i32,
                                windows_sys::Win32::Networking::WinSock::SOCK_STREAM as i32,
                                0,
                                std::ptr::null(),
                                0,
                                windows_sys::Win32::Networking::WinSock::WSA_FLAG_OVERLAPPED,
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
                            PostQueuedCompletionStatus(
                                state.iocp,
                                0,
                                WAKE_KEY,
                                std::ptr::null_mut(),
                            );
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
                SocketAddr::V4(_) => windows_sys::Win32::Networking::WinSock::AF_INET,
                SocketAddr::V6(_) => windows_sys::Win32::Networking::WinSock::AF_INET6,
            };
            let raw_socket = unsafe {
                windows_sys::Win32::Networking::WinSock::WSASocketW(
                    domain as i32,
                    windows_sys::Win32::Networking::WinSock::SOCK_STREAM as i32,
                    0,
                    std::ptr::null(),
                    0,
                    windows_sys::Win32::Networking::WinSock::WSA_FLAG_OVERLAPPED,
                )
            };
            if raw_socket == windows_sys::Win32::Networking::WinSock::INVALID_SOCKET {
                return Err(std::io::Error::last_os_error());
            }

            // ConnectEx requires the socket to already be bound.
            let any_addr = match addr {
                SocketAddr::V4(_) => socket_addr_to_win(&"0.0.0.0:0".parse().unwrap()),
                SocketAddr::V6(_) => socket_addr_to_win(&"[::]:0".parse().unwrap()),
            };
            let (bind_addr, bind_len) = any_addr;
            let bind_res = unsafe {
                windows_sys::Win32::Networking::WinSock::bind(
                    raw_socket,
                    &bind_addr as *const SOCKADDR_STORAGE as *const SOCKADDR,
                    bind_len,
                )
            };
            if bind_res != 0 {
                let e = std::io::Error::last_os_error();
                unsafe { windows_sys::Win32::Networking::WinSock::closesocket(raw_socket) };
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
                        windows_sys::Win32::Networking::WinSock::setsockopt(
                            raw_socket,
                            windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
                            windows_sys::Win32::Networking::WinSock::SO_UPDATE_CONNECT_CONTEXT,
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
                windows_sys::Win32::Networking::WinSock::setsockopt(
                    accept_socket,
                    windows_sys::Win32::Networking::WinSock::SOL_SOCKET,
                    windows_sys::Win32::Networking::WinSock::SO_UPDATE_ACCEPT_CONTEXT,
                    &listen_socket as *const usize as *const u8,
                    std::mem::size_of::<usize>() as i32,
                );
            }

            let stream =
                unsafe { std::net::TcpStream::from_raw_socket(accept_socket as RawSocket) };
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
                let sin = windows_sys::Win32::Networking::WinSock::SOCKADDR_IN {
                    sin_family: windows_sys::Win32::Networking::WinSock::AF_INET as u16,
                    sin_port: a.port().to_be(),
                    sin_addr: windows_sys::Win32::Networking::WinSock::IN_ADDR {
                        S_un: windows_sys::Win32::Networking::WinSock::IN_ADDR_0 {
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
                let sin6 = windows_sys::Win32::Networking::WinSock::SOCKADDR_IN6 {
                    sin6_family: windows_sys::Win32::Networking::WinSock::AF_INET6 as u16,
                    sin6_port: a.port().to_be(),
                    sin6_flowinfo: a.flowinfo(),
                    sin6_addr: windows_sys::Win32::Networking::WinSock::IN6_ADDR {
                        u: windows_sys::Win32::Networking::WinSock::IN6_ADDR_0 {
                            Byte: a.ip().octets(),
                        },
                    },
                    Anonymous: windows_sys::Win32::Networking::WinSock::SOCKADDR_IN6_0 {
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
}

#[cfg(all(feature = "native", windows))]
pub use windows_impl::*;

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_impl {
    use super::*;

    // The `Runtime` itself is wrapped in a Mutex<Option<…>> purely so
    // `shutdown_runtime()` can drop it rather than leaking it until process
    // exit — that Mutex is touched exactly twice (init, shutdown), never on
    // the hot path. `runtime_handle()` used to lock it on *every single*
    // poll of every in-flight op (a `Handle` clone is cheap, but the Mutex
    // acquisition serialises all fibers doing I/O on one global lock); the
    // `Handle` is now cached separately in its own lock-free `OnceLock` so
    // reading it never contends with anything.
    static TOKIO_RUNTIME: std::sync::OnceLock<std::sync::Mutex<Option<tokio::runtime::Runtime>>> =
        std::sync::OnceLock::new();
    static TOKIO_HANDLE: std::sync::OnceLock<tokio::runtime::Handle> = std::sync::OnceLock::new();

    fn runtime_handle() -> tokio::runtime::Handle {
        TOKIO_HANDLE.get().cloned().expect(
            "dtact-io tokio runtime not initialised — \
                 call dtact_io::init_runtime() before performing any I/O",
        )
    }

    // ── Public initialisation API ──────────────────────────────────────────

    /// Initialise the backing Tokio runtime.
    ///
    /// Matches the signature of the native driver so call-sites can
    /// switch drivers with a single feature flag.  The extra parameters
    /// (`buffer_pool_size`, `chunk_size`, `pin_cpus`, `ring_depth`) are
    /// accepted for API compatibility but are ignored by the Tokio backend.
    pub fn init_runtime(
        workers: usize,
        _buffer_pool_size: usize,
        _chunk_size: usize,
        _pin_cpus: &[usize],
        _ring_depth: u32,
    ) {
        TOKIO_RUNTIME.get_or_init(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers.max(1))
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime");
            let _ = TOKIO_HANDLE.set(rt.handle().clone());
            std::sync::Mutex::new(Some(rt))
        });
    }

    /// Shorthand initialiser — uses `workers` Tokio worker threads.
    ///
    /// Equivalent to `init_runtime(workers, 0, 0, &[], 0)`.
    pub fn init(workers: usize) {
        init_runtime(workers, 0, 0, &[], 0);
    }

    /// Gracefully shut down the Tokio runtime, waiting for all spawned
    /// tasks to complete.
    pub fn shutdown_runtime() {
        if let Some(cell) = TOKIO_RUNTIME.get()
            && let Ok(mut guard) = cell.lock()
            && let Some(rt) = guard.take()
        {
            rt.shutdown_background();
        }
    }

    /// Obtain a handle to the underlying Tokio runtime.
    ///
    /// Useful for spawning Tokio tasks from within a dtact fiber.
    ///
    /// # Panics
    /// Panics if `init_runtime()` / `init()` has not been called.
    pub fn get_runtime_handle() -> tokio::runtime::Handle {
        runtime_handle()
    }

    #[doc(hidden)]
    pub struct TokioFutureWrapper<F> {
        inner: F,
    }

    impl<F: Future> Future for TokioFutureWrapper<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let _guard = runtime_handle().enter();
            let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
            inner.poll(cx)
        }
    }

    // =========================================================================
    // OPCODES & DtactIoFuture  (tokio backend)
    // =========================================================================
    //
    // This raw-fd primitive is Unix-only: it wraps `tokio::io::unix::AsyncFd`,
    // which does not exist on Windows (Windows has no readiness-based fd
    // polling model for sockets/pipes — tokio's Windows reactor is IOCP-based
    // and only exposed through the higher-level `TcpStream`/`TcpListener`
    // types). `DtactTcpStream`/`DtactTcpListener` below already ride on top of
    // `tokio::net`, so they work cross-platform without this type; only the
    // low-level `DtactIoFuture`/`OpCode` API is unavailable on Windows for now.
    #[cfg(unix)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum OpCode {
        Read,
        Write,
        Accept,
        Connect,
    }

    /// Tokio-backend equivalent of the native `DtactIoFuture`.
    ///
    /// Accepts the same public fields as the native variant so
    /// call-sites compile without change when switching backends.
    /// Internally it wraps the raw fd in a `tokio::io::unix::AsyncFd`
    /// (registered with the tokio reactor) and issues direct `libc`
    /// syscalls when the fd becomes ready.
    ///
    /// `worker_idx`, `direct_fd_idx`, and `slot_idx` are present for API
    /// compatibility only and are ignored by this backend.
    #[cfg(unix)]
    pub struct DtactIoFuture {
        pub worker_idx: usize,
        pub fd: u32,
        pub direct_fd_idx: u32,
        pub op: OpCode,
        pub buf_ptr: *mut u8,
        pub len: usize,
        pub offset: i64,
        pub addr: Option<libc::sockaddr_storage>,
        pub addr_len: libc::socklen_t,
        pub slot_idx: Option<usize>,
        // Internal: lazily created on the first WouldBlock.
        async_fd: Option<tokio::io::unix::AsyncFd<std::os::unix::io::RawFd>>,
    }

    #[cfg(unix)]
    unsafe impl Send for DtactIoFuture {}
    #[cfg(unix)]
    unsafe impl Sync for DtactIoFuture {}

    #[cfg(unix)]
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
            addr: Option<libc::sockaddr_storage>,
            addr_len: libc::socklen_t,
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
                async_fd: None,
            }
        }

        /// Attempt the underlying syscall once, returning the byte count or an
        /// error (including `WouldBlock` / `EAGAIN`).
        #[inline]
        fn try_syscall(
            fd: std::os::unix::io::RawFd,
            op: OpCode,
            buf_ptr: *mut u8,
            len: usize,
            addr: *const libc::sockaddr_storage,
            addr_len: libc::socklen_t,
        ) -> std::io::Result<usize> {
            let r = match op {
                OpCode::Read => unsafe { libc::read(fd, buf_ptr as *mut libc::c_void, len) },
                OpCode::Write => unsafe { libc::write(fd, buf_ptr as *const libc::c_void, len) },
                OpCode::Accept => unsafe {
                    libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) as isize
                },
                OpCode::Connect => {
                    // Check SO_ERROR first to see if a previous async connect attempt completed with an error.
                    let mut err: libc::c_int = 0;
                    let mut err_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                    let r = unsafe {
                        libc::getsockopt(
                            fd,
                            libc::SOL_SOCKET,
                            libc::SO_ERROR,
                            &mut err as *mut libc::c_int as *mut libc::c_void,
                            &mut err_len,
                        )
                    };
                    if r == 0 && err != 0 {
                        return Err(std::io::Error::from_raw_os_error(err));
                    }

                    let r = unsafe { libc::connect(fd, addr as *const libc::sockaddr, addr_len) };
                    if r < 0 {
                        let e = std::io::Error::last_os_error();
                        let os_err = e.raw_os_error();
                        if os_err == Some(libc::EISCONN) {
                            return Ok(0);
                        }
                        return Err(e);
                    }
                    return Ok(0);
                }
            };
            if r < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(r as usize)
            }
        }

        #[inline]
        fn is_blocking_error(e: &std::io::Error) -> bool {
            let kind = e.kind();
            kind == std::io::ErrorKind::WouldBlock
                || e.raw_os_error() == Some(libc::EINPROGRESS)
                || e.raw_os_error() == Some(libc::EALREADY)
                || e.raw_os_error() == Some(libc::EINTR)
        }
    }

    #[cfg(unix)]
    impl Future for DtactIoFuture {
        type Output = std::io::Result<usize>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // Always enter the tokio runtime context so AsyncFd can register
            // with the reactor even when polled from a dtact fiber.
            let _guard = runtime_handle().enter();

            // SAFETY: DtactIoFuture is !Unpin only through PhantomPinned; the
            // fields we mutate here (async_fd) are not structurally pinned.
            let this = unsafe { self.get_unchecked_mut() };

            let fd = this.fd as std::os::unix::io::RawFd;
            let op = this.op;
            let buf_ptr = this.buf_ptr;
            let len = this.len;
            let addr_ptr: *const libc::sockaddr_storage = this
                .addr
                .as_ref()
                .map_or(std::ptr::null(), |a| a as *const _);
            let addr_len = this.addr_len;

            // ── Phase 1: first attempt, no registration yet ─────────────────
            if this.async_fd.is_none() {
                match Self::try_syscall(fd, op, buf_ptr, len, addr_ptr, addr_len) {
                    Ok(n) => return Poll::Ready(Ok(n)),
                    Err(ref e) if Self::is_blocking_error(e) => {
                        // Register with the tokio reactor.
                        match tokio::io::unix::AsyncFd::new(fd) {
                            Ok(afd) => this.async_fd = Some(afd),
                            Err(e) => return Poll::Ready(Err(e)),
                        }
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }

            // ── Phase 2: wait for reactor readiness then retry ───────────────
            let is_read_op = matches!(op, OpCode::Read | OpCode::Accept);
            let afd = this.async_fd.as_ref().unwrap();

            let mut guard = if is_read_op {
                match afd.poll_read_ready(cx) {
                    Poll::Ready(Ok(g)) => g,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                match afd.poll_write_ready(cx) {
                    Poll::Ready(Ok(g)) => g,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            };

            // Retry the syscall now that the fd is reportedly ready.
            match Self::try_syscall(fd, op, buf_ptr, len, addr_ptr, addr_len) {
                Ok(n) => Poll::Ready(Ok(n)),
                Err(ref e) if Self::is_blocking_error(e) => {
                    // Spurious wakeup — clear the readiness flag so the reactor
                    // will re-arm and we'll be polled again when truly ready.
                    guard.clear_ready();
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        }
    }

    #[cfg(unix)]
    impl Drop for DtactIoFuture {
        fn drop(&mut self) {
            // Dropping async_fd deregisters the fd from the reactor automatically.
            // We do NOT close the fd — ownership remains with DtactTcpStream.
            drop(self.async_fd.take());
        }
    }

    pub struct DtactTcpStream {
        inner: tokio::net::TcpStream,
    }

    impl DtactTcpStream {
        pub fn from_std(stream: std::net::TcpStream) -> std::io::Result<Self> {
            stream.set_nonblocking(true)?;
            // See the equivalent comment on the native backend's
            // `from_std` — Nagle + delayed ACK stalls small request/response
            // traffic by tens to hundreds of milliseconds.
            stream.set_nodelay(true)?;
            let _guard = runtime_handle().enter();
            let inner = tokio::net::TcpStream::from_std(stream)?;
            Ok(Self { inner })
        }

        pub async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
            loop {
                match self.inner.try_read(buf) {
                    Ok(n) => return Ok(n),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                let fut = self.inner.readable();
                TokioFutureWrapper { inner: fut }.await?;
            }
        }

        pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
            loop {
                match self.inner.try_write(buf) {
                    Ok(n) => return Ok(n),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                let fut = self.inner.writable();
                TokioFutureWrapper { inner: fut }.await?;
            }
        }

        pub async fn connect(addr: std::net::SocketAddr) -> std::io::Result<Self> {
            let handle = runtime_handle();
            // Build the future inside the runtime context, then drop the guard before awaiting.
            let fut = {
                let _guard = handle.enter();
                tokio::net::TcpStream::connect(addr)
            };
            let inner = TokioFutureWrapper { inner: fut }.await?;
            inner.set_nodelay(true)?;
            Ok(Self { inner })
        }
    }

    pub struct DtactTcpListener {
        inner: tokio::net::TcpListener,
    }

    impl DtactTcpListener {
        pub fn from_std(listener: std::net::TcpListener) -> std::io::Result<Self> {
            listener.set_nonblocking(true)?;
            let _guard = runtime_handle().enter();
            let inner = tokio::net::TcpListener::from_std(listener)?;
            Ok(Self { inner })
        }

        pub async fn accept(&self) -> std::io::Result<(DtactTcpStream, std::net::SocketAddr)> {
            // Build the future while inside the runtime context, drop the guard before awaiting
            // so the future remains Send (EnterGuard is !Send).
            let fut = {
                let _guard = runtime_handle().enter();
                self.inner.accept()
            };
            let (stream, addr) = TokioFutureWrapper { inner: fut }.await?;
            stream.set_nodelay(true)?;
            Ok((DtactTcpStream { inner: stream }, addr))
        }
    }

    // =========================================================================
    // COMPAT: convert DtactTcpStream to futures-io / tokio AsyncRead+AsyncWrite
    // =========================================================================

    /// Wraps a `DtactTcpStream` to implement standard async I/O traits:
    /// - `futures_io::AsyncRead` / `futures_io::AsyncWrite`
    /// - `tokio::io::AsyncRead`  / `tokio::io::AsyncWrite`
    pub struct DtactCompat<T>(T);

    impl<T> DtactCompat<T> {
        /// Wrap `inner` in a compat adapter.
        pub fn new(inner: T) -> Self {
            Self(inner)
        }

        /// Unwrap back to the original type.
        pub fn into_inner(self) -> T {
            self.0
        }

        /// Shared reference to the wrapped value.
        pub fn get_ref(&self) -> &T {
            &self.0
        }

        /// Exclusive reference to the wrapped value.
        pub fn get_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    /// Extension trait: call `.compat()` on a `DtactTcpStream` to obtain a
    /// [`DtactCompat`] adapter that implements `AsyncRead`/`AsyncWrite`.
    pub trait DtactCompatExt: Sized {
        fn compat(self) -> DtactCompat<Self>;
    }

    impl DtactCompatExt for DtactTcpStream {
        fn compat(self) -> DtactCompat<Self> {
            DtactCompat(self)
        }
    }

    #[cfg(unix)]
    impl DtactCompatExt for DtactIoFuture {
        fn compat(self) -> DtactCompat<Self> {
            DtactCompat(self)
        }
    }

    impl<F: Future> Future for DtactCompat<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
            inner.poll(cx)
        }
    }

    // ── futures-io ──────────────────────────────────────────────────────────

    impl futures_io::AsyncRead for DtactCompat<DtactTcpStream> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            loop {
                match this.0.inner.try_read(buf) {
                    Ok(n) => return Poll::Ready(Ok(n)), // 0 == EOF, bubble up
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                match this.0.inner.poll_read_ready(cx) {
                    Poll::Ready(Ok(())) => {} // re-try try_read
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    impl futures_io::AsyncWrite for DtactCompat<DtactTcpStream> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            loop {
                match this.0.inner.try_write(buf) {
                    Ok(n) => return Poll::Ready(Ok(n)),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                match this.0.inner.poll_write_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            // TCP has no user-visible flush; writes go directly to the kernel buffer.
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // ── tokio::io ───────────────────────────────────────────────────────────

    impl tokio::io::AsyncRead for DtactCompat<DtactTcpStream> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            loop {
                let unfilled = buf.initialize_unfilled();
                match this.0.inner.try_read(unfilled) {
                    Ok(0) => return Poll::Ready(Ok(())), // EOF
                    Ok(n) => {
                        buf.advance(n);
                        return Poll::Ready(Ok(()));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                match this.0.inner.poll_read_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    impl tokio::io::AsyncWrite for DtactCompat<DtactTcpStream> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            loop {
                match this.0.inner.try_write(buf) {
                    Ok(n) => return Poll::Ready(Ok(n)),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
                match this.0.inner.poll_write_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
pub use tokio_impl::*;
