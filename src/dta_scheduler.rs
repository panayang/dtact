use alloc::vec::Vec;
#[allow(unused_imports)]
use core::arch::asm;
use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};

/// Task Index used for Zero-Copy passing within the `ContextPool`.
pub type TaskIndex = u32;

/// Number of tasks in a single `TaskChunk`.
pub const CHUNK_SIZE: usize = 32;

/// Capacity of a single core-to-core mailbox.
/// MUST be a power of two for bitwise masking.
pub const MAILBOX_CAPACITY: usize = 1024;
/// Mask for mailbox index wrap-around.
pub const MAILBOX_MASK: usize = MAILBOX_CAPACITY - 1;

/// Capacity of a worker's local execution queue.
/// Sized to exactly hold the max queue without global locks.
pub const LOCAL_QUEUE_CAPACITY: usize = 131_072;
/// Mask for local queue index wrap-around.
pub const LOCAL_QUEUE_MASK: usize = LOCAL_QUEUE_CAPACITY - 1;

/// Batch Ownership Transfer Chunk.
///
/// A chunk of 32 task indices, transferred in a single atomic pointer exchange
/// to minimize coherency traffic across the P2P mesh.
#[derive(Debug, Clone, Copy)]
pub struct TaskChunk {
    /// Array of task indices in this chunk.
    pub tasks: [TaskIndex; CHUNK_SIZE],
    /// Number of active tasks in this chunk.
    pub count: usize,
}

impl Default for TaskChunk {
    #[inline(always)]
    fn default() -> Self {
        Self {
            tasks: [0; CHUNK_SIZE],
            count: 0,
        }
    }
}

/// Helper for Huge Page Allocation to eliminate TLB Misses.
///
/// Manages page-aligned memory regions that utilize 2MB or 1GB huge pages
/// (where supported by the OS) to maximize memory throughput.
#[allow(dead_code)]
pub struct HugeBuffer<T> {
    /// Pointer to the allocated memory.
    ptr: *mut T,
    size_bytes: usize,
    is_mmap: bool,
}

unsafe impl<T> Send for HugeBuffer<T> {}
unsafe impl<T> Sync for HugeBuffer<T> {}

impl<T> Default for HugeBuffer<T> {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl<T> HugeBuffer<T> {
    /// Allocates a new `HugeBuffer` using OS-specific huge page primitives.
    ///
    /// # Panics
    /// Panics if the OS fails to allocate memory.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let size_bytes = core::mem::size_of::<T>();

        #[cfg(unix)]
        unsafe {
            let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
            if size_bytes >= 2 * 1024 * 1024 {
                flags |= 0x40000; // MAP_HUGETLB
            }
            let ptr = libc::mmap(
                core::ptr::null_mut(),
                size_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                -1,
                0,
            );
            if ptr == libc::MAP_FAILED {
                // Fallback to aligned std::alloc to prevent mmap exhaustion on QEMU/aarch64
                let layout = std::alloc::Layout::from_size_align(size_bytes, 64).unwrap();
                let alloc_ptr = std::alloc::alloc_zeroed(layout);
                assert!(!alloc_ptr.is_null(), "HugeBuffer std::alloc failed");
                Self {
                    ptr: alloc_ptr.cast::<T>(),
                    size_bytes,
                    is_mmap: false,
                }
            } else {
                core::ptr::write_bytes(ptr, 0, size_bytes);
                Self {
                    ptr: ptr.cast::<T>(),
                    size_bytes,
                    is_mmap: true,
                }
            }
        }

        #[cfg(windows)]
        unsafe {
            use windows_sys::Win32::System::Memory;
            #[cfg(feature = "windows-root")]
            {
                let mut ptr = Memory::VirtualAlloc(
                    core::ptr::null_mut(),
                    size_bytes,
                    Memory::MEM_RESERVE | Memory::MEM_COMMIT | Memory::MEM_LARGE_PAGES,
                    Memory::PAGE_READWRITE,
                );
                if ptr.is_null() {
                    ptr = Memory::VirtualAlloc(
                        core::ptr::null_mut(),
                        size_bytes,
                        Memory::MEM_RESERVE | Memory::MEM_COMMIT,
                        Memory::PAGE_READWRITE,
                    );
                    assert!(!ptr.is_null(), "HugeBuffer VirtualAlloc failed");
                }
                Self {
                    ptr: ptr.cast::<T>(),
                    size_bytes,
                    is_mmap: false,
                }
            }
            #[cfg(not(feature = "windows-root"))]
            {
                let ptr = Memory::VirtualAlloc(
                    core::ptr::null_mut(),
                    size_bytes,
                    Memory::MEM_RESERVE | Memory::MEM_COMMIT,
                    Memory::PAGE_READWRITE,
                );
                assert!(!ptr.is_null(), "HugeBuffer VirtualAlloc failed");
                Self {
                    ptr: ptr as *mut T,
                    size_bytes,
                    is_mmap: false,
                }
            }
        }
    }
}

impl<T> Drop for HugeBuffer<T> {
    #[inline(always)]
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            if self.is_mmap {
                libc::munmap(self.ptr.cast::<libc::c_void>(), self.size_bytes);
            } else {
                let layout = std::alloc::Layout::from_size_align(self.size_bytes, 64).unwrap();
                std::alloc::dealloc(self.ptr.cast::<u8>(), layout);
            }
        }
        #[cfg(windows)]
        unsafe {
            windows_sys::Win32::System::Memory::VirtualFree(
                self.ptr.cast::<core::ffi::c_void>(),
                0,
                windows_sys::Win32::System::Memory::MEM_RELEASE,
            );
        }
    }
}

/// Single-Producer Single-Consumer (SPSC) Queue for the P2P Mesh Mailbox.
///
/// Aligned to 64 bytes to prevent false sharing between sender and receiver cores.
#[repr(align(64))]
pub struct Mailbox {
    pub head: AtomicUsize,
    _pad1: [u8; 64 - core::mem::size_of::<AtomicUsize>()],

    pub tail: AtomicUsize,
    _pad2: [u8; 64 - core::mem::size_of::<AtomicUsize>()],

    pub buffer: HugeBuffer<UnsafeCell<[TaskChunk; MAILBOX_CAPACITY]>>,
}

unsafe impl Sync for Mailbox {}
unsafe impl Send for Mailbox {}

impl Default for Mailbox {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl Mailbox {
    /// Creates a new, empty Mailbox.
    #[inline(always)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            head: AtomicUsize::new(0),
            _pad1: [0; 56],
            tail: AtomicUsize::new(0),
            _pad2: [0; 56],
            buffer: HugeBuffer::new(),
        }
    }

    /// Pushes a `TaskChunk` into the mailbox.
    ///
    /// Utilizes hardware-specific demote/clean instructions to accelerate
    /// visibility of the updated tail pointer to the consumer core.
    ///
    /// # Errors
    /// Returns the `TaskChunk` back to the caller if the mailbox is full.
    #[inline(always)]
    #[allow(clippy::result_large_err)]
    pub fn push(&self, chunk: TaskChunk) -> Result<(), TaskChunk> {
        let current_tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (current_tail + 1) & MAILBOX_MASK;

        if next_tail == self.head.load(Ordering::Acquire) {
            return Err(chunk);
        }

        unsafe {
            let buffer_ptr = (*self.buffer.ptr).get().cast::<TaskChunk>();
            *buffer_ptr.add(current_tail) = chunk;
        }

        self.tail.store(
            next_tail,
            if cfg!(any(target_arch = "aarch64", target_arch = "riscv64")) {
                core::sync::atomic::Ordering::SeqCst
            } else {
                core::sync::atomic::Ordering::Release
            },
        );

        #[cfg(all(
            feature = "hw-acceleration",
            any(target_arch = "x86", target_arch = "x86_64")
        ))]
        unsafe {
            core::arch::asm!("cldemote [{}]", in(reg) &raw const self.tail);
        }

        #[cfg(all(feature = "hw-acceleration", target_arch = "aarch64"))]
        unsafe {
            core::arch::asm!("dc cvac, {}", in(reg) &self.tail);
        }

        #[cfg(all(feature = "hw-acceleration", target_arch = "riscv64"))]
        unsafe {
            core::arch::asm!("cbo.clean 0({0})", in(reg) &self.tail);
        }

        Ok(())
    }

    /// Pops a `TaskChunk` from the mailbox.
    #[inline(always)]
    pub fn pop(&self) -> Option<TaskChunk> {
        let current_head = self.head.load(Ordering::Relaxed);

        // Optimization: Relaxed check first to avoid Acquire load on empty mailboxes
        if current_head == self.tail.load(core::sync::atomic::Ordering::Relaxed) {
            return None;
        }

        if current_head == self.tail.load(core::sync::atomic::Ordering::Acquire) {
            return None; // Double-check with Acquire
        }

        let chunk = unsafe {
            let buffer_ptr = (*self.buffer.ptr).get().cast::<TaskChunk>();
            core::ptr::read(buffer_ptr.add(current_head))
        };

        let next_head = (current_head + 1) & MAILBOX_MASK;
        self.head.store(next_head, Ordering::Release);
        Some(chunk)
    }
}

/// Hardware-specific CPU hierarchy information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuLevel {
    /// Physical Core ID.
    pub core_id: u16,
    /// Core Complex (CCX) ID.
    pub ccx_id: u16,
    /// NUMA Node ID.
    pub numa_id: u16,
}

pub use crate::common_types::TopologyMode;

/// Execution unit managed by a single OS thread.
///
/// Contains the local SPSC queue, load metrics, and work-deflection heuristics.
#[repr(align(64))]
pub struct Worker {
    /// Hierarchy information for this worker's core.
    pub cpu: CpuLevel,
    /// Current load level (0-100).
    pub load_level: AtomicU8,
    /// Load threshold above which tasks are deflected to peers.
    pub deflection_threshold: AtomicU8,
    /// Counter for incoming work signals, used for hardware-assisted wakeups.
    /// Uses `AtomicU32` for direct compatibility with Linux futex system calls.
    pub event_signal: AtomicU32,

    /// Local SPSC execution queue.
    pub local_queue: HugeBuffer<[TaskIndex; LOCAL_QUEUE_CAPACITY]>,
    /// Head of the local queue (Atomic for `AArch64` visibility).
    pub local_head: AtomicUsize,
    /// Tail of the local queue (Atomic for `AArch64` visibility).
    pub local_tail: AtomicUsize,

    /// Total scheduler ticks executed.
    pub ticks: u64,
    /// Ordered list of peer core IDs for mailbox polling.
    pub polling_order: Vec<usize>,
}

unsafe impl Sync for Worker {}
unsafe impl Send for Worker {}

impl Worker {
    /// Creates a new `Worker` and calculates its CCX-aware polling order.
    #[inline(always)]
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(cpu: CpuLevel, total_cores: usize) -> Self {
        let mut polling_order = Vec::with_capacity(total_cores - 1);
        let my_core = cpu.core_id as usize;
        let my_ccx = cpu.ccx_id;

        for i in 0..total_cores {
            if i != my_core && (i / 8) as u16 == my_ccx {
                polling_order.push(i);
            }
        }
        for i in 0..total_cores {
            if i != my_core && (i / 8) as u16 != my_ccx {
                polling_order.push(i);
            }
        }

        Self {
            cpu,
            load_level: AtomicU8::new(0),
            deflection_threshold: AtomicU8::new(80),
            local_queue: HugeBuffer::new(),
            local_head: AtomicUsize::new(0),
            local_tail: AtomicUsize::new(0),
            ticks: 0,
            event_signal: AtomicU32::new(0),
            polling_order,
        }
    }

    /// Returns the current number of tasks in the local queue.
    #[inline(always)]
    pub fn local_queue_len(&self) -> usize {
        let head = self.local_head.load(core::sync::atomic::Ordering::Acquire);
        let tail = self.local_tail.load(core::sync::atomic::Ordering::Acquire);
        tail.wrapping_sub(head) & LOCAL_QUEUE_MASK
    }

    /// Updates the `load_level` based on the current queue length.
    #[inline(always)]
    pub fn update_load(&self) {
        let queue_len = self.local_queue_len();
        #[allow(clippy::cast_possible_truncation)]
        let load = core::cmp::min((queue_len * 100) >> 13, 100) as u8;
        self.load_level.store(load, Ordering::Relaxed);
    }

    /// Performs internal maintenance tasks (e.g., adaptive threshold updates).
    #[inline(always)]
    pub fn tick(&mut self) {
        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks.trailing_zeros() >= 10 {
            let load = self.load_level.load(Ordering::Relaxed);
            let current_thresh = self.deflection_threshold.load(Ordering::Relaxed);

            let new_thresh = if load > 90 {
                current_thresh.saturating_sub(5).max(40)
            } else if load < 30 {
                current_thresh.saturating_add(5).min(95)
            } else {
                current_thresh
            };

            self.deflection_threshold
                .store(new_thresh, Ordering::Relaxed);
        }
    }

    /// Pushes a single task into the local queue. Returns true if successful.
    #[inline(always)]
    pub fn push_local(&self, task: TaskIndex) -> bool {
        let tail = self.local_tail.load(Ordering::Relaxed);
        if self.local_queue_len() >= LOCAL_QUEUE_CAPACITY - 1 {
            return false;
        }
        unsafe {
            let buffer_ptr = self.local_queue.ptr.cast::<TaskIndex>();
            *buffer_ptr.add(tail) = task;
        }
        self.local_tail
            .store((tail + 1) & LOCAL_QUEUE_MASK, Ordering::Release);
        true
    }

    /// Pushes a batch of tasks into the local queue.
    #[inline(always)]
    pub fn push_batch(&mut self, chunk: &TaskChunk) {
        let count = chunk.count;
        let tail = self.local_tail.load(core::sync::atomic::Ordering::Relaxed);
        let end_idx = tail.wrapping_add(count);

        if end_idx <= LOCAL_QUEUE_CAPACITY {
            unsafe {
                core::ptr::copy_nonoverlapping(
                    chunk.tasks.as_ptr(),
                    (*self.local_queue.ptr).as_mut_ptr().add(tail),
                    count,
                );
            }
        } else {
            let first_part = LOCAL_QUEUE_CAPACITY - tail;
            let second_part = count - first_part;
            unsafe {
                core::ptr::copy_nonoverlapping(
                    chunk.tasks.as_ptr(),
                    (*self.local_queue.ptr).as_mut_ptr().add(tail),
                    first_part,
                );
                core::ptr::copy_nonoverlapping(
                    chunk.tasks.as_ptr().add(first_part),
                    (*self.local_queue.ptr).as_mut_ptr(),
                    second_part,
                );
            }
        }
        self.local_tail.store(
            end_idx & LOCAL_QUEUE_MASK,
            core::sync::atomic::Ordering::Release,
        );
    }

    /// Primary execution loop for the worker thread.
    ///
    /// Drains the local queue, performs O(1) context alignment, and executes
    /// the context switch to the fiber.
    ///
    /// # Safety
    /// * `context_base` must point to the start of the `ContextPool` memory region.
    /// * `context_size` and `group_guard_size` must match the pool's initialized layout.
    #[inline(always)]
    pub unsafe fn dispatch_loop(&self, pool: &crate::memory_management::ContextPool) {
        let mut head = self.local_head.load(Ordering::Acquire);
        while head != self.local_tail.load(Ordering::Acquire) {
            let task = unsafe {
                let buffer_ptr = self.local_queue.ptr.cast::<TaskIndex>();
                *buffer_ptr.add(head)
            };

            head = (head + 1) & LOCAL_QUEUE_MASK;
            self.local_head
                .store(head, core::sync::atomic::Ordering::Release);

            let target_ptr = pool.get_context_ptr(task);

            // Hardware Prefetch: Bring FiberContext to L1 using T0 hint immediately
            #[cfg(target_arch = "x86_64")]
            unsafe {
                core::arch::x86_64::_mm_prefetch::<0>(target_ptr as *const i8);
            }
            #[cfg(target_arch = "aarch64")]
            unsafe {
                core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) target_ptr, options(nostack, preserves_flags));
            }
            #[cfg(all(target_arch = "riscv64", feature = "hw-acceleration"))]
            unsafe {
                core::arch::asm!("prefetch.r 0({0})", in(reg) target_ptr, options(nostack, preserves_flags));
            }

            crate::future_bridge::CURRENT_FIBER.with(|c| c.set(target_ptr));

            unsafe {
                ((*target_ptr).switch_fn)(
                    &raw mut (*target_ptr).executor_regs,
                    &raw const (*target_ptr).regs,
                );
            }

            crate::future_bridge::CURRENT_FIBER.with(|c| c.set(core::ptr::null_mut()));

            // Optimized lifecycle state machine: handle Finished, Notified, or Suspending transitions.
            // We use a single load and robust checks to minimize hot-path latency.
            let post_state = unsafe {
                (*target_ptr)
                    .state
                    .load(core::sync::atomic::Ordering::Acquire)
            };

            let mut final_state = post_state;
            if post_state == crate::memory_management::FiberStatus::Suspending as u32 {
                // Fiber requested suspension. Transition to Yielded to allow cross-core migration.
                // If this CAS fails, it means a concurrent wake() moved it to Notified.
                match unsafe {
                    (*target_ptr).state.compare_exchange(
                        crate::memory_management::FiberStatus::Suspending as u32,
                        crate::memory_management::FiberStatus::Yielded as u32,
                        core::sync::atomic::Ordering::Release,
                        core::sync::atomic::Ordering::Acquire,
                    )
                } {
                    Ok(_) => final_state = crate::memory_management::FiberStatus::Yielded as u32,
                    Err(actual) => final_state = actual,
                }
            }

            // Terminal states (Finished, Panicked)
            if final_state == crate::memory_management::FiberStatus::Finished as u32
                || final_state == crate::memory_management::FiberStatus::Panicked as u32
            {
                pool.free_context(task);
            } else if final_state == crate::memory_management::FiberStatus::Notified as u32 {
                // Cooperative yield or backpressure-induced suspension: re-enqueue.
                self.push_local(task);
                // Return to allow mailbox polling and prevent live-locks on high contention.
                return;
            }
        }
    }
}

/// The Dtact-V3 Distributed Scheduler.
///
/// Manages a set of `Worker` units and the P2P Mailbox matrix for
/// cross-core task migration.
pub struct DtaScheduler {
    /// Thread-local worker states.
    pub workers: Vec<UnsafeCell<Worker>>,
    /// N x N Mailbox matrix for P2P communication.
    pub mailboxes: Vec<Vec<Mailbox>>,
    /// Mailboxes for tasks spawned from external host threads.
    pub external_mailboxes: Vec<Mailbox>,
    /// Locks for external mailboxes (to allow multiple host threads to spawn).
    pub external_locks: Vec<crate::utils::SpinLock>,
    /// Active topology mode.
    pub topology: TopologyMode,
    /// Branchless jump table for task enqueuing.
    #[allow(clippy::type_complexity)]
    pub enqueue_jmp: [fn(&Self, usize, usize, TaskIndex) -> bool; 2],
}

unsafe impl Sync for DtaScheduler {}
unsafe impl Send for DtaScheduler {}

impl DtaScheduler {
    /// Creates a new `DtaScheduler` for the specified number of workers.
    #[inline(always)]
    #[must_use]
    pub fn new(num_workers: usize, topology: TopologyMode) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        let mut mailboxes = Vec::with_capacity(num_workers);
        let mut external_mailboxes = Vec::with_capacity(num_workers);
        let mut external_locks = Vec::with_capacity(num_workers);

        for i in 0..num_workers {
            #[allow(clippy::cast_possible_truncation)]
            workers.push(UnsafeCell::new(Worker::new(
                CpuLevel {
                    core_id: i as u16,
                    ccx_id: (i / 8) as u16,
                    numa_id: (i / 64) as u16,
                },
                num_workers,
            )));

            let mut row = Vec::with_capacity(num_workers);
            for _ in 0..num_workers {
                row.push(Mailbox::new());
            }
            mailboxes.push(row);
            external_mailboxes.push(Mailbox::new());
            external_locks.push(crate::utils::SpinLock::new());
        }

        Self {
            workers,
            mailboxes,
            external_mailboxes,
            external_locks,
            topology,
            enqueue_jmp: [Self::do_push_local, Self::do_push_remote],
        }
    }

    /// Signals a worker that new work is available in its mailboxes.
    #[inline(always)]
    fn signal_worker(&self, target_core: usize) {
        unsafe {
            let worker = &*self.workers[target_core].get();
            // On AArch64 and RISC-V, SeqCst is necessary for signaling across cores to ensure
            // that all preceding mailbox/queue stores are globally visible.
            let order = core::sync::atomic::Ordering::Release;
            worker.event_signal.fetch_add(1, order);
            // Must call futex_wake to awaken workers in Tier 3 (deep sleep).
            crate::utils::futex_wake(
                (&raw const worker.event_signal).cast::<core::sync::atomic::AtomicU32>(),
            );
        }
    }

    #[inline(always)]
    fn do_push_local(&self, source_core: usize, target_core: usize, task: TaskIndex) -> bool {
        let current_worker = crate::future_bridge::CURRENT_WORKER_ID.with(std::cell::Cell::get);
        if current_worker == source_core {
            unsafe {
                let worker = &*self.workers[source_core].get();
                if worker.push_local(task) {
                    return true;
                }
            }
        }

        // Fallback to external mailbox if local queue is full or cross-thread
        loop {
            self.external_locks[target_core].lock();
            let mut chunk = TaskChunk::default();
            chunk.tasks[0] = task;
            chunk.count = 1;
            let res = self.external_mailboxes[target_core].push(chunk);
            self.external_locks[target_core].unlock();

            if res.is_ok() {
                self.signal_worker(target_core);
                return true;
            }
            core::hint::spin_loop();
        }
    }

    #[inline(always)]
    fn do_push_remote(&self, _source_core: usize, target_core: usize, task: TaskIndex) -> bool {
        let current_worker = crate::future_bridge::CURRENT_WORKER_ID.with(std::cell::Cell::get);

        let mut retries = 0u32;
        loop {
            let success = if current_worker < self.workers.len() {
                let mut chunk = TaskChunk::default();
                chunk.tasks[0] = task;
                chunk.count = 1;
                self.mailboxes[current_worker][target_core]
                    .push(chunk)
                    .is_ok()
            } else {
                // External thread: Push to external mailbox
                self.external_locks[target_core].lock();
                let mut chunk = TaskChunk::default();
                chunk.tasks[0] = task;
                chunk.count = 1;
                let success = self.external_mailboxes[target_core].push(chunk).is_ok();
                self.external_locks[target_core].unlock();
                success
            };

            if success {
                self.signal_worker(target_core);
                break;
            }

            retries = retries.saturating_add(1);
            if retries > 1024 {
                std::thread::yield_now();
            } else {
                core::hint::spin_loop();
            }
        }

        #[cfg(all(
            feature = "hw-acceleration",
            any(target_arch = "x86", target_arch = "x86_64")
        ))]
        unsafe {
            core::arch::asm!(
                "mov rax, {}",
                ".byte 0xf3, 0x0f, 0xc7, 0xf0",
                in(reg) target_core as u64,
                out("rax") _,
                options(nostack, preserves_flags),
            );
        }

        #[cfg(all(feature = "hw-acceleration", target_arch = "aarch64"))]
        unsafe {
            core::arch::asm!("sev", options(nostack, preserves_flags));
        }

        #[cfg(all(feature = "hw-acceleration", target_arch = "riscv64"))]
        unsafe {
            core::arch::asm!("csrw uipi, {0}", in(reg) target_core);
        }
        true
    }

    /// Enqueues a task into the mesh, applying work-deflection if necessary.
    ///
    /// If `TopologyMode::P2PMesh` is active, deflection is restricted to
    /// local CCX neighbors. If `TopologyMode::Global` is active, tasks can
    /// be deflected to any available core in the runtime.
    #[inline(always)]
    #[must_use]
    pub fn enqueue_task(&self, source_core: usize, flow_id: u64, task: TaskIndex) -> bool {
        let num_workers = self.workers.len();
        let source_core = source_core % num_workers;
        let worker_ref = unsafe { &*self.workers[source_core].get() };
        let threshold = worker_ref.deflection_threshold.load(Ordering::Relaxed);
        let load = worker_ref.load_level.load(Ordering::Relaxed);

        let deflect_mask = if load > threshold { usize::MAX } else { 0 };
        #[allow(clippy::cast_possible_truncation)]
        let h1 = (flow_id & 7) as usize;
        #[allow(clippy::cast_possible_truncation)]
        let h2 = ((flow_id >> 3) & 7 | 1) as usize;

        let target_core = if self.topology == TopologyMode::Global {
            // Global mode: Hash across all workers
            (source_core + h1 + h2) % num_workers
        } else {
            // P2P Mesh mode: Restricted to CCX (8-core boundary)
            let ccx_base = source_core & !7;
            let local_idx = source_core & 7;
            let deflect_target = (local_idx + h1 + h2) & 7;
            let target_idx = local_idx ^ ((local_idx ^ deflect_target) & deflect_mask);
            (ccx_base | target_idx) % num_workers
        };

        let jump_idx = usize::from(target_core != source_core);
        (self.enqueue_jmp[jump_idx])(self, source_core, target_core, task)
    }

    /// Polls all incoming mailboxes for the current core.
    #[inline(always)]
    pub fn poll_mailboxes(&self, current_core: usize) {
        let worker = unsafe { &mut *self.workers[current_core].get() };

        let num_polls = worker.polling_order.len();

        for idx in 0..num_polls {
            let i = worker.polling_order[idx];

            let row = &self.mailboxes[i];

            while let Some(chunk) = row[current_core].pop() {
                worker.push_batch(&chunk);
            }
        }

        // 2. Poll External Mailbox for external host-thread spawns
        while let Some(chunk) = self.external_mailboxes[current_core].pop() {
            worker.push_batch(&chunk);
        }

        worker.update_load();
        worker.tick();
    }

    /// Main heartbeat loop for a hardware worker thread with cooperative shutdown.
    ///
    /// Periodically polls local queues, mailboxes, and external queues for work.
    /// Supports cooperative shutdown via the provided atomic flag.
    #[inline]
    #[allow(clippy::too_many_lines)]
    pub fn run_worker_static(
        scheduler: &Self,
        current_core: usize,
        pool: &crate::memory_management::ContextPool,
        shutdown: &core::sync::atomic::AtomicBool,
    ) {
        crate::future_bridge::CURRENT_WORKER_ID.with(|c| c.set(current_core));
        let mut idle_count: u32 = 0;

        loop {
            if shutdown.load(core::sync::atomic::Ordering::Acquire) {
                return;
            }

            let mut activity = false;

            // 1. Dispatch local tasks
            unsafe {
                let worker = &*scheduler.workers[current_core].get();
                let head_before = worker.local_head.load(Ordering::Acquire);
                worker.dispatch_loop(pool);
                if worker.local_head.load(Ordering::Acquire) != head_before {
                    activity = true;
                }
            }

            // 2. Poll incoming mailboxes
            let q_len_before =
                unsafe { (&*scheduler.workers[current_core].get()).local_queue_len() };
            scheduler.poll_mailboxes(current_core);
            let q_len_after =
                unsafe { (&*scheduler.workers[current_core].get()).local_queue_len() };

            if q_len_after > q_len_before {
                activity = true;
            }

            if activity {
                idle_count = 0;
                continue;
            }

            // 3. Adaptive Backoff Strategy
            idle_count = idle_count.saturating_add(1);

            // Tier 1: Fast Spinning (Low Latency)
            if idle_count < 256 {
                core::hint::spin_loop();
                continue;
            }

            // Tier 2: Light Hardware Pause (Power Efficiency)
            if idle_count < 2048 {
                #[cfg(target_arch = "aarch64")]
                unsafe {
                    core::arch::asm!("yield", options(nostack, preserves_flags));
                }

                #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
                core::hint::spin_loop(); // On x86, this is PAUSE

                #[cfg(all(feature = "hw-acceleration", target_arch = "riscv64"))]
                unsafe {
                    core::arch::asm!("pause", options(nostack, preserves_flags));
                }

                #[cfg(not(any(
                    target_arch = "aarch64",
                    target_arch = "x86",
                    target_arch = "x86_64",
                    all(feature = "hw-acceleration", target_arch = "riscv64")
                )))]
                for _ in 0..8 {
                    core::hint::spin_loop();
                }
                continue;
            }

            // Tier 3: Adaptive Deep Sleep (OS-level suspension or hardware standby)
            unsafe {
                let worker = &*scheduler.workers[current_core].get();

                let signal_before = worker
                    .event_signal
                    .load(core::sync::atomic::Ordering::Acquire);

                // Architecture-specific Hardware Standby hints
                #[cfg(all(feature = "hw-acceleration", target_arch = "aarch64"))]
                core::arch::asm!("wfe", options(nostack, preserves_flags));

                #[cfg(all(feature = "hw-acceleration", target_arch = "riscv64"))]
                core::arch::asm!("pause", options(nostack, preserves_flags));

                #[cfg(all(
                    feature = "hw-acceleration",
                    any(target_arch = "x86_64", target_arch = "x86")
                ))]
                {
                    let sig_ptr = &raw const worker.event_signal as *mut core::ffi::c_void;
                    let control = 1u32; // C0.1 (Fast wakeup)
                    let timeout_low = 2_000_000u32;
                    let timeout_high = 0u32;
                    core::arch::asm!(
                        "umonitor {0}",
                        "cmp {1:e}, {2:e}",
                        "jne 2f",
                        "umwait {3:e}",
                        "2:",
                        in(reg) sig_ptr,
                        in(reg) signal_before,
                        in(reg) worker.event_signal.load(core::sync::atomic::Ordering::Relaxed),
                        in(reg) control,
                        inout("eax") timeout_low => _,
                        inout("edx") timeout_high => _,
                        options(nostack, preserves_flags)
                    );
                }

                // Barrier: Ensure signal_before load happens BEFORE the final check of work.
                // This prevents the CPU from reordering a stale 'empty' check before a fresh signal load.
                #[cfg(any(target_arch = "aarch64", target_arch = "riscv64"))]
                core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);

                // Final check before OS-level de-scheduling via futex.
                scheduler.poll_mailboxes(current_core);
                let head = worker
                    .local_head
                    .load(core::sync::atomic::Ordering::Acquire);
                let tail = worker
                    .local_tail
                    .load(core::sync::atomic::Ordering::Acquire);

                if head == tail {
                    // Enter OS-managed sleep. The kernel will wake us when event_signal changes.
                    crate::utils::futex_wait(&raw const worker.event_signal, signal_before);
                }

                #[cfg(not(feature = "hw-acceleration"))]
                {
                    // Yield to OS to avoid burning cycles in deep idle
                    if idle_count > 10000 {
                        std::thread::yield_now();
                        idle_count = 2048; // Stay in Tier 3
                    } else {
                        core::hint::spin_loop();
                    }
                }
            }

            // Cap idle_count to prevent overflow and stay in Tier 3
            if idle_count > 20000 {
                idle_count = 10000;
            }
        }
    }
}
