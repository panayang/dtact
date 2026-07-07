// Included into `windows_impl` via `include!`. See the comment at the call
// site in lib.rs for why this duplicates (rather than shares) the Unix
// backend's lock-free primitives.

// =========================================================================
// LATENCY-BREAKDOWN TRACING (DTACT_IO_TRACE=1)
// =========================================================================
#[inline]
fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("DTACT_IO_TRACE").is_some())
}

#[inline]
fn trace_now_us() -> u128 {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START.get_or_init(std::time::Instant::now).elapsed().as_micros()
}

macro_rules! io_trace {
    ($($arg:tt)*) => {
        if trace_enabled() {
            eprintln!($($arg)*);
        }
    };
}

// =========================================================================
// LOCK-FREE TREIBER STACK (ABA-free via tag+index packed into one u64)
// =========================================================================
#[repr(align(64))]
struct TreiberStack {
    head: std::sync::atomic::AtomicU64,
    next: Box<[std::sync::atomic::AtomicU32]>,
}

impl TreiberStack {
    fn new(size: usize) -> Self {
        let mut next = Vec::with_capacity(size);
        for i in 0..size {
            next.push(std::sync::atomic::AtomicU32::new((i + 1) as u32));
        }
        if size > 0 {
            next[size - 1].store(u32::MAX, Ordering::Relaxed);
        }
        Self {
            head: std::sync::atomic::AtomicU64::new(u32::MAX as u64),
            next: next.into_boxed_slice(),
        }
    }

    fn push(&self, idx: u32) {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let head_idx = (head & 0xFFFF_FFFF) as u32;
            let tag = (head >> 32) as u32;
            self.next[idx as usize].store(head_idx, Ordering::Release);
            let new_head = ((tag.wrapping_add(1) as u64) << 32) | (idx as u64);
            match self
                .head
                .compare_exchange_weak(head, new_head, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
    }

    fn pop(&self) -> Option<u32> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let head_idx = (head & 0xFFFF_FFFF) as u32;
            if head_idx == u32::MAX {
                return None;
            }
            let tag = (head >> 32) as u32;
            let next = self.next[head_idx as usize].load(Ordering::Acquire);
            let new_head = ((tag.wrapping_add(1) as u64) << 32) | (next as u64);
            match self
                .head
                .compare_exchange_weak(head, new_head, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => return Some(head_idx),
                Err(actual) => head = actual,
            }
        }
    }
}

// =========================================================================
// CACHE-ALIGNED LOCK-FREE SPSC RINGBUFFER
// =========================================================================
struct CacheAlignedUsize {
    value: AtomicUsize,
}

struct SpscQueue<T> {
    head: CacheAlignedUsize,
    tail: CacheAlignedUsize,
    buffer: Box<[std::mem::MaybeUninit<T>]>,
    capacity: usize,
}

unsafe impl<T: Send> Send for SpscQueue<T> {}
unsafe impl<T: Send> Sync for SpscQueue<T> {}

impl<T> SpscQueue<T> {
    fn new(capacity: usize) -> Self {
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

    fn push(&self, value: T) -> Result<(), T> {
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
        self.tail.value.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    fn pop(&self) -> Option<T> {
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
        self.head.value.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    fn is_empty(&self) -> bool {
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
    /// The SOCKET (as usize) this op was issued against, so `cancel_queue`
    /// draining can find/clean up the right side of the op without the
    /// dropping thread touching IOCP-associated state directly.
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
        let data = wait_slot.waker_data.swap(std::ptr::null_mut(), Ordering::Relaxed);
        let vtable = wait_slot.waker_vtable.swap(std::ptr::null_mut(), Ordering::Relaxed);
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
        static NEXT_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
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
