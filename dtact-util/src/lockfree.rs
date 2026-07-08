//! Shared lock-free building blocks used by every module in this crate's
//! native backends (`timer`, `fs`, and, going forward, `process`/`signal`/
//! `stream`). Nothing here takes an `std::sync::Mutex`/`Condvar` on any
//! hot path — completion state is plain atomics, waker storage is a
//! single wait-free `AtomicPtr<Waker>` swap, and cross-thread handoff
//! queues are lock-free Treiber stacks, not `Mutex<Vec<_>>`.

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

// =============================================================================
// TreiberStack — lock-free, ABA-safe, index-based free-list
// =============================================================================
// Moved here from `io::native` (previously a private copy inside that
// module) so every native backend that wants a preallocated slot pool —
// `io`, and now `fs` — shares one implementation instead of each hand-
// rolling its own. The tagged 64-bit head (32-bit index + 32-bit
// generation tag packed together) makes push/pop immune to the classic
// ABA problem on a lock-free stack: a popped-then-repushed index can't be
// mistaken for "unchanged" because the tag always advances.
#[repr(align(64))]
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
            head: AtomicU64::new(u32::MAX as u64), // empty index (u32::MAX), tag 0
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

// =============================================================================
// BufferPool — page-aligned arena carved into fixed-size chunks, handed
// out/reclaimed via a TreiberStack free-list
// =============================================================================
// Also moved here from `io::native` (previously private, and duplicated
// in spirit by `fs`'s earlier per-op `Vec<u8>`/`Box<OpState>` allocations
// before this pass). One arena `alloc()` up front, then `acquire()`/
// `release()` are index-stack push/pop — no allocator call, no lock, on
// the per-operation hot path.
pub struct BufferPool {
    arena_ptr: *mut u8,
    layout: std::alloc::Layout,
    chunk_size: usize,
    free: TreiberStack,
}

unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    pub fn new(total_chunks: usize, chunk_size: usize) -> Self {
        let layout =
            std::alloc::Layout::from_size_align(total_chunks.max(1) * chunk_size.max(1), 4096)
                .expect("Invalid layout alignment for BufferPool");
        let arena_ptr = unsafe { std::alloc::alloc(layout) };
        if arena_ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        let free = TreiberStack::new(total_chunks);
        for i in 0..total_chunks as u32 {
            free.push(i);
        }
        Self {
            arena_ptr,
            layout,
            chunk_size,
            free,
        }
    }

    #[inline]
    pub fn get_ptr(&self, idx: u32) -> *mut u8 {
        unsafe { self.arena_ptr.add(idx as usize * self.chunk_size) }
    }

    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Borrow a chunk index from the pool; `None` if exhausted.
    #[inline]
    pub fn acquire(&self) -> Option<u32> {
        self.free.pop()
    }

    /// Return a chunk index to the pool.
    #[inline]
    pub fn release(&self, idx: u32) {
        self.free.push(idx);
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.arena_ptr, self.layout);
        }
    }
}

// =============================================================================
// AtomicWakerSlot — wait-free waker storage
// =============================================================================
// A single `AtomicPtr<Waker>` pointing at a heap-boxed, owned `Waker`.
// `register`/`take_and_wake` are each exactly one `AtomicPtr::swap` — no
// spinlock, no manual `RawWaker`/vtable pointer surgery (an earlier version
// of this type stored `(data, vtable)` as two separate atomics guarded by a
// spinlock; that had a real, hard-to-pin-down soundness bug that showed up
// as intermittent heap corruption under concurrent register/wake traffic —
// see the commit this replaced). Plain `Box::into_raw`/`Box::from_raw`
// ownership transfer is trivial to reason about by comparison: whichever
// thread's `swap` observes a given pointer owns it, full stop, and every
// code path here either takes ownership (and eventually drops or wakes,
// which consumes) or hands ownership off atomically to the next swap.
pub struct AtomicWakerSlot {
    ptr: AtomicPtr<Waker>,
}

impl Default for AtomicWakerSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicWakerSlot {
    pub const fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Store `waker`, replacing (and dropping) whatever was previously
    /// registered.
    pub fn register(&self, waker: &Waker) {
        let boxed = Box::into_raw(Box::new(waker.clone()));
        let old = self.ptr.swap(boxed, Ordering::AcqRel);
        if !old.is_null() {
            drop(unsafe { Box::from_raw(old) });
        }
    }

    /// Take whatever waker is registered (if any) and wake it.
    pub fn take_and_wake(&self) {
        let p = self.ptr.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            let waker = unsafe { Box::from_raw(p) };
            waker.wake();
        }
    }
}

impl Drop for AtomicWakerSlot {
    fn drop(&mut self) {
        let p = *self.ptr.get_mut();
        if !p.is_null() {
            drop(unsafe { Box::from_raw(p) });
        }
    }
}

unsafe impl Send for AtomicWakerSlot {}
unsafe impl Sync for AtomicWakerSlot {}

// =============================================================================
// MpmcStack<T> — lock-free multi-producer multi-consumer Treiber stack
// =============================================================================
// Used as the cross-thread handoff for "many task threads submit ops, one
// worker thread drains and issues them" (fs::uring_linux's SQE queue,
// timer's per-bucket entry lists). Ordering within a bucket/batch is
// irrelevant for both use sites, so a stack (LIFO) is as good as any other
// MPMC structure and is the simplest one that's genuinely lock-free.
pub struct MpmcStack<T> {
    head: AtomicPtr<Node<T>>,
    len: AtomicUsize,
}

struct Node<T> {
    value: T,
    next: *mut Node<T>,
}

impl<T> Default for MpmcStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MpmcStack<T> {
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            len: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, value: T) {
        let node = Box::into_raw(Box::new(Node {
            value,
            next: ptr::null_mut(),
        }));
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            unsafe { (*node).next = head };
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    pub fn pop(&self) -> Option<T> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if head.is_null() {
                return None;
            }
            let next = unsafe { (*head).next };
            match self
                .head
                .compare_exchange_weak(head, next, Ordering::Acquire, Ordering::Acquire)
            {
                Ok(_) => {
                    self.len.fetch_sub(1, Ordering::Relaxed);
                    let boxed = unsafe { Box::from_raw(head) };
                    return Some(boxed.value);
                }
                Err(actual) => head = actual,
            }
        }
    }

    /// Atomically take the entire stack's contents as a `Vec`, leaving the
    /// stack empty. O(1) swap of the head pointer plus an O(n) linked-list
    /// walk to materialize the `Vec` — no CAS retries beyond the single
    /// head swap regardless of `n`.
    pub fn drain_all(&self) -> Vec<T> {
        let mut head = self.head.swap(ptr::null_mut(), Ordering::AcqRel);
        let mut out = Vec::new();
        while !head.is_null() {
            let boxed = unsafe { Box::from_raw(head) };
            head = boxed.next;
            out.push(boxed.value);
        }
        self.len.store(0, Ordering::Relaxed);
        // LIFO push order means `drain_all` naturally yields
        // most-recently-pushed-first; reverse so batches submit in
        // roughly FIFO order (cosmetic — correctness doesn't depend on it).
        out.reverse();
        out
    }

    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed).is_null()
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }
}

unsafe impl<T: Send> Send for MpmcStack<T> {}
unsafe impl<T: Send> Sync for MpmcStack<T> {}

impl<T> Drop for MpmcStack<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

// =============================================================================
// SpscQueue<T> — cache-aligned, lock-free single-producer/single-consumer
// ring buffer
// =============================================================================
// Moved here from `io::native` (previously private) for the same reason as
// `TreiberStack`/`BufferPool`: `stream`'s native duplex-pipe backend needs
// exactly this shape — one writer, one reader, fixed capacity, no lock —
// for each direction of a pipe, so it reuses this implementation rather
// than hand-rolling its own ring buffer.
//
// No outer `repr(align(64))` — `head`/`tail` already each own a cache line
// via `CacheAlignedUsize`, which is what actually matters for avoiding
// false sharing between producer and consumer; aligning the container
// itself only pads the start of `buffer` for no benefit.
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

    /// Single-producer push. Returns `Err(value)` (handing the value back)
    /// if the queue is full — never blocks.
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

    /// Single-consumer pop. Returns `None` if the queue is empty.
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

    pub fn is_full(&self) -> bool {
        let tail = self.tail.value.load(Ordering::Relaxed);
        let head = self.head.value.load(Ordering::Acquire);
        tail.wrapping_sub(head) == self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> Drop for SpscQueue<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

// =============================================================================
// OnceSlot<T> — a single-fire, wait-free async result cell
// =============================================================================
// The generalization of the `PENDING`-sentinel-`AtomicI64` pattern
// `fs::iocp_windows`/`fs::uring_linux` use, for ops whose result isn't a
// plain integer (a `std::process::ExitStatus`, a `(usize, Vec<u8>)` read
// result, etc). One `AtomicPtr<T>` starts null; `set` heap-boxes the value
// and swaps it in; `poll` follows the same double-check-around-
// waker-registration shape as every other completion primitive in this
// module. Exactly one `set` call is ever expected per `OnceSlot` — calling
// it twice is a caller bug, not something this type tries to paper over
// (debug-asserted, not defended against in release).
pub struct OnceSlot<T> {
    ptr: AtomicPtr<T>,
    waker: AtomicWakerSlot,
}

impl<T> Default for OnceSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OnceSlot<T> {
    pub const fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
            waker: AtomicWakerSlot::new(),
        }
    }

    /// Complete this slot with `value`, waking whatever's polling it.
    /// Must be called at most once per `OnceSlot`.
    pub fn set(&self, value: T) {
        let boxed = Box::into_raw(Box::new(value));
        let prev = self.ptr.swap(boxed, Ordering::AcqRel);
        debug_assert!(
            prev.is_null(),
            "OnceSlot::set called more than once — second value leaked"
        );
        self.waker.take_and_wake();
    }

    /// Poll for completion, registering `cx`'s waker if not yet complete.
    ///
    /// Both checks `swap` the pointer out (not just `load`) before
    /// reconstructing the `Box` — polling again after an already-observed
    /// `Ready` is not something callers are expected to do (standard
    /// `Future` contract), but doing it anyway must not double-free, and
    /// a `load`-then-`Box::from_raw` on the fast path would leave a
    /// dangling non-null pointer behind for exactly that case.
    pub fn poll(&self, cx: &Context<'_>) -> Poll<T> {
        let p = self.ptr.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            return Poll::Ready(*unsafe { Box::from_raw(p) });
        }
        self.waker.register(cx.waker());
        let p = self.ptr.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            return Poll::Ready(*unsafe { Box::from_raw(p) });
        }
        Poll::Pending
    }
}

impl<T> Drop for OnceSlot<T> {
    fn drop(&mut self) {
        let p = *self.ptr.get_mut();
        if !p.is_null() {
            drop(unsafe { Box::from_raw(p) });
        }
    }
}

unsafe impl<T: Send> Send for OnceSlot<T> {}
unsafe impl<T: Send> Sync for OnceSlot<T> {}
