//! Shared lock-free building blocks used by every module in this crate's
//! native backends.
//!
//! Covers `timer`, `fs`, and, going forward, `process`/`signal`/`stream`.
//! Nothing here takes an `std::sync::Mutex`/`Condvar` on any hot path —
//! completion state is plain atomics, waker storage is a single wait-free
//! `AtomicPtr<Waker>` swap, and cross-thread handoff queues are lock-free
//! Treiber stacks, not `Mutex<Vec<_>>`.

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
/// Lock-free, ABA-safe, index-based free-list of `size` slots (indices
/// `0..size`), all initially free.
#[repr(align(64))]
pub struct TreiberStack {
    head: AtomicU64,
    next: Box<[AtomicU32]>,
}

impl TreiberStack {
    /// Build a stack holding indices `0..size`, all free.
    #[must_use]
    pub fn new(size: usize) -> Self {
        let mut next = Vec::with_capacity(size);
        for i in 0..size {
            next.push(AtomicU32::new((i + 1) as u32));
        }
        if size > 0 {
            next[size - 1].store(u32::MAX, Ordering::Relaxed);
        }
        Self {
            head: AtomicU64::new(u64::from(u32::MAX)), // empty index (u32::MAX), tag 0
            next: next.into_boxed_slice(),
        }
    }

    /// Return `idx` to the free-list. `idx` must have previously come from
    /// [`Self::pop`] (or from initial construction) and not currently be
    /// held by anything else — pushing an index that's already free is a
    /// caller bug (double-free of the index) and will corrupt the
    /// free-list for subsequent `pop`s.
    #[inline]
    pub fn push(&self, idx: u32) {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let head_idx = (head & 0xFFFF_FFFF) as u32;
            let tag = (head >> 32) as u32;
            self.next[idx as usize].store(head_idx, Ordering::Release);
            let new_head = (u64::from(tag.wrapping_add(1)) << 32) | u64::from(idx);
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

    /// Take an index off the free-list, or `None` if it's exhausted.
    #[inline]
    pub fn pop(&self) -> Option<u32> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let head_idx = (head & 0xFFFF_FFFF) as u32;
            if head_idx == u32::MAX {
                return None;
            }
            let tag = (head >> 32) as u32;
            let next = self.next[head_idx as usize].load(Ordering::Acquire);
            let new_head = (u64::from(tag.wrapping_add(1)) << 32) | u64::from(next);
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
/// Page-aligned arena carved into `total_chunks` fixed-size chunks, handed
/// out and reclaimed via a [`TreiberStack`] free-list.
pub struct BufferPool {
    arena_ptr: *mut u8,
    layout: std::alloc::Layout,
    chunk_size: usize,
    free: TreiberStack,
}

// SAFETY: `arena_ptr` points at a heap allocation this `BufferPool`
// exclusively owns (freed only in `Drop`); handing out `*mut u8` chunk
// pointers via `get_ptr` doesn't alias Rust-level state, and the
// free-list itself (`TreiberStack`) is already lock-free/thread-safe.
unsafe impl Send for BufferPool {}
// SAFETY: same reasoning as `Send` above — concurrent `acquire`/`release`
// only ever goes through the already-thread-safe `TreiberStack`.
unsafe impl Sync for BufferPool {}

impl BufferPool {
    /// Allocate a page-aligned arena of `total_chunks` chunks of
    /// `chunk_size` bytes each, with all chunks initially free.
    ///
    /// # Panics
    ///
    /// Panics if `total_chunks * chunk_size` (each floored to at least 1)
    /// overflows or produces a layout with invalid size/alignment for the
    /// global allocator (`Layout::from_size_align` failure), or if the
    /// allocator itself fails to satisfy the allocation
    /// (`handle_alloc_error`).
    #[must_use]
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

    /// Raw pointer to the start of chunk `idx` within the arena. `idx`
    /// must be `< total_chunks` as passed to [`Self::new`] — out-of-range
    /// indices produce a pointer outside the arena allocation, which is
    /// unsound to dereference (this function itself performs no
    /// dereference, only pointer arithmetic).
    #[inline]
    pub const fn get_ptr(&self, idx: u32) -> *mut u8 {
        unsafe { self.arena_ptr.add(idx as usize * self.chunk_size) }
    }

    /// The fixed chunk size (in bytes) this pool was constructed with.
    #[inline]
    pub const fn chunk_size(&self) -> usize {
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
// AtomicWakerSlot — single-slot waker storage, no per-register allocation
// =============================================================================
// Single-`AtomicUsize`-state-machine design (the same algorithm as tokio's
// production `AtomicWaker`, which this is a from-scratch reimplementation
// of against the same well-reviewed shape — not a copy of tokio's source).
// The waker itself lives inline in an `UnsafeCell<Option<Waker>>`; the
// `AtomicUsize` state is the *sole* arbiter of who's allowed to touch that
// cell, so there is exactly one atomic to reason about per critical
// section — no torn reads across two independently-updated atomics.
//
// This replaces an earlier `Box::into_raw`/`Box::from_raw`-per-register
// version: correct (a swap-based ownership handoff is easy to reason
// about), but it paid a heap allocation *and* a deallocation on every
// single `register()` call, including the extremely common case where the
// exact same task polls the exact same listener repeatedly without the
// registered waker ever changing. That version itself replaced an even
// earlier two-separate-atomics-plus-spinlock design that had a real,
// hard-to-pin-down soundness bug (intermittent heap corruption under
// concurrent register/wake traffic) — the bug there was two independently
// racy atomics (`data`, `vtable`) with no single point of truth for
// "who owns the slot right now". The state machine below avoids that
// specific failure mode structurally: there is only one atomic, and every
// participant CASes it before touching the `UnsafeCell`, so "who owns the
// slot" is always unambiguous.
const WAITING: usize = 0;
const REGISTERING: usize = 0b01;
const WAKING: usize = 0b10;

/// Wait-free-on-the-fast-path single-slot waker storage.
///
/// `register` skips re-storing when the incoming waker already wakes the
/// same task as what's registered ([`Waker::will_wake`]), and even when it
/// does need to store, does so into an inline cell — no heap allocation on
/// the `register`/`take_and_wake` path at all, unlike an `AtomicPtr<Waker>`
/// swap-based design.
pub struct AtomicWakerSlot {
    state: AtomicUsize,
    waker: std::cell::UnsafeCell<Option<Waker>>,
}

impl Default for AtomicWakerSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicWakerSlot {
    /// An empty slot with no waker registered.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(WAITING),
            waker: std::cell::UnsafeCell::new(None),
        }
    }

    /// Store `waker`, replacing whatever was previously registered (unless
    /// it already [`Waker::will_wake`] the same task, in which case this is
    /// a no-op beyond the CAS). If a [`Self::take_and_wake`] races with
    /// this call and observes the slot mid-registration, this call takes
    /// over waking `waker` itself before returning, so a delivery racing a
    /// registration is never lost.
    #[inline]
    pub fn register(&self, waker: &Waker) {
        match self.state.compare_exchange(
            WAITING,
            REGISTERING,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // SAFETY: the CAS above is the sole gate on touching
                // `waker` — we now hold the only `REGISTERING` token in
                // existence for this slot (the CAS is exclusive), and
                // `take_and_wake` never touches the cell when it observes
                // anything other than `WAITING`. No other code path reads
                // or writes the cell while we hold this state.
                let slot = unsafe { &mut *self.waker.get() };
                let already_registered = slot.as_ref().is_some_and(|w| w.will_wake(waker));
                if !already_registered {
                    *slot = Some(waker.clone());
                }
                // Release back to WAITING — unless a concurrent
                // `take_and_wake` set the `WAKING` bit while we were
                // registering, in which case it deferred the actual wake
                // to us (it saw we were mid-registration and couldn't
                // safely touch the cell itself).
                if self
                    .state
                    .compare_exchange(REGISTERING, WAITING, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    // SAFETY: same as above — we still hold exclusive
                    // access; the only other party that could have
                    // touched `state` here is `take_and_wake`, and it's
                    // structurally forbidden from touching the cell once
                    // it sees we're `REGISTERING`.
                    let pending = unsafe { &mut *self.waker.get() }.take();
                    self.state.store(WAITING, Ordering::Release);
                    if let Some(w) = pending {
                        w.wake();
                    }
                }
            }
            Err(_) => {
                // Someone else currently holds the slot (either
                // `take_and_wake` is mid-delivery, or — for a listener
                // type with a single-poller contract that a concurrent
                // `register` would itself violate — an overlapping
                // register). Either way, the safe fallback that can never
                // lose a wakeup is to wake `waker` directly rather than
                // storing it: worst case this causes one spurious extra
                // poll, never a missed one.
                waker.wake_by_ref();
            }
        }
    }

    /// Take whatever waker is registered (if any) and wake it.
    #[inline]
    pub fn take_and_wake(&self) {
        match self
            .state
            .compare_exchange(WAITING, WAKING, Ordering::Acquire, Ordering::Acquire)
        {
            Ok(_) => {
                // SAFETY: we hold the only `WAKING` token for this slot,
                // and `register` never touches the cell once it observes
                // anything other than `WAITING`, so this access is
                // exclusive.
                let waker = unsafe { &mut *self.waker.get() }.take();
                self.state.store(WAITING, Ordering::Release);
                if let Some(w) = waker {
                    w.wake();
                }
            }
            Err(_) => {
                // A `register` is currently in flight (state is
                // `REGISTERING`, possibly with `WAKING` already OR'd in by
                // an even earlier racing `take_and_wake` — either way, at
                // most one delivery needs to be recorded, and ORing the
                // bit in is enough to tell `register` "wake whatever you
                // end up storing before you return", which it does. We
                // don't touch the cell ourselves here — only whoever's
                // currently `REGISTERING` is allowed to.
                self.state.fetch_or(WAKING, Ordering::AcqRel);
            }
        }
    }
}

impl Drop for AtomicWakerSlot {
    fn drop(&mut self) {
        // SAFETY: `&mut self` — no concurrent access is possible.
        drop(self.waker.get_mut().take());
    }
}

// SAFETY: `waker`'s `UnsafeCell` is only ever touched by whichever thread
// currently holds the `REGISTERING` or `WAKING` token in `state` — the CAS
// on `state` is the single, unambiguous point of truth for exclusive
// access, so no two threads ever read or write the cell concurrently.
unsafe impl Send for AtomicWakerSlot {}
// SAFETY: same reasoning as `Send`.
unsafe impl Sync for AtomicWakerSlot {}

// =============================================================================
// MpmcStack<T> — lock-free multi-producer multi-consumer Treiber stack
// =============================================================================
// Used as the cross-thread handoff for "many task threads submit ops, one
// worker thread drains and issues them" (fs::uring_linux's SQE queue,
// timer's per-bucket entry lists). Ordering within a bucket/batch is
// irrelevant for both use sites, so a stack (LIFO) is as good as any other
// MPMC structure and is the simplest one that's genuinely lock-free.
/// Lock-free multi-producer multi-consumer stack (LIFO), used as a
/// cross-thread handoff queue where ordering within a batch doesn't
/// matter. See the module-level comment above for the intended use
/// sites.
pub struct MpmcStack<T> {
    head: AtomicPtr<Node<T>>,
    len: AtomicUsize,
}

struct Node<T> {
    value: T,
    next: *mut Self,
}

impl<T> Default for MpmcStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MpmcStack<T> {
    /// An empty stack.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            len: AtomicUsize::new(0),
        }
    }

    /// Push `value` onto the stack. Never blocks, never fails (heap
    /// allocation aside).
    #[inline]
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

    /// Pop the most-recently-pushed value, or `None` if empty.
    #[inline]
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

    /// Whether the stack currently has no elements. Racy under concurrent
    /// push/pop from other threads — a `true` result can be stale by the
    /// time the caller acts on it, same as any lock-free length check.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed).is_null()
    }

    /// Approximate current length. Racy under concurrent push/pop for the
    /// same reason as [`Self::is_empty`].
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }
}

// SAFETY: nodes are heap-boxed and moved between threads only via the
// atomic `head` pointer swap (`push`/`pop`/`drain_all`); whichever thread
// wins the CAS/swap has sole ownership of the node it took, so `T: Send`
// is sufficient for the stack itself to be `Send`.
unsafe impl<T: Send> Send for MpmcStack<T> {}
// SAFETY: all mutation goes through the atomic `head`/`len` fields; no
// two threads ever get concurrent mutable access to the same `Node<T>`.
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
/// Cache-aligned, lock-free single-producer/single-consumer ring buffer.
///
/// Fixed power-of-two `capacity`. See the module-level comment above for
/// why `head`/`tail` are each cache-line-aligned separately instead of
/// aligning the whole struct.
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

// SAFETY: exactly one producer thread ever calls `push` and exactly one
// consumer thread ever calls `pop` (the SPSC contract callers must
// uphold); `head`/`tail` atomics establish the happens-before edges that
// make handing a `T` from the producer's `push` to the consumer's `pop`
// sound, so `T: Send` is sufficient for the queue itself to be `Send`.
unsafe impl<T: Send> Send for SpscQueue<T> {}
// SAFETY: same SPSC contract as `Send` — no two threads ever access the
// same slot concurrently, so sharing `&SpscQueue<T>` across threads
// (one producer, one consumer) is sound whenever `T: Send`.
unsafe impl<T: Send> Sync for SpscQueue<T> {}

impl<T> SpscQueue<T> {
    /// Build an empty queue. `capacity` must be a power of two.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is not a power of two.
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns `Err(value)`, handing the value straight back to the
    /// caller, if the ring buffer is currently full (`tail - head` has
    /// reached `capacity`). This is not a fault — it is the normal
    /// backpressure signal for a bounded SPSC queue; the caller decides
    /// whether to retry, drop the value, or apply its own backoff.
    #[inline]
    pub fn push(&self, value: T) -> Result<(), T> {
        let tail = self.tail.value.load(Ordering::Relaxed);
        let head = self.head.value.load(Ordering::Acquire);
        if tail.wrapping_sub(head) == self.capacity {
            return Err(value);
        }
        let mask = self.capacity - 1;
        let idx = tail & mask;
        unsafe {
            let ptr = self.buffer[idx].as_ptr().cast_mut();
            ptr.write(value);
        }
        self.tail
            .value
            .store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Single-consumer pop. Returns `None` if the queue is empty.
    #[inline]
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

    /// Whether the queue currently has no elements.
    pub fn is_empty(&self) -> bool {
        let head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Acquire);
        head == tail
    }

    /// Whether the queue is currently at capacity (the next [`Self::push`]
    /// would be rejected).
    pub fn is_full(&self) -> bool {
        let tail = self.tail.value.load(Ordering::Relaxed);
        let head = self.head.value.load(Ordering::Acquire);
        tail.wrapping_sub(head) == self.capacity
    }

    /// Number of elements currently queued.
    #[inline]
    pub fn len(&self) -> usize {
        let head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    /// The fixed capacity this queue was constructed with.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T: Copy> SpscQueue<T> {
    /// Single-producer bulk push: copy as many elements from `src` as fit
    /// into the ring, returning the count actually pushed (`0` if full).
    ///
    /// This is the bulk analogue of [`push`](Self::push) and exists purely
    /// as a hot-path optimization for `T: Copy` element types (e.g. the
    /// byte pipe in `stream::native`): pushing a run of N bytes one
    /// [`push`](Self::push) call at a time pays N separate atomic
    /// load/store pairs and N bounds-checked writes, whereas this issues at
    /// most two `copy_nonoverlapping` runs (the ring may wrap once) and a
    /// single `tail` store. Behaviour is otherwise identical — same
    /// capacity accounting, same `Release` publication of the new tail.
    pub fn push_slice(&self, src: &[T]) -> usize {
        let tail = self.tail.value.load(Ordering::Relaxed);
        let head = self.head.value.load(Ordering::Acquire);
        let free = self.capacity - tail.wrapping_sub(head);
        let to_push = free.min(src.len());
        if to_push == 0 {
            return 0;
        }
        let mask = self.capacity - 1;
        let start = tail & mask;
        // The ring may wrap: `first` covers the run from `start` to the end
        // of the backing buffer, the remainder wraps to the front.
        let first = to_push.min(self.capacity - start);
        // SAFETY: `MaybeUninit<T>` has the same layout as `T`; `to_push`
        // never exceeds the free capacity, so both runs stay in bounds and
        // never overlap the consumer's occupied region.
        unsafe {
            let base = self.buffer.as_ptr().cast_mut().cast::<T>();
            ptr::copy_nonoverlapping(src.as_ptr(), base.add(start), first);
            if to_push > first {
                ptr::copy_nonoverlapping(src.as_ptr().add(first), base, to_push - first);
            }
        }
        self.tail
            .value
            .store(tail.wrapping_add(to_push), Ordering::Release);
        to_push
    }

    /// Single-consumer bulk pop: copy up to `dst.len()` queued elements into
    /// `dst`, returning the count actually popped (`0` if empty).
    ///
    /// Bulk analogue of [`pop`](Self::pop); see [`push_slice`](Self::push_slice)
    /// for the rationale.
    pub fn pop_slice(&self, dst: &mut [T]) -> usize {
        let head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Acquire);
        let avail = tail.wrapping_sub(head);
        let to_pop = avail.min(dst.len());
        if to_pop == 0 {
            return 0;
        }
        let mask = self.capacity - 1;
        let start = head & mask;
        let first = to_pop.min(self.capacity - start);
        // SAFETY: `to_pop` never exceeds the number of occupied slots, so
        // every element read here was published by a prior `push`/`push_slice`
        // (observed under the `Acquire` load of `tail` above) and is
        // initialized; the two runs stay in bounds and never overlap the
        // producer's free region.
        unsafe {
            let base = self.buffer.as_ptr().cast::<T>();
            ptr::copy_nonoverlapping(base.add(start), dst.as_mut_ptr(), first);
            if to_pop > first {
                ptr::copy_nonoverlapping(base, dst.as_mut_ptr().add(first), to_pop - first);
            }
        }
        self.head
            .value
            .store(head.wrapping_add(to_pop), Ordering::Release);
        to_pop
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
/// A single-fire, wait-free async result cell.
///
/// The generalization of the `PENDING`-sentinel-`AtomicI64` pattern used
/// elsewhere in this crate, for results that aren't a plain integer. See
/// the module doc above for the exactly-once `set` contract.
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
    /// An empty, not-yet-completed slot.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
            waker: AtomicWakerSlot::new(),
        }
    }

    /// Complete this slot with `value`, waking whatever's polling it.
    /// Must be called at most once per `OnceSlot`.
    #[inline]
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
    #[inline]
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

// SAFETY: `set` heap-boxes the value and atomically swaps it into `ptr`;
// `poll` atomically swaps it back out. Whichever side observes the
// non-null pointer has sole ownership, so `T: Send` suffices for `Send`.
unsafe impl<T: Send> Send for OnceSlot<T> {}
// SAFETY: same reasoning as `Send` — every access to the boxed value goes
// through an atomic swap, never a shared read of `&T` from two threads.
unsafe impl<T: Send> Sync for OnceSlot<T> {}
