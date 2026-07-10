//! Multi-producer, multi-consumer broadcast channel.
//!
//! Pre-allocated, zero-allocation on the send path, and cache-friendly.
//! Eliminates hazard pointer linear scans entirely.

use super::wait_queue::WaitQueue;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

/// A single pre-allocated slot inside the contiguous ring buffer.
#[repr(align(64))]
struct Slot<T> {
    /// Tracks the absolute message sequence number.
    /// Bit 63 can be used as a "writing/locked" flag, or we can just rely on
    /// updating it after writing the value.
    seq: AtomicU64,
    /// The value is stored completely inline. Zero heap allocations.
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Sync> Sync for Slot<T> {}

impl<T> Slot<T> {
    const fn new() -> Self {
        Self {
            seq: AtomicU64::new(u64::MAX),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

#[repr(align(64))]
struct Shared<T> {
    /// Contiguous, pre-allocated ring buffer. Zero heap allocations on the hot path.
    slots: Box<[Slot<T>]>,
    capacity: usize,
    next_seq: AtomicU64,
    sender_count: AtomicUsize,
    wait: WaitQueue,
}

unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Sync> Sync for Shared<T> {}

/// Create a broadcast channel with a `capacity`-entry backlog.
#[must_use]
#[inline]
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let capacity = capacity.max(1);

    let mut slots = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        slots.push(Slot::new());
    }

    let shared = Arc::new(Shared {
        slots: slots.into_boxed_slice(),
        capacity,
        next_seq: AtomicU64::new(0),
        sender_count: AtomicUsize::new(1),
        wait: WaitQueue::new(),
    });

    let receiver = Receiver {
        shared: shared.clone(),
        next_seq: 0,
    };

    (Sender { shared }, receiver)
}

/// The sending half of a [`channel`]. Cheaply [`Clone`]-able.
#[repr(align(64))]
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    #[inline(always)]
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.wait.wake_all();
        }
    }
}

impl<T: Clone> Sender<T> {
    /// Broadcast `value` to every current and future receiver.
    ///
    /// Zero heap allocations. Purely atomic array coordination.
    ///
    /// # Errors
    ///
    /// Returns `SendError` if all downstream receivers have been dropped,
    /// leaving nobody left to consume the payload.
    #[inline(always)]
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.receiver_count() == 0 {
            return Err(SendError(value));
        }

        let seq = self.shared.next_seq.fetch_add(1, Ordering::Relaxed);
        let idx = (seq as usize) % self.shared.capacity;
        let slot = &self.shared.slots[idx];

        // 1. Mark the slot as transitioning/locked by storing an intermediate flag
        // or letting readers know it's being overwritten.
        // Storing u64::MAX - 1 flags a transient state.
        slot.seq.store(u64::MAX - 1, Ordering::Release);

        // 2. Write/Overwrite the element directly in place (Zero-alloc)
        unsafe {
            let ptr = slot.value.get();
            // If the channel has wrapped around, we need to drop the old value inline safely
            if seq >= self.shared.capacity as u64 {
                std::ptr::drop_in_place((*ptr).as_mut_ptr());
            }
            std::ptr::write((*ptr).as_mut_ptr(), value);
        }

        // 3. Publish the final absolute sequence number. Unlocks for readers.
        slot.seq.store(seq, Ordering::Release);

        if self.shared.wait.has_waiters() {
            self.shared.wait.wake_all();
        }

        Ok(())
    }

    /// Current number of live receivers (a lower bound under concurrent
    /// clone/drop, same caveat `tokio`'s equivalent has).
    #[must_use]
    #[inline(always)]
    pub fn receiver_count(&self) -> usize {
        Arc::strong_count(&self.shared)
            .saturating_sub(self.shared.sender_count.load(Ordering::Acquire))
    }
}

/// The receiving half of a [`channel`].
#[repr(align(64))]
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    next_seq: u64,
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

impl<T> Clone for Receiver<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            next_seq: self.next_seq,
        }
    }
}

impl<T: Clone> Receiver<T> {
    /// Receive the next value, waiting if none is available yet.
    ///
    /// # Errors
    /// Returns [`RecvError::Lagged`] (and advances past the gap) if this
    /// receiver fell behind the buffer's `capacity` since its last
    /// `recv()`, or [`RecvError::Closed`] once every [`Sender`] has been
    /// dropped and the backlog is drained.
    #[inline(always)]
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    #[inline]
    fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Result<T, RecvError>> {
        if let Some(result) = self.try_recv_one() {
            return Poll::Ready(result);
        }
        if self.is_closed() {
            return Poll::Ready(self.try_recv_one().unwrap_or(Err(RecvError::Closed)));
        }
        let token = self.shared.wait.register(cx.waker());
        if let Some(result) = self.try_recv_one() {
            self.shared.wait.cancel(token);
            return Poll::Ready(result);
        }
        if self.is_closed() {
            let result = self.try_recv_one().unwrap_or(Err(RecvError::Closed));
            self.shared.wait.cancel(token);
            return Poll::Ready(result);
        }
        Poll::Pending
    }

    #[inline]
    fn try_recv_one(&mut self) -> Option<Result<T, RecvError>> {
        let idx = (self.next_seq as usize) % self.shared.capacity;
        let slot = &self.shared.slots[idx];

        let slot_seq = slot.seq.load(Ordering::Acquire);

        // If the slot is uninitialized or currently being written to, bail immediately.
        // No tight spin loops! Let the async runtime handle re-polling.
        if slot_seq == u64::MAX || slot_seq == u64::MAX - 1 {
            return None;
        }

        match self.next_seq.cmp(&slot_seq) {
            std::cmp::Ordering::Equal => {
                // SAFETY: slot_seq matches exactly what we expect. Clone inline data.
                let cloned_val = unsafe {
                    let ptr = slot.value.get();
                    (*ptr).assume_init_ref().clone()
                };

                // Double check sequence to ensure a blazing fast sender didn't
                // wrap around and overwrite us mid-clone.
                if slot.seq.load(Ordering::Acquire) != slot_seq {
                    return Some(Err(RecvError::Lagged(1)));
                }

                self.next_seq += 1;
                Some(Ok(cloned_val))
            }
            std::cmp::Ordering::Less => {
                let skipped = slot_seq - self.next_seq;
                self.next_seq = slot_seq;
                Some(Err(RecvError::Lagged(skipped)))
            }
            std::cmp::Ordering::Greater => {
                None // Sender hasn't caught up to this slot index yet
            }
        }
    }

    #[inline(always)]
    fn is_closed(&self) -> bool {
        self.shared.sender_count.load(Ordering::Acquire) == 0
    }
}

impl<T> Drop for Shared<T> {
    #[inline(always)]
    fn drop(&mut self) {
        let next_seq = self.next_seq.load(Ordering::Acquire);
        // Only drop slots that were actually written to
        let initialized_elements = next_seq.min(self.capacity as u64) as usize;
        for i in 0..initialized_elements {
            unsafe {
                let ptr = self.slots[i].value.get();
                std::ptr::drop_in_place((*ptr).as_mut_ptr());
            }
        }
    }
}

/// Error returned by [`Sender::send`] when there are no receivers to
/// deliver to. Carries the value back to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(align(64))]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("broadcast channel has no receivers")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned by [`Receiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(align(64))]
pub enum RecvError {
    /// This receiver fell behind by the contained number of messages;
    /// they were overwritten before it could read them. The receiver has
    /// been fast-forwarded past the gap and will resume from the oldest
    /// still-buffered message on the next `recv()`.
    Lagged(u64),
    /// Every [`Sender`] has been dropped and the backlog is drained —
    /// no further values will ever arrive.
    Closed,
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lagged(n) => write!(f, "receiver lagged behind by {n} messages"),
            Self::Closed => f.write_str("channel closed: every sender dropped"),
        }
    }
}

impl std::error::Error for RecvError {}
