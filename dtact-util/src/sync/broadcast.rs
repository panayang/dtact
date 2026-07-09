//! Multi-producer, multi-consumer broadcast channel.
//!
//! Every [`Receiver`] gets every value sent after it was created, up to a
//! fixed backlog (`capacity`) — a receiver that falls more than
//! `capacity` messages behind gets [`RecvError::Lagged`] instead of
//! silently missing values.

use super::wait_queue::WaitQueue;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

struct Shared<T> {
    /// Ring buffer of `(sequence_number, value)`, oldest first. Capped at
    /// `capacity` — pushing past that drops the oldest entry, which is
    /// exactly what turns a slow receiver's next `recv()` into `Lagged`
    /// rather than blocking the sender.
    buffer: Mutex<VecDeque<(u64, T)>>,
    capacity: usize,
    next_seq: AtomicU64,
    sender_count: AtomicUsize,
    wait: WaitQueue,
}

/// Create a broadcast channel with a `capacity`-entry backlog.
#[must_use]
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        buffer: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
        capacity: capacity.max(1),
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
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Wake every receiver so a pending `recv()` observes closure
            // instead of waiting forever for a value that'll never come.
            self.shared.wait.wake_all();
        }
    }
}

impl<T: Clone> Sender<T> {
    /// Broadcast `value` to every current and future (until they lag out
    /// of the backlog) receiver.
    ///
    /// # Errors
    /// Returns [`SendError`] (value handed back) if there are no
    /// receivers at all — matches `tokio::sync::broadcast::Sender::send`,
    /// which treats "nobody could possibly receive this" as an error
    /// rather than silently discarding.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.receiver_count() == 0 {
            return Err(SendError(value));
        }
        let seq = self.shared.next_seq.fetch_add(1, Ordering::AcqRel);
        let mut buf = self
            .shared
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buf.len() >= self.shared.capacity {
            buf.pop_front();
        }
        buf.push_back((seq, value));
        drop(buf);
        self.shared.wait.wake_all();
        Ok(())
    }

    /// Current number of live receivers (a lower bound under concurrent
    /// clone/drop, same caveat `tokio`'s equivalent has).
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        // `Arc::strong_count` counts every `Sender` and `Receiver` clone
        // sharing this `Shared`; subtracting the sender side isolates the
        // receiver side without a separate counter (mirrors this
        // module's `Shared` not tracking `receiver_count` explicitly,
        // unlike `watch`'s `Shared`, since broadcast has no per-receiver
        // "close" side effect that needs an exact count).
        Arc::strong_count(&self.shared)
            .saturating_sub(self.shared.sender_count.load(Ordering::Acquire))
    }
}

/// The receiving half of a [`channel`].
///
/// [`Clone`]-able — each clone tracks its own read position
/// independently, so every receiver (original and clones) sees every
/// broadcast value via its own [`recv`](Self::recv) calls.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    next_seq: u64,
}

impl<T> Clone for Receiver<T> {
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
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&mut self, cx: &Context<'_>) -> Poll<Result<T, RecvError>> {
        if let Some(result) = self.try_recv_one() {
            return Poll::Ready(result);
        }
        if self.is_closed() {
            return Poll::Ready(Err(RecvError::Closed));
        }
        self.shared.wait.register(cx.waker());
        if let Some(result) = self.try_recv_one() {
            return Poll::Ready(result);
        }
        if self.is_closed() {
            return Poll::Ready(Err(RecvError::Closed));
        }
        Poll::Pending
    }

    fn try_recv_one(&mut self) -> Option<Result<T, RecvError>> {
        let buf = self
            .shared
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(&(oldest_seq, _)) = buf.front() else {
            return None; // buffer empty — nothing to receive yet
        };
        if self.next_seq < oldest_seq {
            // We fell behind: the entries we hadn't read yet were
            // overwritten. Report how many, then fast-forward.
            let skipped = oldest_seq - self.next_seq;
            self.next_seq = oldest_seq;
            return Some(Err(RecvError::Lagged(skipped)));
        }
        let idx = usize::try_from(self.next_seq - oldest_seq).ok()?;
        let value = buf.get(idx)?.1.clone();
        drop(buf);
        self.next_seq += 1;
        Some(Ok(value))
    }

    fn is_closed(&self) -> bool {
        self.shared.sender_count.load(Ordering::Acquire) == 0
    }
}

/// Error returned by [`Sender::send`] when there are no receivers to
/// deliver to. Carries the value back to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("broadcast channel has no receivers")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned by [`Receiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
