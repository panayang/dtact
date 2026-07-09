//! Multi-producer, single-consumer channel — bounded ([`channel`], with
//! backpressure) and unbounded ([`unbounded_channel`]).

use super::wait_queue::WaitQueue;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

struct Shared<T> {
    queue: Mutex<VecDeque<T>>,
    /// `usize::MAX` for the unbounded variant — `queue.len() < capacity`
    /// is then never false, so the backpressure path in `Sender::send`
    /// is simply never taken.
    capacity: usize,
    sender_count: AtomicUsize,
    receiver_dropped: AtomicBool,
    /// Woken when the queue transitions empty -> nonempty (or the last
    /// sender drops) — always exactly zero or one waiter (`Receiver`
    /// isn't `Clone`), but reuses the same shared queue type as
    /// everything else in this module for consistency.
    recv_wait: WaitQueue,
    /// Woken when the queue transitions full -> not-full (or the
    /// receiver drops) — potentially many waiting senders.
    send_wait: WaitQueue,
}

impl<T> Shared<T> {
    fn is_closed_for_send(&self) -> bool {
        self.receiver_dropped.load(Ordering::Acquire)
    }

    fn is_closed_for_recv(&self) -> bool {
        self.sender_count.load(Ordering::Acquire) == 0
    }
}

/// Create a bounded channel: [`Sender::send`] waits (without blocking the
/// OS thread) once `capacity` unreceived messages are buffered.
#[must_use]
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    build(capacity.max(1))
}

/// Create an unbounded channel: [`UnboundedSender::send`] never waits,
/// buffering as many messages as memory allows.
#[must_use]
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (tx, rx) = build(usize::MAX);
    (
        UnboundedSender { inner: tx },
        UnboundedReceiver { inner: rx },
    )
}

fn build<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        capacity,
        sender_count: AtomicUsize::new(1),
        receiver_dropped: AtomicBool::new(false),
        recv_wait: WaitQueue::new(),
        send_wait: WaitQueue::new(),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

/// The sending half of a bounded [`channel`]. Cheaply [`Clone`]-able —
/// every clone counts toward the channel staying open.
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
            // Last sender gone — wake the receiver so a pending `.recv()`
            // observes channel closure instead of hanging forever.
            self.shared.recv_wait.wake_all();
        }
    }
}

impl<T> Sender<T> {
    /// Send `value`, waiting if the channel is currently at `capacity`.
    ///
    /// # Errors
    /// Returns `value` back in [`SendError`] if the receiver has been
    /// dropped.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut value = Some(value);
        std::future::poll_fn(|cx| self.poll_send(cx, &mut value)).await
    }

    fn poll_send(&self, cx: &Context<'_>, value: &mut Option<T>) -> Poll<Result<(), SendError<T>>> {
        if self.shared.is_closed_for_send() {
            return Poll::Ready(Err(SendError(value.take().expect("value present"))));
        }
        {
            let mut q = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if q.len() < self.shared.capacity {
                q.push_back(value.take().expect("value present"));
                drop(q);
                self.shared.recv_wait.wake_one();
                return Poll::Ready(Ok(()));
            }
        }
        // See `Mutex::poll_lock` for why registration precedes the
        // re-check rather than following it.
        self.shared.send_wait.register(cx.waker());
        if self.shared.is_closed_for_send() {
            return Poll::Ready(Err(SendError(value.take().expect("value present"))));
        }
        let mut q = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if q.len() < self.shared.capacity {
            q.push_back(value.take().expect("value present"));
            drop(q);
            self.shared.recv_wait.wake_one();
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }

    /// `true` once the receiver has been dropped — a subsequent
    /// [`send`](Self::send) is guaranteed to fail.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed_for_send()
    }
}

/// The receiving half of a bounded [`channel`].
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_dropped.store(true, Ordering::Release);
        self.shared.send_wait.wake_all();
    }
}

impl<T> Receiver<T> {
    /// Receive the next message, waiting if the channel is currently
    /// empty. Returns `None` once every [`Sender`] has been dropped and
    /// the buffer is drained.
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&self, cx: &Context<'_>) -> Poll<Option<T>> {
        if let Some(v) = self.try_pop() {
            return Poll::Ready(Some(v));
        }
        if self.shared.is_closed_for_recv() {
            return Poll::Ready(None);
        }
        self.shared.recv_wait.register(cx.waker());
        if let Some(v) = self.try_pop() {
            return Poll::Ready(Some(v));
        }
        if self.shared.is_closed_for_recv() {
            return Poll::Ready(None);
        }
        Poll::Pending
    }

    fn try_pop(&self) -> Option<T> {
        let mut q = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let v = q.pop_front();
        drop(q);
        if v.is_some() {
            self.shared.send_wait.wake_one();
        }
        v
    }
}

/// The sending half of an [`unbounded_channel`]. Cheaply [`Clone`]-able.
pub struct UnboundedSender<T> {
    inner: Sender<T>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> UnboundedSender<T> {
    /// Send `value`. Never waits — the unbounded channel has no capacity
    /// limit, so this is a plain (non-`async`) method, matching
    /// `tokio::sync::mpsc::UnboundedSender::send`.
    ///
    /// # Errors
    /// Returns `value` back in [`SendError`] if the receiver has been
    /// dropped.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.inner.shared.is_closed_for_send() {
            return Err(SendError(value));
        }
        self.inner
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(value);
        self.inner.shared.recv_wait.wake_one();
        Ok(())
    }

    /// `true` once the receiver has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

/// The receiving half of an [`unbounded_channel`].
pub struct UnboundedReceiver<T> {
    inner: Receiver<T>,
}

impl<T> UnboundedReceiver<T> {
    /// Receive the next message, waiting if the channel is currently
    /// empty. Returns `None` once every sender has been dropped and the
    /// buffer is drained.
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await
    }
}

/// Error returned when sending on a channel whose receiver has been
/// dropped. Carries the value that couldn't be delivered back to the
/// caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("channel closed: receiver dropped")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}
