//! Exercises `dtact_util::sync` — no feature gate (matches the module
//! itself, which has no native/tokio split), driven by a minimal
//! from-scratch single-threaded executor plus real OS threads for the
//! genuinely-concurrent cases.

use dtact_util::sync::{Barrier, Mutex, Notify, OnceCell, RwLock, Semaphore};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake};

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = Arc::new(NoopWaker).into();
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn mutex_basic_lock_unlock() {
    let m = Mutex::new(0);
    block_on(async {
        {
            let mut g = m.lock().await;
            *g += 1;
        }
        assert_eq!(*m.lock().await, 1);
    });
}

#[test]
fn mutex_contended_across_threads() {
    let m = Arc::new(Mutex::new(0usize));
    let mut handles = vec![];
    for _ in 0..8 {
        let m = m.clone();
        handles.push(std::thread::spawn(move || {
            block_on(async {
                for _ in 0..1000 {
                    let mut g = m.lock().await;
                    *g += 1;
                }
            });
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(block_on(async { *m.lock().await }), 8000);
}

#[test]
fn mutex_try_lock() {
    let m = Mutex::new(5);
    let g1 = m.try_lock().unwrap();
    assert!(m.try_lock().is_none(), "already locked");
    drop(g1);
    assert!(m.try_lock().is_some());
}

#[test]
fn rwlock_multiple_readers_one_writer() {
    let lock = Arc::new(RwLock::new(0i32));
    block_on(async {
        let r1 = lock.read().await;
        let r2 = lock.read().await;
        assert_eq!(*r1, 0);
        assert_eq!(*r2, 0);
        assert!(
            lock.try_write().is_none(),
            "write must be blocked while readers hold the lock"
        );
        drop(r1);
        drop(r2);
        let mut w = lock.write().await;
        *w = 42;
        drop(w);
        assert_eq!(*lock.read().await, 42);
    });
}

#[test]
fn semaphore_limits_concurrency() {
    let sem = Arc::new(Semaphore::new(2));
    block_on(async {
        let p1 = sem.acquire().await;
        let p2 = sem.acquire().await;
        assert!(sem.try_acquire().is_err(), "no permits left");
        drop(p1);
        assert!(sem.try_acquire().is_ok());
        drop(p2);
    });
}

#[test]
fn notify_permit_survives_early_notify() {
    let notify = Notify::new();
    // notify_one() before anyone is waiting must be remembered.
    notify.notify_one();
    block_on(notify.notified());
}

#[test]
fn barrier_releases_all_participants() {
    let barrier = Arc::new(Barrier::new(4));
    let leaders = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handles = vec![];
    for _ in 0..4 {
        let barrier = barrier.clone();
        let leaders = leaders.clone();
        handles.push(std::thread::spawn(move || {
            let result = block_on(barrier.wait());
            if result.is_leader() {
                leaders.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(leaders.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn once_cell_initializes_exactly_once() {
    let cell: OnceCell<u32> = OnceCell::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    block_on(async {
        for _ in 0..5 {
            let calls = calls.clone();
            let v = cell
                .get_or_init(|| async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    7
                })
                .await;
            assert_eq!(*v, 7);
        }
    });
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

mod oneshot_tests {
    use super::block_on;
    use dtact_util::sync::oneshot;

    #[test]
    fn send_then_recv() {
        let (tx, rx) = oneshot::channel::<u32>();
        tx.send(9).unwrap();
        assert_eq!(block_on(rx).unwrap(), 9);
    }

    #[test]
    fn dropped_sender_errors_receiver() {
        let (tx, rx) = oneshot::channel::<u32>();
        drop(tx);
        assert!(block_on(rx).is_err());
    }

    #[test]
    fn dropped_receiver_errors_send() {
        let (tx, rx) = oneshot::channel::<u32>();
        drop(rx);
        assert_eq!(tx.send(1), Err(1));
    }
}

mod mpsc_tests {
    use super::block_on;
    use dtact_util::sync::mpsc;

    #[test]
    fn bounded_roundtrip() {
        let (tx, mut rx) = mpsc::channel::<u32>(4);
        block_on(async {
            for i in 0..4 {
                tx.send(i).await.unwrap();
            }
            for i in 0..4 {
                assert_eq!(rx.recv().await, Some(i));
            }
        });
    }

    #[test]
    fn closes_when_all_senders_dropped() {
        let (tx, mut rx) = mpsc::channel::<u32>(4);
        drop(tx);
        assert_eq!(block_on(rx.recv()), None);
    }

    #[test]
    fn unbounded_roundtrip() {
        let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
        for i in 0..100 {
            tx.send(i).unwrap();
        }
        block_on(async {
            for i in 0..100 {
                assert_eq!(rx.recv().await, Some(i));
            }
        });
    }

    #[test]
    fn multi_producer_across_threads() {
        // Capacity comfortably above the total send volume: this test is
        // about multiple producers landing in one channel correctly, not
        // about backpressure (that's `bounded_roundtrip` /
        // `closes_when_all_senders_dropped` above) — with a small
        // capacity here, producers would block on a full queue that
        // nothing drains until after `handles` are joined below, which
        // never happens (classic producer/consumer test deadlock, not a
        // channel bug).
        let (tx, mut rx) = mpsc::channel::<u32>(200);
        let mut handles = vec![];
        for i in 0..4u32 {
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                block_on(async {
                    for j in 0..25u32 {
                        tx.send(i * 100 + j).await.unwrap();
                    }
                });
            }));
        }
        drop(tx);
        for h in handles {
            h.join().unwrap();
        }
        let mut received = vec![];
        block_on(async {
            while let Some(v) = rx.recv().await {
                received.push(v);
            }
        });
        assert_eq!(received.len(), 100);
    }

    /// Small capacity + concurrent drain: actually exercises the
    /// `Poll::Pending` backpressure path (not just the always-room
    /// sequential case `bounded_roundtrip` covers) across real threads.
    #[test]
    fn concurrent_backpressure_drains_correctly() {
        let (tx, mut rx) = mpsc::channel::<u32>(4);
        let producer = std::thread::spawn(move || {
            block_on(async {
                for i in 0..50u32 {
                    tx.send(i).await.unwrap();
                }
            });
        });
        let mut received = vec![];
        block_on(async {
            for _ in 0..50 {
                received.push(rx.recv().await.unwrap());
            }
        });
        producer.join().unwrap();
        assert_eq!(received, (0..50).collect::<Vec<_>>());
    }
}

mod watch_tests {
    use super::block_on;
    use dtact_util::sync::watch;

    #[test]
    fn changed_observes_new_value() {
        let (tx, mut rx) = watch::channel(0);
        assert_eq!(*rx.borrow(), 0);
        tx.send(5);
        block_on(async {
            rx.changed().await.unwrap();
        });
        assert_eq!(*rx.borrow(), 5);
    }

    #[test]
    fn closes_when_sender_dropped() {
        let (tx, mut rx) = watch::channel(0);
        drop(tx);
        assert!(block_on(rx.changed()).is_err());
    }
}

mod broadcast_tests {
    use super::block_on;
    use dtact_util::sync::broadcast;

    #[test]
    fn all_receivers_get_every_message() {
        let (tx, mut rx1) = broadcast::channel::<u32>(8);
        let mut rx2 = rx1.clone();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        block_on(async {
            assert_eq!(rx1.recv().await.unwrap(), 1);
            assert_eq!(rx1.recv().await.unwrap(), 2);
            assert_eq!(rx2.recv().await.unwrap(), 1);
            assert_eq!(rx2.recv().await.unwrap(), 2);
        });
    }

    #[test]
    fn lagging_receiver_gets_lagged_error() {
        let (tx, mut rx) = broadcast::channel::<u32>(2);
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        block_on(async {
            match rx.recv().await {
                Err(broadcast::RecvError::Lagged(n)) => assert!(n > 0),
                other => panic!("expected Lagged, got {other:?}"),
            }
        });
    }

    #[test]
    fn send_with_no_receivers_errors() {
        let (tx, rx) = broadcast::channel::<u32>(2);
        drop(rx);
        assert!(tx.send(1).is_err());
    }
}
