//! Criterion bench for `dtact_util::sync`'s primitives vs their
//! `tokio::sync` counterparts, run side by side: uncontended fast-path
//! latency (the case every primitive here optimizes for — a plain atomic
//! CAS/fetch, no waiter ever parked) for the lock/permit/notify-style
//! primitives, and multi-producer throughput for the channel flavors.
//!
//! Run:  cargo bench --bench sync_performance
//! Test: cargo bench --bench sync_performance -- --test

use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, Wake};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }
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

/// Like [`block_on`], but actually parks the OS thread on `Pending`
/// instead of spin-yielding, waking via the real [`Waker`] a primitive's
/// `wake_one`/`wake_all` calls.
///
/// The uncontended benches above never hit `Pending` at all (their fast
/// path always succeeds on the first poll), so the busy-spin `block_on`
/// above costs nothing extra there. The multi-producer channel
/// benchmarks below are different: they *do* block on backpressure, and
/// a spin-yield loop across several real OS threads all contending for
/// the same channel turned a first draft of this benchmark into a
/// measurement of "whose busy-wait loop burns less CPU" rather than "whose
/// channel is faster" — `tokio`'s side, run through its own multi-threaded
/// runtime, always parks properly, so comparing it against a spinning
/// `dtact-sync` side wasn't a fair fight. Parking here for real puts both
/// sides on equal footing.
fn block_on_parked<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, Wake};
    use std::thread::Thread;

    struct ThreadWaker(Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Arc::new(ThreadWaker(std::thread::current())).into();
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            // A wake that lands between the poll above returning
            // `Pending` and this `park()` call isn't lost — `unpark`
            // sets a token that the very next `park()` consumes
            // immediately instead of blocking, so this can't miss a
            // wakeup and hang; it can only, rarely, wake up one spurious
            // extra time, which just costs one harmless extra `poll`.
            Poll::Pending => std::thread::park(),
        }
    }
}

fn bench_mutex_uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutex_uncontended_lock_unlock");

    let dtact_mutex = dtact_util::sync::Mutex::new(0u64);
    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            block_on(async {
                let mut g = dtact_mutex.lock().await;
                *g += 1;
            });
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_mutex = tokio::sync::Mutex::new(0u64);
    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_rt.block_on(async {
                let mut g = tokio_mutex.lock().await;
                *g += 1;
            });
        });
    });

    group.finish();
}

fn bench_rwlock_uncontended_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("rwlock_uncontended_read");

    let dtact_lock = dtact_util::sync::RwLock::new(0u64);
    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            block_on(async {
                let g = dtact_lock.read().await;
                std::hint::black_box(&*g);
            });
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_lock = tokio::sync::RwLock::new(0u64);
    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_rt.block_on(async {
                let g = tokio_lock.read().await;
                std::hint::black_box(&*g);
            });
        });
    });

    group.finish();
}

fn bench_semaphore_uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("semaphore_uncontended_acquire_release");

    let dtact_sem = dtact_util::sync::Semaphore::new(1);
    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            block_on(async {
                let permit = dtact_sem.acquire().await;
                drop(permit);
            });
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_sem = tokio::sync::Semaphore::new(1);
    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_rt.block_on(async {
                let permit = tokio_sem.acquire().await.unwrap();
                drop(permit);
            });
        });
    });

    group.finish();
}

fn bench_notify_permit_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("notify_permit_already_stored");

    let dtact_notify = dtact_util::sync::Notify::new();
    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            dtact_notify.notify_one();
            block_on(dtact_notify.notified());
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let tokio_notify = tokio::sync::Notify::new();
    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_notify.notify_one();
            tokio_rt.block_on(tokio_notify.notified());
        });
    });

    group.finish();
}

fn bench_oneshot_send_recv(c: &mut Criterion) {
    let mut group = c.benchmark_group("oneshot_send_recv");

    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            let (tx, rx) = dtact_util::sync::oneshot::channel::<u64>();
            tx.send(42).unwrap();
            let v = block_on(rx).unwrap();
            std::hint::black_box(v);
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    group.bench_function("tokio", |b| {
        b.iter(|| {
            let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
            tx.send(42).unwrap();
            let v = tokio_rt.block_on(rx).unwrap();
            std::hint::black_box(v);
        });
    });

    group.finish();
}

/// 4 producer threads each sending 2500 messages through one bounded
/// mpsc channel to a single consumer — the shape every real mpsc
/// workload actually stresses (contended `Mutex<VecDeque>` push/pop from
/// multiple threads), not the single-threaded `send`/`recv` alternation
/// `oneshot_send_recv`-style benches would measure instead.
fn bench_mpsc_multi_producer_throughput(c: &mut Criterion) {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: usize = 2500;

    let mut group = c.benchmark_group("mpsc_multi_producer_throughput");
    group.throughput(criterion::Throughput::Elements(
        (PRODUCERS * PER_PRODUCER) as u64,
    ));

    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            let (tx, mut rx) = dtact_util::sync::mpsc::channel::<u32>(64);
            let handles: Vec<_> = (0..PRODUCERS)
                .map(|_| {
                    let tx = tx.clone();
                    std::thread::spawn(move || {
                        block_on_parked(async {
                            for i in 0..PER_PRODUCER as u32 {
                                tx.send(i).await.unwrap();
                            }
                        });
                    })
                })
                .collect();
            drop(tx);
            let mut received = 0usize;
            block_on_parked(async {
                while rx.recv().await.is_some() {
                    received += 1;
                }
            });
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(received, PRODUCERS * PER_PRODUCER);
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_rt.block_on(async {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(64);
                let handles: Vec<_> = (0..PRODUCERS)
                    .map(|_| {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            for i in 0..PER_PRODUCER as u32 {
                                tx.send(i).await.unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);
                let mut received = 0usize;
                while rx.recv().await.is_some() {
                    received += 1;
                }
                for h in handles {
                    h.await.unwrap();
                }
                assert_eq!(received, PRODUCERS * PER_PRODUCER);
            });
        });
    });

    group.finish();
}

/// One sender broadcasting to 4 concurrent receivers — the shape every
/// real broadcast workload stresses (contended `Mutex<VecDeque>` reads
/// from multiple threads plus a `wake_all` fan-out on every send), same
/// rationale as `bench_mpsc_multi_producer_throughput` above. Capacity is
/// sized comfortably above the message count so no receiver lags (lag
/// handling is exercised separately by `sync_test.rs`, not this
/// throughput bench).
fn bench_broadcast_multi_receiver_throughput(c: &mut Criterion) {
    const RECEIVERS: usize = 4;
    const MESSAGES: usize = 5000;

    let mut group = c.benchmark_group("broadcast_multi_receiver_throughput");
    group.throughput(criterion::Throughput::Elements(
        (RECEIVERS * MESSAGES) as u64,
    ));

    group.bench_function("dtact-sync", |b| {
        b.iter(|| {
            let (tx, rx) = dtact_util::sync::broadcast::channel::<u32>(MESSAGES);
            let handles: Vec<_> = (0..RECEIVERS)
                .map(|_| {
                    let mut rx = rx.clone();
                    std::thread::spawn(move || {
                        let mut received = 0usize;
                        block_on_parked(async {
                            while rx.recv().await.is_ok() {
                                received += 1;
                            }
                        });
                        received
                    })
                })
                .collect();
            drop(rx);
            block_on_parked(async {
                for i in 0..MESSAGES as u32 {
                    tx.send(i).unwrap();
                }
            });
            drop(tx);
            for h in handles {
                assert_eq!(h.join().unwrap(), MESSAGES);
            }
        });
    });

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    group.bench_function("tokio", |b| {
        b.iter(|| {
            tokio_rt.block_on(async {
                let (tx, rx) = tokio::sync::broadcast::channel::<u32>(MESSAGES);
                let handles: Vec<_> = (0..RECEIVERS)
                    .map(|_| {
                        let mut rx = tx.subscribe();
                        tokio::spawn(async move {
                            let mut received = 0usize;
                            while rx.recv().await.is_ok() {
                                received += 1;
                            }
                            received
                        })
                    })
                    .collect();
                drop(rx);
                for i in 0..MESSAGES as u32 {
                    tx.send(i).unwrap();
                }
                drop(tx);
                for h in handles {
                    assert_eq!(h.await.unwrap(), MESSAGES);
                }
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mpsc_multi_producer_throughput,
    bench_broadcast_multi_receiver_throughput,
    bench_mutex_uncontended,
    bench_rwlock_uncontended_read,
    bench_semaphore_uncontended,
    bench_notify_permit_path,
    bench_oneshot_send_recv
);
criterion_main!(benches);
