// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Benchmarks blocking Monitor wait registration and notification overhead.

use std::{
    hint::black_box,
    sync::{
        Arc,
        mpsc::{
            self,
            Receiver,
        },
    },
    thread::{
        self,
        JoinHandle,
    },
    time::Duration,
};

use criterion::{
    BatchSize,
    BenchmarkId,
    Criterion,
    criterion_group,
    criterion_main,
};
use parking_lot::{
    Condvar,
    Mutex,
};
use qubit_lock::{
    ParkingLotMonitor,
    StdMonitor,
    WaitTimeoutStatus,
};

/// Generous safety deadline that detects stalled notify-one benchmarks without
/// allowing ordinary setup delays to select the timeout path.
const NOTIFY_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Resources for one prepared ParkingLotMonitor notify-all measurement.
type MonitorWaiters = (
    Arc<ParkingLotMonitor<bool>>,
    Vec<JoinHandle<()>>,
    Receiver<()>,
);

/// Resources for one prepared parking_lot Condvar notify-all measurement.
type CondvarWaiters = (
    Arc<(Mutex<bool>, Condvar)>,
    Vec<JoinHandle<()>>,
    Receiver<()>,
);

/// Resources for one timed ParkingLotMonitor notify-one measurement.
type TimedMonitorWaiter = (
    Arc<ParkingLotMonitor<bool>>,
    JoinHandle<()>,
    Receiver<WaitTimeoutStatus>,
);

/// Resources for one timed parking_lot Condvar notify-one measurement.
type TimedCondvarWaiter =
    (Arc<(Mutex<bool>, Condvar)>, JoinHandle<()>, Receiver<bool>);

/// Prepares registered ParkingLotMonitor waiters outside the measured routine.
///
/// # Parameters
///
/// * `waiter_count` - Number of threads to register with the monitor.
///
/// # Returns
///
/// The monitor, waiter handles, and completion receiver.
fn prepare_monitor_waiters(waiter_count: usize) -> MonitorWaiters {
    let monitor = Arc::new(ParkingLotMonitor::new(false));
    let (registered_tx, registered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiters = (0..waiter_count)
        .map(|_| {
            let monitor = Arc::clone(&monitor);
            let registered_tx = registered_tx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let mut registered_tx = Some(registered_tx);
                monitor.wait_until(
                    move |ready| {
                        if !*ready
                            && let Some(registered_tx) = registered_tx.take()
                        {
                            registered_tx.send(()).expect(
                                "benchmark should observe registration",
                            );
                        }
                        *ready
                    },
                    |_| (),
                );
                done_tx
                    .send(())
                    .expect("benchmark should observe waiter completion");
            })
        })
        .collect::<Vec<_>>();

    for _ in 0..waiter_count {
        registered_rx
            .recv()
            .expect("benchmark waiter should register");
    }
    drop(monitor.lock());
    (monitor, waiters, done_rx)
}

/// Prepares registered parking_lot Condvar waiters outside the measured
/// routine.
///
/// # Parameters
///
/// * `waiter_count` - Number of threads to register with the condition
///   variable.
///
/// # Returns
///
/// The shared condition state, waiter handles, and completion receiver.
fn prepare_condvar_waiters(waiter_count: usize) -> CondvarWaiters {
    let condition = Arc::new((Mutex::new(false), Condvar::new()));
    let (registered_tx, registered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiters = (0..waiter_count)
        .map(|_| {
            let condition = Arc::clone(&condition);
            let registered_tx = registered_tx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let (state, changed) = &*condition;
                let mut ready = state.lock();
                registered_tx
                    .send(())
                    .expect("benchmark should observe registration");
                while !*ready {
                    changed.wait(&mut ready);
                }
                done_tx
                    .send(())
                    .expect("benchmark should observe waiter completion");
            })
        })
        .collect::<Vec<_>>();

    for _ in 0..waiter_count {
        registered_rx
            .recv()
            .expect("benchmark waiter should register");
    }
    drop(condition.0.lock());
    (condition, waiters, done_rx)
}

/// Prepares one ParkingLotMonitor waiter performing a nonzero timed wait.
///
/// # Returns
///
/// The monitor, waiter handle, and completion receiver.
fn prepare_timed_monitor_waiter() -> TimedMonitorWaiter {
    let monitor = Arc::new(ParkingLotMonitor::new(false));
    let (registered_tx, registered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut guard = waiter_monitor.lock();
        registered_tx
            .send(())
            .expect("benchmark should observe registration");
        let status = guard
            .wait_for(NOTIFY_WAIT_TIMEOUT)
            .expect("standard Timer should register");
        done_tx
            .send(status)
            .expect("benchmark should observe waiter completion");
    });

    registered_rx
        .recv()
        .expect("benchmark timed waiter should register");
    drop(monitor.lock());
    (monitor, waiter, done_rx)
}

/// Prepares one parking_lot Condvar waiter performing a nonzero timed wait.
///
/// # Returns
///
/// The shared condition state, waiter handle, and completion receiver.
fn prepare_timed_condvar_waiter() -> TimedCondvarWaiter {
    let condition = Arc::new((Mutex::new(false), Condvar::new()));
    let (registered_tx, registered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_condition = Arc::clone(&condition);
    let waiter = thread::spawn(move || {
        let (state, changed) = &*waiter_condition;
        let mut guard = state.lock();
        registered_tx
            .send(())
            .expect("benchmark should observe registration");
        let status = changed.wait_for(&mut guard, NOTIFY_WAIT_TIMEOUT);
        done_tx
            .send(status.timed_out())
            .expect("benchmark should observe waiter completion");
    });

    registered_rx
        .recv()
        .expect("benchmark timed waiter should register");
    drop(condition.0.lock());
    (condition, waiter, done_rx)
}

/// Receives one timed waiter outcome and joins its thread.
///
/// # Parameters
///
/// * `done_rx` - Receiver carrying the timed waiter outcome.
/// * `waiter` - Thread to join after completion.
///
/// # Returns
///
/// The outcome reported by the timed wait.
fn finish_timed_waiter<T>(done_rx: Receiver<T>, waiter: JoinHandle<()>) -> T {
    let outcome = done_rx.recv().expect("benchmark waiter should complete");
    waiter.join().expect("benchmark waiter should not panic");
    outcome
}

/// Verifies that each timed notify-one workload completes by notification.
///
/// This check deliberately runs outside Criterion's measured iteration so
/// assertion work is never attributed to the notification benchmark.
fn validate_notify_one_workloads() {
    let (monitor, waiter, done_rx) = prepare_timed_monitor_waiter();
    monitor.notify_one();
    assert_eq!(
        finish_timed_waiter(done_rx, waiter),
        WaitTimeoutStatus::Woken,
    );

    let (condition, waiter, done_rx) = prepare_timed_condvar_waiter();
    condition.1.notify_one();
    assert!(
        !finish_timed_waiter(done_rx, waiter),
        "notify-one benchmark waiter timed out",
    );
}

/// Waits for every prepared thread to complete and joins it.
///
/// # Parameters
///
/// * `waiter_count` - Number of completion messages to receive.
/// * `done_rx` - Receiver carrying waiter completion messages.
/// * `waiters` - Threads to join after completion.
fn finish_waiters(
    waiter_count: usize,
    done_rx: Receiver<()>,
    waiters: Vec<JoinHandle<()>>,
) {
    for _ in 0..waiter_count {
        done_rx.recv().expect("benchmark waiter should complete");
    }
    for waiter in waiters {
        waiter.join().expect("benchmark waiter should not panic");
    }
}

/// Measures the cost of a zero-duration timed blocking wait.
///
/// # Parameters
///
/// * `criterion` - Criterion runner receiving the benchmark cases.
fn benchmark_zero_timeout(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("blocking_zero_timeout");
    let parking_monitor = ParkingLotMonitor::new(());
    group.bench_function("parking_lot_monitor", |bencher| {
        bencher.iter(|| {
            let mut guard = parking_monitor.lock();
            black_box(
                guard
                    .wait_for(Duration::ZERO)
                    .expect("standard Timer should register"),
            )
        });
    });

    let std_monitor = StdMonitor::new(());
    group.bench_function("std_monitor", |bencher| {
        bencher.iter(|| {
            let mut guard = std_monitor.lock();
            black_box(
                guard
                    .wait_for(Duration::ZERO)
                    .expect("standard Timer should register"),
            )
        });
    });

    let state = Mutex::new(());
    let changed = Condvar::new();
    group.bench_function("parking_lot_condvar", |bencher| {
        bencher.iter(|| {
            let mut guard = state.lock();
            black_box(changed.wait_for(&mut guard, Duration::ZERO).timed_out())
        });
    });
    group.finish();
}

/// Measures notify-all completion with several registered waiter counts.
///
/// # Parameters
///
/// * `criterion` - Criterion runner receiving the benchmark cases.
fn benchmark_notify_all(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("blocking_notify_all");
    for waiter_count in [1_usize, 8, 32] {
        group.bench_with_input(
            BenchmarkId::new("parking_lot_monitor", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched(
                    || prepare_monitor_waiters(waiter_count),
                    |(monitor, waiters, done_rx)| {
                        monitor.with_write_notify_all(|ready| *ready = true);
                        finish_waiters(waiter_count, done_rx, waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("parking_lot_condvar", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched(
                    || prepare_condvar_waiters(waiter_count),
                    |(condition, waiters, done_rx)| {
                        *condition.0.lock() = true;
                        condition.1.notify_all();
                        finish_waiters(waiter_count, done_rx, waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Measures one-waiter notify-one completion for blocking monitor users.
///
/// # Parameters
///
/// * `criterion` - Criterion runner receiving the benchmark cases.
fn benchmark_notify_one(criterion: &mut Criterion) {
    validate_notify_one_workloads();
    let mut group = criterion.benchmark_group("blocking_notify_one");
    group.bench_function("parking_lot_monitor", |bencher| {
        bencher.iter_batched(
            prepare_timed_monitor_waiter,
            |(monitor, waiter, done_rx)| {
                monitor.notify_one();
                let _ = black_box(finish_timed_waiter(done_rx, waiter));
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("parking_lot_condvar", |bencher| {
        bencher.iter_batched(
            prepare_timed_condvar_waiter,
            |(condition, waiter, done_rx)| {
                condition.1.notify_one();
                black_box(finish_timed_waiter(done_rx, waiter));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_zero_timeout,
    benchmark_notify_all,
    benchmark_notify_one
);
criterion_main!(benches);
