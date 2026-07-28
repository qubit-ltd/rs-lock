// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures notification and cancellation costs for registered Tokio monitor
//! waiters.

use std::{
    future::Future,
    pin::Pin,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use criterion::{
    BatchSize,
    BenchmarkId,
    Criterion,
    criterion_group,
    criterion_main,
};
use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
};
use qubit_lock::{
    AsyncConditionWaiter,
    TokioMonitor,
};
use std::sync::Arc;

/// Registry sizes used to reveal cancellation scaling.
const WAITER_COUNTS: [usize; 4] = [32, 128, 512, 2_048];

/// A boxed owned wait future that can be cancelled by dropping it.
type OwnedWaitFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Pending Tokio monitor waiters registered against one monitor.
struct RegisteredWaiters {
    monitor: Arc<TokioMonitor<bool>>,
    waiters: Vec<OwnedWaitFuture>,
}

/// Creates `count` futures and polls each one until it registers as a waiter.
///
/// # Parameters
///
/// * `count` - Number of pending wait futures to register.
///
/// # Returns
///
/// The monitor and registered pending futures whose drop path performs
/// cancellation.
fn create_registered_waiters(count: usize) -> RegisteredWaiters {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(TokioMonitor::with_timer(false, clock.new_timer()));
    let mut waiters = Vec::with_capacity(count);
    for _ in 0..count {
        let waiter_monitor = monitor.clone();
        let waiter: OwnedWaitFuture = Box::pin(async move {
            waiter_monitor
                .wait_until_async(|ready| *ready, |_| ())
                .await;
        });
        waiters.push(waiter);
    }

    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for waiter in &mut waiters {
        assert_eq!(waiter.as_mut().poll(&mut context), Poll::Pending);
    }
    RegisteredWaiters { monitor, waiters }
}

/// Benchmarks dropping all registered waiters for several registry sizes.
///
/// # Parameters
///
/// * `criterion` - Criterion benchmark coordinator.
fn benchmark_tokio_monitor_cancellation(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("tokio_monitor_cancellation");
    for waiter_count in WAITER_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched(
                    || create_registered_waiters(waiter_count),
                    drop,
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

/// Benchmarks notification selection for several registered waiter counts.
///
/// # Parameters
///
/// * `criterion` - Criterion benchmark coordinator.
fn benchmark_tokio_monitor_notification(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("tokio_monitor_notification");
    for waiter_count in WAITER_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("notify_one", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched(
                    || create_registered_waiters(waiter_count),
                    |registered| {
                        let waiter_count = registered.waiters.len();
                        registered.monitor.notify_one();
                        std::hint::black_box((waiter_count, registered))
                    },
                    BatchSize::PerIteration,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("notify_all", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched(
                    || create_registered_waiters(waiter_count),
                    |registered| {
                        let waiter_count = registered.waiters.len();
                        registered.monitor.notify_all();
                        std::hint::black_box((waiter_count, registered))
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_tokio_monitor_cancellation,
    benchmark_tokio_monitor_notification
);
criterion_main!(benches);
