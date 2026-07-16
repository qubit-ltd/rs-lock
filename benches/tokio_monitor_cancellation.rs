// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures cancellation cost for registered Tokio monitor waiters.

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
use qubit_lock::{
    ArcTokioMonitor,
    AsyncConditionWaiter,
};

/// Registry sizes used to reveal cancellation scaling.
const WAITER_COUNTS: [usize; 4] = [32, 128, 512, 2_048];

/// A boxed owned wait future that can be cancelled by dropping it.
type OwnedWaitFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Creates `count` futures and polls each one until it registers as a waiter.
///
/// # Arguments
///
/// * `count` - Number of pending wait futures to register.
///
/// # Returns
///
/// Registered pending futures whose drop path performs cancellation.
fn create_registered_waiters(count: usize) -> Vec<OwnedWaitFuture> {
    let monitor = ArcTokioMonitor::new(false);
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
    waiters
}

/// Benchmarks dropping all registered waiters for several registry sizes.
///
/// # Arguments
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

criterion_group!(benches, benchmark_tokio_monitor_cancellation);
criterion_main!(benches);
