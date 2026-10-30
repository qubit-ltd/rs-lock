// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by Tokio waiter registrations.

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_lock::AsyncConditionWaiter;
use qubit_lock::AsyncTimeoutConditionWaiter;
use qubit_lock::TokioMonitor;

fn assert_cancelled_registration_is_removed(timed: bool) {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = TokioMonitor::with_timer(false, clock.new_timer());
    let mut context = Context::from_waker(Waker::noop());
    let mut cancelled: Pin<Box<dyn Future<Output = ()> + '_>> = if timed {
        Box::pin(async {
            let _ = monitor
                .wait_until_for_async(
                    Duration::from_secs(8),
                    |ready| *ready,
                    |_| -> () { panic!("cancelled action must not run") },
                )
                .await;
            panic!("cancelled action must not complete");
        })
    } else {
        Box::pin(monitor.wait_until_async(|ready| *ready, |_| panic!("cancelled action must not run")))
    };
    assert!(cancelled.as_mut().poll(&mut context).is_pending());
    drop(cancelled);

    let mut successor = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
    assert!(successor.as_mut().poll(&mut context).is_pending());
    let mut update = Box::pin(monitor.with_write_async(|ready| *ready = true));
    assert!(matches!(update.as_mut().poll(&mut context), Poll::Ready(())));
    drop(update);
    monitor.notify_one();
    assert!(matches!(successor.as_mut().poll(&mut context), Poll::Ready(true)));
}

#[test]
fn test_tokio_waiter_registration_is_removed_after_cancellation() {
    assert_cancelled_registration_is_removed(false);
}

#[test]
fn test_tokio_timed_waiter_registration_is_removed_after_cancellation() {
    assert_cancelled_registration_is_removed(true);
}
