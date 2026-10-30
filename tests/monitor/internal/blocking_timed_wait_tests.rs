// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the shared blocking timed-wait loop.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_lock::StdMonitor;
use qubit_lock::WaitTimeoutResult;

/// Verifies a timed blocking wait performs its deciding predicate check.
#[test]
fn test_blocking_timed_wait_runs_action_when_predicate_is_already_ready() {
    let monitor = StdMonitor::new(true);

    let result = monitor
        .wait_until_for(Duration::ZERO, |ready| *ready, |_| 7)
        .expect("default timer should register");

    assert_eq!(result, WaitTimeoutResult::Ready(7));
}

/// Verifies a timed blocking wait reports timeout while the predicate remains
/// waiting.
#[test]
fn test_blocking_timed_wait_returns_timed_out_when_predicate_remains_waiting() {
    let monitor = StdMonitor::new(false);

    let result = monitor
        .wait_until_for(Duration::from_millis(1), |ready| *ready, |_| 7)
        .expect("default timer should register");

    assert_eq!(result, WaitTimeoutResult::TimedOut);
}

/// Verifies a timed blocking wait rechecks the predicate after a notification.
#[test]
fn test_blocking_timed_wait_returns_ready_after_notification() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(StdMonitor::with_timer(false, clock.new_timer()));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        let result = waiter_monitor.wait_until_for(
            Duration::from_secs(1),
            move |ready| {
                if !*ready && let Some(checked_tx) = checked_tx.take() {
                    checked_tx.send(()).expect("initial check should be observed");
                }
                *ready
            },
            |_| 7,
        );
        done_tx.send(result).expect("wait result should be received");
    });
    checked_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("waiter should check the initial state");
    assert!(clock.wait_for_waiters(1, Duration::from_secs(5)));
    monitor.with_write(|ready| *ready = true);
    monitor.notify_one();
    let result = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("notification should complete the wait")
        .expect("manual timer should complete without error");
    assert_eq!(result, WaitTimeoutResult::Ready(7));
    waiter.join().expect("waiter should finish after notification");
}
