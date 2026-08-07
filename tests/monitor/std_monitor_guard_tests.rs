// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`StdMonitorGuard`](qubit_lock::StdMonitorGuard).

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::TimeError;
use qubit_lock::StdMonitor;
use qubit_lock::WaitTimeoutStatus;

use super::failing_timer_tests::assert_backend_unavailable;
use super::failing_timer_tests::registration_failing_timer;

#[test]
fn test_std_monitor_guard_updates_state() {
    let monitor = StdMonitor::new(Vec::new());

    {
        let mut items = monitor.lock();
        items.push(1);
        items.push(2);
    }

    assert_eq!(monitor.with_read(|items| items.clone()), vec![1, 2]);
}

#[test]
fn test_std_monitor_guard_notify_one_releases_lock_and_wakes_waiter() {
    let monitor = Arc::new(StdMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until(
            |ready| {
                if !*ready {
                    checked_tx.send(()).expect(
                        "test coordinator should receive predicate check",
                    );
                }
                *ready
            },
            |_ready| {
                done_tx.send(()).expect(
                    "test coordinator should receive waiter completion",
                );
            },
        );
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check the initial false predicate");
    let mut guard = monitor.lock();
    *guard = true;
    guard.notify_one();

    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("notify_one should wake the registered waiter");
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_std_monitor_guard_notify_all_releases_lock_and_wakes_waiters() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiters = (0..2)
        .map(|_| {
            let waiter_monitor = Arc::clone(&monitor);
            let checked_tx = checked_tx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                waiter_monitor.wait_until(
                    |ready| {
                        if !*ready {
                            checked_tx
                                .send(())
                                .expect("test coordinator should receive predicate check");
                        }
                        *ready
                    },
                    |_ready| {
                        done_tx
                            .send(())
                            .expect("test coordinator should receive waiter completion");
                    },
                );
            })
        })
        .collect::<Vec<_>>();
    drop(checked_tx);
    drop(done_tx);

    for _ in 0..2 {
        checked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("each waiter should check the initial false predicate");
    }
    let mut guard = monitor.lock();
    *guard = true;
    guard.notify_all();

    for _ in 0..2 {
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("notify_all should allow every waiter to finish");
    }
    for waiter in waiters {
        waiter.join().expect("waiter should not panic");
    }
}

#[test]
fn test_std_monitor_guard_keeps_lock_after_foreign_deadline_error() {
    let clock = ManualMonotonicClock::new();
    let monitor = StdMonitor::with_timer(1, clock.new_timer());
    let foreign_deadline = ManualMonotonicClock::new().now();
    let mut guard = monitor.lock();

    let error = guard
        .wait_until(foreign_deadline)
        .expect_err("foreign deadline should fail before releasing guard");

    assert!(matches!(error, TimeError::ClockDomainMismatch { .. }));
    *guard += 1;
    assert_eq!(*guard, 2);
}

#[test]
fn test_std_monitor_guard_wait_until_accepts_reached_local_deadline() {
    let clock = ManualMonotonicClock::new();
    let monitor = StdMonitor::with_timer(1, clock.new_timer());
    let deadline = clock.now();
    let mut guard = monitor.lock();

    let status = guard
        .wait_until(deadline)
        .expect("local deadline should register");

    assert_eq!(status, WaitTimeoutStatus::TimedOut);
    assert_eq!(*guard, 1);
}

#[test]
fn test_std_monitor_guard_keeps_lock_after_timer_registration_error() {
    let monitor =
        StdMonitor::with_timer(1, Arc::new(registration_failing_timer()));
    let mut guard = monitor.lock();

    let error = guard
        .wait_for(Duration::from_secs(1))
        .expect_err("failing Timer should reject registration");

    assert_backend_unavailable(error);
    *guard += 1;
    assert_eq!(*guard, 2);
}

#[test]
fn test_std_monitor_guard_wait_for_returns_timed_out() {
    let monitor = StdMonitor::new(false);

    let mut guard = monitor.lock();
    let status = guard
        .wait_for(Duration::from_millis(30))
        .expect("standard Timer should register");

    assert!(!*guard);
    assert_eq!(status, WaitTimeoutStatus::TimedOut);
}
