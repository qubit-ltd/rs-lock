// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`StdMonitorGuard`](qubit_lock::StdMonitorGuard).

use std::{
    sync::Arc,
    time::Duration,
};

use super::failing_timer_tests::{
    FailingTimer,
    assert_backend_unavailable,
};
use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
    TimeError,
};
use qubit_lock::{
    StdMonitor,
    WaitTimeoutStatus,
};

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
    let monitor = StdMonitor::with_timer(1, Arc::new(FailingTimer::new()));
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
