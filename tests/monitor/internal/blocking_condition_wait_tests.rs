// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by shared blocking condition-wait algorithms.

use std::{
    cell::Cell,
    time::Duration,
};

use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
    TimeError,
};
use qubit_lock::{
    StdMonitor,
    WaitTimeoutResult,
};

/// Verifies the shared relative wait algorithm checks a zero-budget predicate.
#[test]
fn test_blocking_condition_wait_checks_predicate_for_zero_budget() {
    let monitor = StdMonitor::new(false);

    let result = monitor
        .wait_until_for(Duration::ZERO, |ready| *ready, |_| 7)
        .expect("default timer should register");

    assert_eq!(result, WaitTimeoutResult::TimedOut);
}

/// Verifies the shared relative wait algorithm reports overflow after it finds
/// that the predicate still requires waiting.
#[test]
fn test_blocking_condition_wait_reports_overflow_after_blocking_predicate() {
    let clock = ManualMonotonicClock::new_shared();
    clock
        .advance(Duration::MAX)
        .expect("manual clock should reach its maximum instant");
    let monitor = StdMonitor::with_timer(false, clock.new_timer());
    let predicate_calls = Cell::new(0usize);

    let result = monitor.wait_while_for(
        Duration::from_nanos(1),
        |_| {
            predicate_calls.set(predicate_calls.get() + 1);
            true
        },
        |_| (),
    );

    assert!(matches!(result, Err(TimeError::InstantOverflow)));
    assert_eq!(predicate_calls.get(), 1);
}
