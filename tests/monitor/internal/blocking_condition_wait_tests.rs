// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by shared blocking condition-wait algorithms.

use std::time::Duration;

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
