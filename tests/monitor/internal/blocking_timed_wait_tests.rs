// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the shared blocking timed-wait loop.

use std::time::Duration;

use qubit_lock::{
    StdMonitor,
    WaitTimeoutResult,
};

/// Verifies a timed blocking wait performs its deciding predicate check.
#[test]
fn test_blocking_timed_wait_runs_action_when_predicate_is_already_ready() {
    let monitor = StdMonitor::new(true);

    let result = monitor
        .wait_until_for(Duration::ZERO, |ready| *ready, |_| 7)
        .expect("default timer should register");

    assert_eq!(result, WaitTimeoutResult::Ready(7));
}
