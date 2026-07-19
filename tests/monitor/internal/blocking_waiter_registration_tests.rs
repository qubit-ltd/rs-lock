// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by blocking waiter registrations.

use std::time::Duration;

use qubit_lock::{
    StdMonitor,
    WaitTimeoutResult,
};

/// Verifies a timed-out blocking registration is removed cleanly.
#[test]
fn test_blocking_waiter_registration_is_removed_after_timeout() {
    let monitor = StdMonitor::new(false);

    let result = monitor
        .wait_until_for(Duration::from_millis(1), |ready| *ready, |_| ())
        .expect("standard timer should register");
    assert_eq!(result, WaitTimeoutResult::TimedOut);

    monitor.notify_one();
    monitor.with_write(|ready| *ready = true);
    assert!(monitor.wait_until(|ready| *ready, |ready| *ready));
}
