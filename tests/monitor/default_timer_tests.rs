// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests for the monitor's internal default timer selection.

use std::time::Duration;

use qubit_lock::{
    ParkingLotMonitor,
    WaitTimeoutResult,
};

/// Verifies a default blocking timer reports an elapsed wait as timed out.
#[test]
fn test_default_timer_drives_blocking_monitor_timeout() {
    let monitor = ParkingLotMonitor::new(false);

    let result = monitor
        .wait_while_for(Duration::from_millis(1), |ready| !*ready, |_| ())
        .expect("default timer should register");

    assert_eq!(result, WaitTimeoutResult::TimedOut);
}
