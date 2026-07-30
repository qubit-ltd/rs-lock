// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by shared blocking monitor-guard waits.

use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
};
use qubit_lock::{
    StdMonitor,
    WaitTimeoutStatus,
};

/// Verifies a timed guard wait retains the guard after an immediate timeout.
#[test]
fn test_blocking_monitor_guard_wait_retains_guard_after_timeout() {
    let clock = ManualMonotonicClock::new();
    let monitor = StdMonitor::with_timer(1, clock.new_timer());
    let mut guard = monitor.lock();

    let status = guard
        .wait_until(clock.now())
        .expect("local deadline should register");

    assert_eq!(status, WaitTimeoutStatus::TimedOut);
    *guard += 1;
    assert_eq!(*guard, 2);
}
