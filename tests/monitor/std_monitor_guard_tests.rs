/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`StdMonitorGuard`](qubit_lock::StdMonitorGuard).

use std::time::Duration;

use qubit_lock::{StdMonitor, WaitTimeoutStatus};

#[test]
fn test_std_monitor_guard_updates_state() {
    let monitor = StdMonitor::new(Vec::new());

    {
        let mut items = monitor.lock();
        items.push(1);
        items.push(2);
    }

    assert_eq!(monitor.read(|items| items.clone()), vec![1, 2]);
}

#[test]
fn test_std_monitor_guard_wait_timeout_returns_timed_out() {
    let monitor = StdMonitor::new(false);

    let guard = monitor.lock();
    let (guard, status) = guard.wait_timeout(Duration::from_millis(30));

    assert!(!*guard);
    assert_eq!(status, WaitTimeoutStatus::TimedOut);
}
