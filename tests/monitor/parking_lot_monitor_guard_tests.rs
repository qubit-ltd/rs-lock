/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ParkingLotMonitorGuard`](qubit_lock::ParkingLotMonitorGuard).

use std::time::Duration;

use qubit_lock::{
    ParkingLotMonitor,
    WaitTimeoutStatus,
};

#[test]
fn test_parking_lot_monitor_guard_updates_state() {
    let monitor = ParkingLotMonitor::new(Vec::new());

    {
        let mut items = monitor.lock();
        items.push(1);
        items.push(2);
    }

    assert_eq!(monitor.read(|items| items.clone()), vec![1, 2]);
}

#[test]
fn test_parking_lot_monitor_guard_wait_timeout_returns_timed_out() {
    let monitor = ParkingLotMonitor::new(false);

    let guard = monitor.lock();
    let (guard, status) = guard.wait_timeout(Duration::from_millis(30));

    assert!(!*guard);
    assert_eq!(status, WaitTimeoutStatus::TimedOut);
}
