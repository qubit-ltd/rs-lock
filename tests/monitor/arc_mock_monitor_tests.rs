/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ArcMockMonitor`](qubit_lock::ArcMockMonitor).

use std::time::Duration;

use qubit_lock::{ArcMockMonitor, TimeoutNotificationWaiter, WaitTimeoutStatus};

#[test]
fn test_arc_mock_monitor_clone_shares_state_and_mock_time() {
    let monitor = ArcMockMonitor::new(Vec::<i32>::new());
    let cloned = monitor.clone();

    cloned.write(|items| items.push(7));
    monitor.advance(Duration::from_millis(5));

    assert_eq!(monitor.read(|items| items.clone()), vec![7]);
    assert_eq!(cloned.elapsed(), Duration::from_millis(5));
}

#[test]
fn test_arc_mock_monitor_wait_for_times_out_after_advance() {
    let monitor = ArcMockMonitor::new(false);
    monitor.advance(Duration::from_millis(10));

    assert_eq!(
        monitor.wait_for(Duration::from_millis(0)),
        WaitTimeoutStatus::TimedOut,
    );
}
