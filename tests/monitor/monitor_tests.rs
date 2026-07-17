// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`Monitor`](qubit_lock::Monitor).

use std::{
    sync::Arc,
    time::Duration,
};

use qubit_lock::{
    ArcParkingLotMonitor,
    Monitor,
    ParkingLotMonitor,
    WaitTimeoutResult,
};

/// Exercises timed waiting through the aggregate blocking capability.
fn wait_through_trait<M>(monitor: &M)
where
    M: Monitor<State = bool>,
{
    assert_eq!(
        monitor.wait_until_for(Duration::ZERO, |ready| *ready, |_| 7),
        Ok(WaitTimeoutResult::TimedOut),
    );
}

#[test]
/// Verifies a named shared monitor handle satisfies [`Monitor`].
fn test_monitor_trait_accepts_parking_lot_monitor() {
    wait_through_trait(&ArcParkingLotMonitor::new(false));
}

#[test]
/// Verifies blanket Arc delegation satisfies [`Monitor`].
fn test_monitor_trait_accepts_arc_wrapped_implementation() {
    wait_through_trait(&Arc::new(ParkingLotMonitor::new(false)));
}
