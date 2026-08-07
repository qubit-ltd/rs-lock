// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`Monitor`](qubit_lock::Monitor).

use std::sync::Arc;

use qubit_lock::Monitor;
#[cfg(feature = "parking-lot")]
use qubit_lock::ParkingLotMonitor;
use qubit_lock::StdMonitor;

/// Exercises state access, notification, and untimed waiting through the
/// aggregate blocking capability.
fn use_monitor<M>(monitor: &M)
where
    M: Monitor<State = bool>,
{
    <M as Monitor>::with_write(monitor, |ready| *ready = true);
    assert!(<M as Monitor>::with_read(monitor, |ready| *ready));
    <M as Monitor>::with_write_notify_one(monitor, |ready| *ready = false);
    <M as Monitor>::with_write_notify_all(monitor, |ready| *ready = true);
    assert_eq!(
        monitor.wait_until(
            |ready| *ready,
            |ready| {
                *ready = false;
                7
            },
        ),
        7,
    );
    assert!(!<M as Monitor>::with_read(monitor, |ready| *ready));
}

/// Verifies a named shared monitor handle satisfies [`Monitor`].
#[test]
fn test_monitor_trait_accepts_std_monitor() {
    use_monitor(&Arc::new(StdMonitor::new(false)));
}

/// Verifies blanket Arc delegation satisfies [`Monitor`].
#[test]
fn test_monitor_trait_accepts_arc_wrapped_implementation() {
    use_monitor(&Arc::new(StdMonitor::new(false)));
}

/// Verifies a parking-lot handle satisfies [`Monitor`].
#[cfg(feature = "parking-lot")]
#[test]
fn test_monitor_trait_accepts_parking_lot_monitor() {
    use_monitor(&Arc::new(ParkingLotMonitor::new(false)));
}

/// Verifies [`Arc`] forwards a parking-lot monitor's aggregate capability.
#[cfg(feature = "parking-lot")]
#[test]
fn test_monitor_trait_accepts_arc_wrapped_parking_lot_implementation() {
    use_monitor(&Arc::new(ParkingLotMonitor::new(false)));
}
