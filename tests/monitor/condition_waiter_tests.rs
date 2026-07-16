// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ConditionWaiter`](qubit_lock::ConditionWaiter).

use qubit_lock::{
    ConditionWaiter,
    ParkingLotMonitor,
};

/// Runs an immediately ready condition wait through a generic bound.
fn wait_through_trait<W>(waiter: &W) -> i32
where
    W: ConditionWaiter<State = bool>,
{
    waiter.wait_until(|ready| *ready, |_| 7)
}

#[test]
/// Verifies that a concrete blocking monitor satisfies [`ConditionWaiter`].
fn test_condition_waiter_trait_accepts_parking_lot_monitor() {
    assert_eq!(wait_through_trait(&ParkingLotMonitor::new(true)), 7);
}
