// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TimeoutConditionWaiter`](qubit_lock::TimeoutConditionWaiter).

use std::{
    sync::Arc,
    time::Duration,
};

use qubit_clock::{
    MonotonicInstant,
    TimeError,
};
#[cfg(feature = "parking-lot")]
use qubit_lock::ParkingLotMonitor;
use qubit_lock::{
    StdMonitor,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
};

/// Runs a zero-budget condition wait through a generic timeout bound.
fn wait_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<i32>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_until_for(Duration::ZERO, |ready| *ready, |_| 7)
}

/// Runs an action-free timed condition wait through a generic timeout bound.
fn wait_until_ready_for_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<()>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_until_ready_for(Duration::ZERO, |ready| *ready)
}

/// Runs an action-free deadline wait through a generic timeout bound.
fn wait_until_ready_with_deadline_through_trait<W>(
    waiter: &W,
    deadline: MonotonicInstant,
) -> Result<WaitTimeoutResult<()>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_until_ready_with_deadline(deadline, |ready| *ready)
}

/// Runs a total-timeout wait-while action through a generic timeout bound.
fn wait_while_with_total_timeout_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<i32>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_while_with_total_timeout(
        Duration::ZERO,
        |waiting| *waiting,
        |_| 7,
    )
}

/// Runs a total-timeout wait-until action through a generic timeout bound.
fn wait_until_with_total_timeout_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<i32>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_until_with_total_timeout(Duration::ZERO, |ready| *ready, |_| 7)
}

/// Runs an action-free total-timeout wait through a generic timeout bound.
fn wait_until_ready_with_total_timeout_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<()>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_until_ready_with_total_timeout(Duration::ZERO, |ready| *ready)
}

#[test]
/// Verifies a concrete monitor satisfies [`TimeoutConditionWaiter`].
fn test_timeout_condition_waiter_trait_accepts_std_monitor() {
    assert_time_result_eq!(
        wait_through_trait(&StdMonitor::new(false)),
        Ok(WaitTimeoutResult::TimedOut),
    );
}

#[test]
/// Verifies an Arc-wrapped monitor preserves timeout trait forwarding.
fn test_timeout_condition_waiter_trait_accepts_arc_std_monitor() {
    let monitor = Arc::new(StdMonitor::new(false));

    assert_time_result_eq!(
        wait_through_trait(&monitor),
        Ok(WaitTimeoutResult::TimedOut),
    );
}

#[test]
/// Verifies the action-free timed helper preserves ready and timeout outcomes.
fn test_timeout_condition_waiter_wait_until_ready_for_preserves_outcome() {
    assert_time_result_eq!(
        wait_until_ready_for_through_trait(&StdMonitor::new(false)),
        Ok(WaitTimeoutResult::TimedOut),
    );
    assert_time_result_eq!(
        wait_until_ready_for_through_trait(&StdMonitor::new(true)),
        Ok(WaitTimeoutResult::Ready(())),
    );
}

#[test]
/// Verifies the deadline helper preserves ready and timeout outcomes.
fn test_timeout_condition_waiter_wait_until_ready_with_deadline_preserves_outcome()
 {
    let timed_out = StdMonitor::new(false);
    let ready = StdMonitor::new(true);
    let timed_out_deadline = timed_out.timer().now();
    let ready_deadline = ready.timer().now();

    assert_time_result_eq!(
        wait_until_ready_with_deadline_through_trait(
            &timed_out,
            timed_out_deadline
        ),
        Ok(WaitTimeoutResult::TimedOut),
    );
    assert_time_result_eq!(
        wait_until_ready_with_deadline_through_trait(&ready, ready_deadline),
        Ok(WaitTimeoutResult::Ready(())),
    );
}

#[test]
/// Verifies every total-timeout helper is available through the trait bound.
fn test_timeout_condition_waiter_total_timeout_helpers_preserve_outcome() {
    assert_time_result_eq!(
        wait_while_with_total_timeout_through_trait(&StdMonitor::new(false)),
        Ok(WaitTimeoutResult::Ready(7)),
    );
    assert_time_result_eq!(
        wait_until_with_total_timeout_through_trait(&StdMonitor::new(true)),
        Ok(WaitTimeoutResult::Ready(7)),
    );
    assert_time_result_eq!(
        wait_until_ready_with_total_timeout_through_trait(&StdMonitor::new(
            false
        )),
        Ok(WaitTimeoutResult::TimedOut),
    );
}

#[cfg(feature = "parking-lot")]
#[test]
/// Verifies the parking_lot implementation dispatches the total-timeout
/// primitive through the trait.
fn test_timeout_condition_waiter_total_timeout_accepts_parking_lot_monitor() {
    assert_time_result_eq!(
        wait_while_with_total_timeout_through_trait(&ParkingLotMonitor::new(
            false
        )),
        Ok(WaitTimeoutResult::Ready(7)),
    );
}

#[test]
/// Verifies Arc forwarding preserves the total-timeout trait contract.
fn test_timeout_condition_waiter_arc_forwards_total_timeout() {
    let monitor = Arc::new(StdMonitor::new(true));

    assert_time_result_eq!(
        wait_until_ready_with_total_timeout_through_trait(&monitor),
        Ok(WaitTimeoutResult::Ready(())),
    );
}
