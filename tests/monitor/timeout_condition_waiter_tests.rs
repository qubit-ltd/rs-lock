// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TimeoutConditionWaiter`](qubit_lock::TimeoutConditionWaiter).

use std::time::Duration;

use qubit_clock::TimeError;
use qubit_lock::{StdMonitor, TimeoutConditionWaiter, WaitTimeoutResult};

/// Runs a zero-budget condition wait through a generic timeout bound.
fn wait_through_trait<W>(waiter: &W) -> Result<WaitTimeoutResult<i32>, TimeError>
where
    W: TimeoutConditionWaiter<State = bool>,
{
    waiter.wait_until_for(Duration::ZERO, |ready| *ready, |_| 7)
}

#[test]
/// Verifies a concrete monitor satisfies [`TimeoutConditionWaiter`].
fn test_timeout_condition_waiter_trait_accepts_std_monitor() {
    assert_time_result_eq!(
        wait_through_trait(&StdMonitor::new(false)),
        Ok(WaitTimeoutResult::TimedOut),
    );
}
