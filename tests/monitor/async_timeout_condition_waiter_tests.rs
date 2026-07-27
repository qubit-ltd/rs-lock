// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncTimeoutConditionWaiter`](qubit_lock::AsyncTimeoutConditionWaiter).

use std::time::Duration;

use qubit_clock::TimeError;
use qubit_lock::{
    AsyncTimeoutConditionWaiter,
    TokioMonitor,
    WaitTimeoutResult,
};

/// Runs a zero-budget async wait through a generic timeout bound.
async fn wait_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<i32>, TimeError>
where
    W: AsyncTimeoutConditionWaiter<State = bool>,
{
    waiter
        .wait_until_for_async(Duration::ZERO, |ready| *ready, |_| 7)
        .await
}

/// Runs a zero-budget action-free wait through a generic timeout bound.
async fn wait_until_ready_for_through_trait<W>(
    waiter: &W,
) -> Result<WaitTimeoutResult<()>, TimeError>
where
    W: AsyncTimeoutConditionWaiter<State = bool>,
{
    waiter
        .wait_until_ready_for_async(Duration::ZERO, |ready| *ready)
        .await
}

#[tokio::test]
/// Verifies a Tokio monitor satisfies [`AsyncTimeoutConditionWaiter`].
async fn test_async_timeout_condition_waiter_trait_accepts_tokio_monitor() {
    assert_time_result_eq!(
        wait_through_trait(&TokioMonitor::current(false)).await,
        Ok(WaitTimeoutResult::TimedOut),
    );
    assert_time_result_eq!(
        wait_until_ready_for_through_trait(&TokioMonitor::current(true)).await,
        Ok(WaitTimeoutResult::Ready(())),
    );
    assert_time_result_eq!(
        wait_until_ready_for_through_trait(&TokioMonitor::current(false)).await,
        Ok(WaitTimeoutResult::TimedOut),
    );
}
