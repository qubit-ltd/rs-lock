// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncTimeoutConditionWaiter`](qubit_lock::AsyncTimeoutConditionWaiter).

use std::time::Duration;

use qubit_lock::{
    AsyncTimeoutConditionWaiter, MonotonicInstant, TimeError, TokioMonitor, WaitTimeoutResult,
};

/// Runs a zero-budget async wait through a generic timeout bound.
async fn wait_through_trait<W>(waiter: &W) -> Result<WaitTimeoutResult<i32>, TimeError>
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

/// Runs an action-free deadline async wait through a generic timeout bound.
async fn wait_until_ready_with_deadline_through_trait<W>(
    waiter: &W,
    deadline: MonotonicInstant,
) -> Result<WaitTimeoutResult<()>, TimeError>
where
    W: AsyncTimeoutConditionWaiter<State = bool>,
{
    waiter
        .wait_until_ready_with_deadline_async(deadline, |ready| *ready)
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

#[tokio::test]
/// Verifies the deadline async helper preserves ready and timeout outcomes.
async fn test_async_timeout_condition_waiter_wait_until_ready_with_deadline_preserves_outcome() {
    let timed_out = TokioMonitor::current(false);
    let ready = TokioMonitor::current(true);
    let timed_out_deadline = timed_out.timer().now();
    let ready_deadline = ready.timer().now();

    assert_time_result_eq!(
        wait_until_ready_with_deadline_through_trait(&timed_out, timed_out_deadline).await,
        Ok(WaitTimeoutResult::TimedOut),
    );
    assert_time_result_eq!(
        wait_until_ready_with_deadline_through_trait(&ready, ready_deadline).await,
        Ok(WaitTimeoutResult::Ready(())),
    );
}
