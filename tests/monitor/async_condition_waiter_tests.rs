// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncConditionWaiter`](qubit_lock::AsyncConditionWaiter).

use qubit_lock::{AsyncConditionWaiter, TokioMonitor};

/// Runs an immediately ready async wait through a generic capability bound.
async fn wait_through_trait<W>(waiter: &W) -> i32
where
    W: AsyncConditionWaiter<State = bool>,
{
    waiter.wait_until_async(|ready| *ready, |_| 7).await
}

#[tokio::test]
/// Verifies a Tokio monitor satisfies [`AsyncConditionWaiter`].
async fn test_async_condition_waiter_trait_accepts_tokio_monitor() {
    assert_eq!(wait_through_trait(&TokioMonitor::current(true)).await, 7);
}
