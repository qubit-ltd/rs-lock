// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncTimedMonitor`](qubit_lock::AsyncTimedMonitor).

use std::sync::Arc;
use std::time::Duration;

use qubit_lock::AsyncTimedMonitor;
use qubit_lock::TokioMonitor;
use qubit_lock::WaitTimeoutResult;

/// Exercises timed waiting through the aggregate async timed capability.
async fn wait_through_trait<M>(monitor: &M)
where
    M: AsyncTimedMonitor<State = bool>,
{
    assert_time_result_eq!(
        monitor
            .wait_until_for_async(Duration::from_millis(1), |ready| *ready, |_| 7,)
            .await,
        Ok(WaitTimeoutResult::TimedOut),
    );
}

/// Verifies a Tokio handle satisfies [`AsyncTimedMonitor`].
#[tokio::test(start_paused = true)]
async fn test_async_timed_monitor_trait_accepts_tokio_monitor() {
    wait_through_trait(&Arc::new(TokioMonitor::current(false))).await;
}

/// Verifies [`Arc`] forwards the aggregate async timed monitor capability.
#[tokio::test(start_paused = true)]
async fn test_async_timed_monitor_trait_accepts_arc_forwarding() {
    let monitor = Arc::new(TokioMonitor::current(false));
    wait_through_trait(&monitor).await;
}
