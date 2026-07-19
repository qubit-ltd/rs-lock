// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncMonitor`](qubit_lock::AsyncMonitor).

use std::time::Duration;

use qubit_lock::{
    ArcTokioMonitor,
    AsyncMonitor,
    WaitTimeoutResult,
};

/// Exercises timed waiting through the aggregate async capability.
async fn wait_through_trait<M>(monitor: &M)
where
    M: AsyncMonitor<State = bool>,
{
    assert_time_result_eq!(
        monitor
            .wait_until_for_async(
                Duration::from_millis(1),
                |ready| *ready,
                |_| 7,
            )
            .await,
        Ok(WaitTimeoutResult::TimedOut),
    );
}

#[tokio::test(start_paused = true)]
/// Verifies a Tokio handle satisfies [`AsyncMonitor`].
async fn test_async_monitor_trait_accepts_tokio_monitor() {
    wait_through_trait(&ArcTokioMonitor::new(false)).await;
}
