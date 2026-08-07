// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncMonitor`](qubit_lock::AsyncMonitor).

use std::sync::Arc;

use qubit_lock::AsyncMonitor;
use qubit_lock::TokioMonitor;

/// Exercises state access and untimed waiting through the aggregate capability.
async fn use_async_monitor<M>(monitor: &M)
where
    M: AsyncMonitor<State = bool>,
{
    AsyncMonitor::with_write_async(monitor, |ready| *ready = true).await;
    assert!(AsyncMonitor::with_read_async(monitor, |ready| *ready).await);
    AsyncMonitor::with_write_notify_one_async(monitor, |ready| {
        *ready = false;
    })
    .await;
    AsyncMonitor::with_write_notify_all_async(monitor, |ready| {
        *ready = true;
    })
    .await;
    assert_eq!(monitor.wait_until_async(|ready| *ready, |_| 7).await, 7,);
}

/// Verifies a Tokio handle satisfies [`AsyncMonitor`].
#[tokio::test(start_paused = true)]
async fn test_async_monitor_trait_accepts_tokio_monitor() {
    use_async_monitor(&Arc::new(TokioMonitor::current(false))).await;
}

/// Verifies [`Arc`] forwards the aggregate async monitor capability.
#[tokio::test(start_paused = true)]
async fn test_async_monitor_trait_accepts_arc_forwarding() {
    let monitor = Arc::new(TokioMonitor::current(false));
    use_async_monitor(&monitor).await;
}
