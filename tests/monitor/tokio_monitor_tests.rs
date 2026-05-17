/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`TokioMonitor`](qubit_lock::TokioMonitor).

use std::time::Duration;

use qubit_lock::{
    AsyncConditionWaiter, AsyncTimeoutNotificationWaiter, TokioMonitor, WaitTimeoutStatus,
};

#[tokio::test]
async fn test_tokio_monitor_async_wait_for_times_out() {
    let monitor = TokioMonitor::new(false);

    assert_eq!(
        monitor.async_wait_for(Duration::from_millis(1)).await,
        WaitTimeoutStatus::TimedOut,
    );
}

#[tokio::test]
async fn test_tokio_monitor_async_wait_for_uses_call_time_budget() {
    let monitor = TokioMonitor::new(false);
    let wait = monitor.async_wait_for(Duration::from_millis(5));

    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(wait.await, WaitTimeoutStatus::TimedOut);
}

#[tokio::test]
async fn test_tokio_monitor_async_wait_until_runs_action_after_notify() {
    let monitor = std::sync::Arc::new(TokioMonitor::new(false));
    let waiter_monitor = std::sync::Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .async_wait_until(
                |ready| *ready,
                |ready| {
                    *ready = false;
                    7
                },
            )
            .await
    });

    tokio::task::yield_now().await;
    monitor.async_write_notify_one(|ready| *ready = true).await;

    assert_eq!(waiter.await.expect("waiter task should finish"), 7);
    assert!(!monitor.async_read(|ready| *ready).await);
}
