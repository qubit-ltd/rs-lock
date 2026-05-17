/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ArcTokioMonitor`](qubit_lock::ArcTokioMonitor).

use std::time::Duration;

use qubit_lock::{ArcTokioMonitor, AsyncTimeoutConditionWaiter, WaitTimeoutResult};

#[tokio::test]
async fn test_arc_tokio_monitor_clone_shares_state() {
    let monitor = ArcTokioMonitor::new(Vec::<i32>::new());
    let cloned = monitor.clone();

    cloned.async_write(|items| items.push(7)).await;

    assert_eq!(monitor.async_read(|items| items.clone()).await, vec![7]);
}

#[tokio::test]
async fn test_arc_tokio_monitor_async_wait_until_for_times_out() {
    let monitor = ArcTokioMonitor::new(false);

    assert_eq!(
        monitor
            .async_wait_until_for(Duration::from_millis(1), |ready| *ready, |_| 7)
            .await,
        WaitTimeoutResult::TimedOut,
    );
}
