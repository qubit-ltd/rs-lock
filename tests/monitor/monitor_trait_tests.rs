/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for monitor aggregate traits.

use std::time::Duration;

use qubit_lock::{
    ArcMockMonitor,
    ArcParkingLotMonitor,
    Monitor,
    SharedMonitor,
    TimeoutNotificationWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
#[cfg(feature = "async")]
use qubit_lock::{
    ArcTokioMonitor,
    AsyncMonitor,
    SharedAsyncMonitor,
};

/// Exercises a blocking monitor through the aggregate trait.
fn wait_through_monitor_trait<M>(monitor: &M)
where
    M: Monitor<State = bool>,
{
    assert_eq!(
        monitor.wait_until_for(Duration::ZERO, |ready| *ready, |_| 7),
        WaitTimeoutResult::TimedOut,
    );
}

/// Requires a cloneable shared blocking monitor handle.
fn require_shared_monitor<M>(monitor: M) -> M
where
    M: SharedMonitor<State = bool>,
{
    monitor.clone()
}

#[test]
fn test_monitor_trait_accepts_parking_lot_monitor() {
    let monitor = ArcParkingLotMonitor::new(false);

    wait_through_monitor_trait(&monitor);
}

#[test]
fn test_shared_monitor_trait_accepts_mock_monitor_handle() {
    let monitor = ArcMockMonitor::new(false);
    let cloned = require_shared_monitor(monitor);

    assert_eq!(cloned.wait_for(Duration::ZERO), WaitTimeoutStatus::TimedOut);
}

/// Exercises an async monitor through the aggregate trait.
#[cfg(feature = "async")]
async fn wait_through_async_monitor_trait<M>(monitor: &M)
where
    M: AsyncMonitor<State = bool>,
{
    assert_eq!(
        monitor
            .async_wait_until_for(Duration::from_millis(1), |ready| *ready, |_| 7)
            .await,
        WaitTimeoutResult::TimedOut,
    );
}

/// Requires a cloneable shared async monitor handle.
#[cfg(feature = "async")]
fn require_shared_async_monitor<M>(monitor: M) -> M
where
    M: SharedAsyncMonitor<State = bool>,
{
    monitor.clone()
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_async_monitor_trait_accepts_tokio_monitor() {
    let monitor = ArcTokioMonitor::new(false);

    wait_through_async_monitor_trait(&monitor).await;
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_shared_async_monitor_trait_accepts_tokio_monitor_handle() {
    let monitor = ArcTokioMonitor::new(false);
    let cloned = require_shared_async_monitor(monitor);

    assert!(!cloned.async_read(|ready| *ready).await);
}
