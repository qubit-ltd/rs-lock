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

use qubit_lock::{
    ArcTokioMonitor,
    AsyncConditionWaiter,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
    Notifier,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

#[tokio::test]
async fn test_arc_tokio_monitor_clone_shares_state() {
    let monitor = ArcTokioMonitor::new(Vec::<i32>::new());
    let cloned = monitor.clone();

    cloned.async_write(|items| items.push(7)).await;

    assert_eq!(monitor.async_read(|items| items.clone()).await, vec![7]);
}

#[tokio::test]
async fn test_arc_tokio_monitor_helpers_and_conversions_delegate_to_inner_monitor() {
    let monitor = ArcTokioMonitor::from(vec![1]);

    monitor.async_write(|items| items.push(2)).await;
    assert_eq!(monitor.async_read(|items| items.clone()).await, vec![1, 2]);

    let one_result = monitor
        .async_write_notify_one(|items| {
            items.push(3);
            items.len()
        })
        .await;
    assert_eq!(one_result, 3);

    let all_result = monitor
        .async_write_notify_all(|items| {
            items.push(4);
            items.len()
        })
        .await;
    assert_eq!(all_result, 4);

    monitor.notify_one();
    monitor.notify_all();
    assert_eq!(monitor.as_ref().async_read(|items| items.len()).await, 4);
    assert_eq!((*monitor).async_read(|items| items.len()).await, 4);

    let default_monitor = ArcTokioMonitor::<Vec<i32>>::default();
    assert!(default_monitor.async_read(|items| items.is_empty()).await);
}

#[tokio::test]
async fn test_arc_tokio_monitor_traits_delegate_to_inner_monitor() {
    let monitor = ArcTokioMonitor::new(vec![1, 2]);

    <ArcTokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <ArcTokioMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    let waiter = <ArcTokioMonitor<Vec<i32>> as AsyncNotificationWaiter>::async_wait(&monitor);
    tokio::pin!(waiter);
    <ArcTokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    tokio::time::timeout(Duration::from_millis(100), &mut waiter)
        .await
        .expect("async notification wait should complete");

    let timeout_wait =
        <ArcTokioMonitor<Vec<i32>> as AsyncTimeoutNotificationWaiter>::async_wait_for(
            &monitor,
            Duration::from_secs(1),
        );
    tokio::pin!(timeout_wait);
    <ArcTokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), &mut timeout_wait)
            .await
            .expect("async timeout notification wait should complete"),
        WaitTimeoutStatus::Woken,
    );

    assert_eq!(
        <ArcTokioMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_until(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        2,
    );
    assert_eq!(
        <ArcTokioMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_while(
            &monitor,
            |items| items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        1,
    );

    monitor.async_write(|items| items.push(3)).await;
    assert_eq!(
        <ArcTokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        WaitTimeoutResult::Ready(3),
    );
    monitor.async_write(|items| items.push(4)).await;
    assert_eq!(
        <ArcTokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_while_for(
            &monitor,
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        )
        .await,
        WaitTimeoutResult::Ready(Some(4)),
    );
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
