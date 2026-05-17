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
    AsyncConditionWaiter,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
    Notifier,
    TokioMonitor,
    WaitTimeoutResult,
    WaitTimeoutStatus,
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
async fn test_tokio_monitor_helpers_and_conversions_delegate_to_state() {
    let monitor = TokioMonitor::from(vec![1]);

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

    let default_monitor = TokioMonitor::<Vec<i32>>::default();
    assert!(default_monitor.async_read(|items| items.is_empty()).await);
}

#[tokio::test]
async fn test_tokio_monitor_traits_delegate_to_monitor_methods() {
    let monitor = TokioMonitor::new(vec![1, 2]);

    <TokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <TokioMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    let waiter = <TokioMonitor<Vec<i32>> as AsyncNotificationWaiter>::async_wait(&monitor);
    tokio::pin!(waiter);
    <TokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    tokio::time::timeout(Duration::from_millis(100), &mut waiter)
        .await
        .expect("async notification wait should complete");

    let timeout_wait = <TokioMonitor<Vec<i32>> as AsyncTimeoutNotificationWaiter>::async_wait_for(
        &monitor,
        Duration::from_secs(1),
    );
    tokio::pin!(timeout_wait);
    <TokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), &mut timeout_wait)
            .await
            .expect("async timeout notification wait should complete"),
        WaitTimeoutStatus::Woken,
    );

    assert_eq!(
        <TokioMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_while(
            &monitor,
            |items| items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        2,
    );
    assert_eq!(
        <TokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        WaitTimeoutResult::Ready(1),
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
async fn test_tokio_monitor_async_wait_while_for_uses_call_time_budget() {
    let monitor = TokioMonitor::new(false);
    let wait = monitor.async_wait_while_for(Duration::from_millis(5), |ready| !*ready, |_| 7);

    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(wait.await, WaitTimeoutResult::TimedOut);
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

#[tokio::test]
async fn test_tokio_monitor_async_wait_while_for_returns_ready_after_notify() {
    let monitor = std::sync::Arc::new(TokioMonitor::new(false));
    let waiter_monitor = std::sync::Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .async_wait_while_for(
                Duration::from_secs(1),
                |ready| !*ready,
                |ready| {
                    *ready = false;
                    9
                },
            )
            .await
    });

    tokio::task::yield_now().await;
    monitor.async_write(|ready| *ready = true).await;
    monitor.notify_one();

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(9),
    );
}

#[tokio::test]
async fn test_tokio_monitor_async_wait_while_for_rechecks_state_after_timeout() {
    let monitor = std::sync::Arc::new(TokioMonitor::new(false));
    let waiter_monitor = std::sync::Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .async_wait_while_for(
                Duration::from_millis(20),
                |ready| !*ready,
                |ready| {
                    *ready = false;
                    9
                },
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    monitor.async_write(|ready| *ready = true).await;

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(9),
    );
}
