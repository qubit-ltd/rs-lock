// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TokioMonitor`](qubit_lock::TokioMonitor).

use std::time::Duration;

use qubit_lock::{
    AsyncConditionWaiter, AsyncTimeoutConditionWaiter, Notifier, TokioMonitor, WaitTimeoutResult,
};

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_helpers_and_conversions_delegate_to_state() {
    let monitor = TokioMonitor::from(vec![1]);

    monitor.with_write_async(|items| items.push(2)).await;
    assert_eq!(
        monitor.with_read_async(|items| items.clone()).await,
        vec![1, 2],
    );

    let one_result = monitor
        .with_write_notify_one_async(|items| {
            items.push(3);
            items.len()
        })
        .await;
    assert_eq!(one_result, 3);

    let all_result = monitor
        .with_write_notify_all_async(|items| {
            items.push(4);
            items.len()
        })
        .await;
    assert_eq!(all_result, 4);

    monitor.notify_one();
    monitor.notify_all();

    let default_monitor = TokioMonitor::<Vec<i32>>::default();
    assert!(
        default_monitor
            .with_read_async(|items| items.is_empty())
            .await
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_traits_delegate_to_monitor_methods() {
    let monitor = TokioMonitor::new(vec![1, 2]);

    <TokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <TokioMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    monitor.with_write_async(|items| items.clear()).await;
    let condition_wait = <TokioMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_while_async(
        &monitor,
        |items| items.is_empty(),
        |items| items.pop().expect("item should be ready"),
    );
    tokio::pin!(condition_wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut condition_wait)
            .await
            .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(2))
        .await;
    assert_eq!(condition_wait.await, 2);

    let timeout_condition_wait =
        <TokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            &monitor,
            Duration::from_secs(1),
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(timeout_condition_wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut timeout_condition_wait)
            .await
            .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(1))
        .await;
    assert_eq!(timeout_condition_wait.await, WaitTimeoutResult::Ready(1),);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_uses_call_time_budget() {
    let monitor = TokioMonitor::new(false);
    let start = tokio::time::Instant::now();
    let wait = monitor.wait_while_for_async(Duration::from_millis(5), |ready| !*ready, |_| 7);

    tokio::time::advance(Duration::from_millis(10)).await;

    assert_eq!(wait.await, WaitTimeoutResult::TimedOut);
    assert_eq!(start.elapsed(), Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_until_runs_action_after_notify() {
    let monitor = std::sync::Arc::new(TokioMonitor::new(false));
    let waiter_monitor = std::sync::Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_until_async(
                |ready| *ready,
                |ready| {
                    *ready = false;
                    7
                },
            )
            .await
    });

    tokio::task::yield_now().await;
    monitor
        .with_write_notify_one_async(|ready| *ready = true)
        .await;

    assert_eq!(waiter.await.expect("waiter task should finish"), 7);
    assert!(!monitor.with_read_async(|ready| *ready).await);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_returns_ready_after_notify() {
    let monitor = std::sync::Arc::new(TokioMonitor::new(false));
    let waiter_monitor = std::sync::Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_while_for_async(
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
    monitor.with_write_async(|ready| *ready = true).await;
    monitor.notify_one();

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(9),
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_rechecks_state_after_timeout() {
    let monitor = std::sync::Arc::new(TokioMonitor::new(false));
    let waiter_monitor = std::sync::Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_while_for_async(
                Duration::from_millis(20),
                |ready| !*ready,
                |ready| {
                    *ready = false;
                    9
                },
            )
            .await
    });

    tokio::time::advance(Duration::from_millis(5)).await;
    monitor.with_write_async(|ready| *ready = true).await;

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(9),
    );
}
