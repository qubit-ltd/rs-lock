/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ArcMockMonitor`](qubit_lock::ArcMockMonitor).

use std::{
    sync::mpsc,
    thread,
    time::Duration,
};

use qubit_lock::{
    ArcMockMonitor,
    ConditionWaiter,
    NotificationWaiter,
    Notifier,
    TimeoutConditionWaiter,
    TimeoutNotificationWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
#[cfg(feature = "async")]
use qubit_lock::{
    AsyncConditionWaiter,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
};

#[test]
fn test_arc_mock_monitor_clone_shares_state_and_mock_time() {
    let monitor = ArcMockMonitor::new(Vec::<i32>::new());
    let cloned = monitor.clone();

    cloned.write(|items| items.push(7));
    monitor.advance(Duration::from_millis(5));

    assert_eq!(monitor.read(|items| items.clone()), vec![7]);
    assert_eq!(cloned.elapsed(), Duration::from_millis(5));
}

#[test]
fn test_arc_mock_monitor_wait_for_times_out_after_advance() {
    let monitor = ArcMockMonitor::new(false);
    monitor.advance(Duration::from_millis(10));

    assert_eq!(
        monitor.wait_for(Duration::from_millis(0)),
        WaitTimeoutStatus::TimedOut,
    );
}

#[test]
fn test_arc_mock_monitor_helpers_and_conversions_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::from(false);

    monitor.set_elapsed(Duration::from_millis(7));
    assert_eq!(monitor.elapsed(), Duration::from_millis(7));

    monitor.reset_elapsed();
    assert_eq!(monitor.elapsed(), Duration::ZERO);

    let one_result = monitor.write_notify_one(|ready| {
        *ready = true;
        1
    });
    assert_eq!(one_result, 1);

    let all_result = monitor.write_notify_all(|ready| {
        *ready = false;
        2
    });
    assert_eq!(all_result, 2);
    assert!(!monitor.read(|ready| *ready));

    monitor.notify_one();
    monitor.notify_all();
    assert_eq!(monitor.as_ref().elapsed(), Duration::ZERO);
    assert_eq!((*monitor).elapsed(), Duration::ZERO);

    let default_monitor = ArcMockMonitor::<Vec<i32>>::default();
    assert!(default_monitor.read(|items| items.is_empty()));
}

#[test]
fn test_arc_mock_monitor_traits_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(vec![1, 2]);

    <ArcMockMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <ArcMockMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as TimeoutNotificationWaiter>::wait_for(&monitor, Duration::ZERO,),
        WaitTimeoutStatus::TimedOut,
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as ConditionWaiter>::wait_until(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        2,
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as ConditionWaiter>::wait_while(
            &monitor,
            |items| items.is_empty(),
            |items| {
                items.push(3);
                items.len()
            },
        ),
        2,
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_while_for(
            &monitor,
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        WaitTimeoutResult::Ready(Some(1)),
    );
}

#[test]
fn test_arc_mock_monitor_notification_waiter_trait_wait_returns_after_notify() {
    let monitor = ArcMockMonitor::new(false);
    let waiter_monitor = monitor.clone();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        <ArcMockMonitor<bool> as NotificationWaiter>::wait(&waiter_monitor);
        done_tx.send(()).expect("test should receive wait result");
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        <ArcMockMonitor<bool> as Notifier>::notify_all(&monitor);
        if done_rx.recv_timeout(Duration::from_millis(5)).is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "notification wait should complete before deadline",
        );
    }
    waiter.join().expect("waiter should finish");
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_arc_mock_monitor_async_traits_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(vec![1, 2]);
    let waiter_monitor = monitor.clone();

    let waiter = tokio::spawn(async move {
        <ArcMockMonitor<Vec<i32>> as AsyncNotificationWaiter>::async_wait(&waiter_monitor).await;
    });
    tokio::task::yield_now().await;
    <ArcMockMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);
    tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .expect("async notification wait should complete")
        .expect("waiter task should finish");

    let wait = <ArcMockMonitor<Vec<i32>> as AsyncTimeoutNotificationWaiter>::async_wait_for(
        &monitor,
        Duration::from_secs(1),
    );
    tokio::pin!(wait);
    <ArcMockMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), &mut wait)
            .await
            .expect("async timeout notification wait should complete"),
        WaitTimeoutStatus::Woken,
    );

    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_until(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        2,
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_while(
            &monitor,
            |items| items.is_empty(),
            |items| {
                items.push(3);
                items.len()
            },
        )
        .await,
        2,
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_while_for(
            &monitor,
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        )
        .await,
        WaitTimeoutResult::Ready(Some(1)),
    );
}
