/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`MockMonitor`](qubit_lock::MockMonitor).

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

#[cfg(feature = "async")]
use qubit_lock::{
    AsyncConditionWaiter,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
};
use qubit_lock::{
    ConditionWaiter,
    MockMonitor,
    NotificationWaiter,
    Notifier,
    TimeoutConditionWaiter,
    TimeoutNotificationWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

#[test]
fn test_mock_monitor_wait_for_uses_mock_elapsed_time() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        let status = waiter_monitor.wait_for(Duration::from_millis(100));
        done_tx
            .send(status)
            .expect("test should receive wait status");
    });

    thread::sleep(Duration::from_millis(20));
    assert!(done_rx.try_recv().is_err());

    monitor.advance(Duration::from_millis(99));
    assert!(done_rx.try_recv().is_err());

    monitor.advance(Duration::from_millis(1));
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("mock timeout should complete after mock time advances"),
        WaitTimeoutStatus::TimedOut,
    );
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_mock_monitor_wait_for_returns_woken_after_notification() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        let status = waiter_monitor.wait_for(Duration::from_millis(100));
        done_tx
            .send(status)
            .expect("test should receive wait status");
    });

    thread::sleep(Duration::from_millis(10));
    monitor.notify_one();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("notification should wake waiter"),
        WaitTimeoutStatus::Woken,
    );
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_mock_monitor_wait_blocks_until_notification() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        waiter_monitor.wait();
        done_tx.send(()).expect("test should receive wait result");
    });

    thread::sleep(Duration::from_millis(10));
    assert!(done_rx.try_recv().is_err());

    monitor.notify_all();
    done_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("notification should wake waiter");
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_mock_monitor_elapsed_helpers_and_conversions() {
    let monitor = MockMonitor::from(false);

    monitor.set_elapsed(Duration::from_millis(7));
    assert_eq!(monitor.elapsed(), Duration::from_millis(7));

    monitor.reset_elapsed();
    assert_eq!(monitor.elapsed(), Duration::ZERO);

    let result = monitor.write_notify_all(|ready| {
        *ready = true;
        11
    });
    assert_eq!(result, 11);
    assert!(monitor.read(|ready| *ready));

    let default_monitor = MockMonitor::<Vec<i32>>::default();
    assert!(default_monitor.read(|items| items.is_empty()));
}

#[test]
fn test_mock_monitor_traits_delegate_to_monitor_methods() {
    let monitor = MockMonitor::new(vec![1, 2]);

    <MockMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <MockMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    assert_eq!(
        <MockMonitor<Vec<i32>> as TimeoutNotificationWaiter>::wait_for(&monitor, Duration::ZERO,),
        WaitTimeoutStatus::TimedOut,
    );

    assert_eq!(
        <MockMonitor<Vec<i32>> as ConditionWaiter>::wait_until(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        2,
    );
    assert_eq!(
        <MockMonitor<Vec<i32>> as ConditionWaiter>::wait_while(
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
        <MockMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        <MockMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_while_for(
            &monitor,
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        WaitTimeoutResult::Ready(Some(1)),
    );
}

#[test]
fn test_mock_monitor_wait_while_blocks_until_notification() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        let result = <MockMonitor<bool> as ConditionWaiter>::wait_while(
            waiter_monitor.as_ref(),
            |ready| !*ready,
            |ready| {
                *ready = false;
                17
            },
        );
        done_tx
            .send(result)
            .expect("test should receive wait result");
    });

    thread::sleep(Duration::from_millis(10));
    monitor.write_notify_all(|ready| *ready = true);

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notification"),
        17,
    );
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_mock_monitor_notification_waiter_trait_wait_returns_after_notify() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        <MockMonitor<bool> as NotificationWaiter>::wait(waiter_monitor.as_ref());
        done_tx.send(()).expect("test should receive wait result");
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        <MockMonitor<bool> as Notifier>::notify_all(monitor.as_ref());
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

#[test]
fn test_mock_monitor_wait_until_for_times_out_on_mock_time() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        let result =
            waiter_monitor.wait_until_for(Duration::from_millis(50), |ready| *ready, |_| 7);
        done_tx
            .send(result)
            .expect("test should receive wait result");
    });

    thread::sleep(Duration::from_millis(10));
    assert!(done_rx.try_recv().is_err());

    monitor.advance(Duration::from_millis(50));
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("mock timeout should complete"),
        WaitTimeoutResult::TimedOut,
    );
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_mock_monitor_wait_until_runs_action_after_notification() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        let result = waiter_monitor.wait_until_for(
            Duration::from_millis(100),
            |ready| *ready,
            |ready| {
                *ready = false;
                7
            },
        );
        done_tx
            .send(result)
            .expect("test should receive wait result");
    });

    thread::sleep(Duration::from_millis(10));
    monitor.write_notify_one(|ready| *ready = true);

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("condition should become ready"),
        WaitTimeoutResult::Ready(7),
    );
    assert!(!monitor.read(|ready| *ready));
    waiter.join().expect("waiter should finish");
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_wait_for_uses_mock_elapsed_time() {
    let monitor = MockMonitor::new(false);
    let wait = monitor.async_wait_for(Duration::from_millis(100));
    tokio::pin!(wait);

    monitor.advance(Duration::from_millis(99));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut wait)
            .await
            .is_err(),
        "mock async timeout should not use real elapsed time",
    );

    monitor.advance(Duration::from_millis(1));
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(50), &mut wait)
            .await
            .expect("mock async wait should complete after mock time advances"),
        WaitTimeoutStatus::TimedOut,
    );
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_traits_delegate_to_monitor_methods() {
    let monitor = Arc::new(MockMonitor::new(vec![1, 2]));

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = tokio::spawn(async move {
        <MockMonitor<Vec<i32>> as AsyncNotificationWaiter>::async_wait(waiter_monitor.as_ref())
            .await;
    });
    tokio::task::yield_now().await;
    <MockMonitor<Vec<i32>> as Notifier>::notify_all(monitor.as_ref());
    tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .expect("async notification wait should complete")
        .expect("waiter task should finish");

    let wait = <MockMonitor<Vec<i32>> as AsyncTimeoutNotificationWaiter>::async_wait_for(
        monitor.as_ref(),
        Duration::from_secs(1),
    );
    tokio::pin!(wait);
    <MockMonitor<Vec<i32>> as Notifier>::notify_one(monitor.as_ref());
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), &mut wait)
            .await
            .expect("async timeout notification wait should complete"),
        WaitTimeoutStatus::Woken,
    );

    assert_eq!(
        <MockMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_until(
            monitor.as_ref(),
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        2,
    );
    assert_eq!(
        <MockMonitor<Vec<i32>> as AsyncConditionWaiter>::async_wait_while(
            monitor.as_ref(),
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
        <MockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_until_for(
            monitor.as_ref(),
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        <MockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::async_wait_while_for(
            monitor.as_ref(),
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        )
        .await,
        WaitTimeoutResult::Ready(Some(1)),
    );
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_wait_while_waits_for_notification() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        <MockMonitor<bool> as AsyncConditionWaiter>::async_wait_while(
            waiter_monitor.as_ref(),
            |ready| !*ready,
            |ready| {
                *ready = false;
                17
            },
        )
        .await
    });

    tokio::task::yield_now().await;
    monitor.write_notify_all(|ready| *ready = true);

    assert_eq!(waiter.await.expect("waiter task should finish"), 17);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_wait_while_for_waits_for_mock_change() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        <MockMonitor<bool> as AsyncTimeoutConditionWaiter>::async_wait_while_for(
            waiter_monitor.as_ref(),
            Duration::from_secs(1),
            |ready| !*ready,
            |ready| {
                *ready = false;
                17
            },
        )
        .await
    });

    tokio::task::yield_now().await;
    monitor.write_notify_all(|ready| *ready = true);

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(17),
    );
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_wait_until_for_times_out_on_mock_elapsed() {
    let monitor = MockMonitor::new(false);

    assert_eq!(
        <MockMonitor<bool> as AsyncTimeoutConditionWaiter>::async_wait_until_for(
            &monitor,
            Duration::ZERO,
            |ready| *ready,
            |_| 17,
        )
        .await,
        WaitTimeoutResult::TimedOut,
    );
}
