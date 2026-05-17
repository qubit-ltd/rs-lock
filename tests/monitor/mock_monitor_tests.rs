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
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

#[cfg(feature = "async")]
use qubit_lock::AsyncTimeoutNotificationWaiter;
use qubit_lock::{
    MockMonitor, NotificationWaiter, TimeoutConditionWaiter, TimeoutNotificationWaiter,
    WaitTimeoutResult, WaitTimeoutStatus,
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
