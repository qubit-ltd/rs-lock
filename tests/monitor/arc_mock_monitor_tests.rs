// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ArcMockMonitor`](qubit_lock::ArcMockMonitor).

use std::{thread, time::Duration};

use qubit_clock::ManualMonotonicClock;
use qubit_lock::{
    ArcMockMonitor, ConditionWaiter, Notifier, TimeoutConditionWaiter, WaitTimeoutResult,
};
#[cfg(feature = "async")]
use qubit_lock::{AsyncConditionWaiter, AsyncTimeoutConditionWaiter};

#[test]
fn test_arc_mock_monitor_clone_shares_state_and_mock_time() {
    let monitor = ArcMockMonitor::new(Vec::<i32>::new());
    let cloned = monitor.clone();

    cloned.with_write(|items| items.push(7));
    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(5))
        .expect("manual clock should advance");

    assert_eq!(monitor.with_read(|items| items.clone()), vec![7]);
    assert_eq!(cloned.elapsed(), Duration::from_millis(5));
}

#[test]
fn test_arc_mock_monitor_from_clock_shares_external_clock() {
    let clock = std::sync::Arc::new(ManualMonotonicClock::new());
    let monitor = ArcMockMonitor::from_clock(false, std::sync::Arc::clone(&clock));

    clock
        .advance(Duration::from_secs(2))
        .expect("manual clock should advance");

    assert_eq!(Duration::from_secs(2), monitor.elapsed());
}

#[test]
fn test_arc_mock_monitor_timeout_waiter_helpers_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(false);
    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until_for(Duration::from_secs(1), |ready| *ready, |_| ())
    });

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    assert_eq!(1, monitor.pending_timeout_waiters());
    monitor
        .monotonic_clock()
        .advance(Duration::from_secs(1))
        .expect("manual clock should advance");

    assert_eq!(WaitTimeoutResult::TimedOut, waiter.join().unwrap());
    assert_eq!(0, monitor.pending_timeout_waiters());
}

#[test]
fn test_arc_mock_monitor_helpers_and_conversions_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::from(false);

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(7))
        .expect("manual clock should advance");
    assert_eq!(monitor.elapsed(), Duration::from_millis(7));

    let one_result = monitor.with_write_notify_one(|ready| {
        *ready = true;
        1
    });
    assert_eq!(one_result, 1);

    let all_result = monitor.with_write_notify_all(|ready| {
        *ready = false;
        2
    });
    assert_eq!(all_result, 2);
    assert!(!monitor.with_read(|ready| *ready));

    monitor.notify_one();
    monitor.notify_all();
    assert_eq!(monitor.as_ref().elapsed(), Duration::from_millis(7));
    assert_eq!((*monitor).elapsed(), Duration::from_millis(7));

    let default_monitor = ArcMockMonitor::<Vec<i32>>::default();
    assert!(default_monitor.with_read(|items| items.is_empty()));
}

#[test]
fn test_arc_mock_monitor_traits_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(vec![1, 2]);

    <ArcMockMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <ArcMockMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

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
fn test_arc_mock_monitor_wait_methods_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(vec![1, 2]);

    assert_eq!(
        monitor.wait_until(
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        2,
    );
    assert_eq!(
        monitor.wait_while(
            |items| items.is_empty(),
            |items| {
                items.push(3);
                items.len()
            },
        ),
        2,
    );
    assert_eq!(
        monitor.wait_until_for(
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        monitor.wait_while_for(
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        WaitTimeoutResult::Ready(Some(1)),
    );
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_arc_mock_monitor_async_traits_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(vec![1, 2]);

    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_until_async(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        2,
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_while_async(
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
        <ArcMockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        <ArcMockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
            &monitor,
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
async fn test_arc_mock_monitor_async_wait_methods_delegate_to_inner_monitor() {
    let monitor = ArcMockMonitor::new(vec![1, 2]);

    assert_eq!(
        monitor
            .wait_until_async(
                |items| !items.is_empty(),
                |items| items.pop().expect("item should be ready"),
            )
            .await,
        2,
    );
    assert_eq!(
        monitor
            .wait_while_async(
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
        monitor
            .wait_until_for_async(
                Duration::ZERO,
                |items| !items.is_empty(),
                |items| items.pop().expect("item should be ready"),
            )
            .await,
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        monitor
            .wait_while_for_async(
                Duration::ZERO,
                |items| items.is_empty(),
                |items| items.pop(),
            )
            .await,
        WaitTimeoutResult::Ready(Some(1)),
    );
}
