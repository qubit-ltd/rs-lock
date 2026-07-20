// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ArcTokioMonitor`](qubit_lock::ArcTokioMonitor).

use std::{
    sync::Arc,
    time::Duration,
};

use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
    TokioRuntimeError,
};
use qubit_lock::{
    ArcTokioMonitor,
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
    Notifier,
    TokioMonitor,
    WaitTimeoutResult,
};

/// Verifies that fallible shared-monitor construction reports a missing
/// runtime.
#[test]
fn test_arc_tokio_monitor_try_current_reports_missing_runtime() {
    let error = match ArcTokioMonitor::try_current(false) {
        Ok(_) => panic!("construction outside a runtime should fail"),
        Err(error) => error,
    };

    assert!(matches!(error, TokioRuntimeError::NotEntered { .. }));
}

/// Verifies that infallible shared-monitor construction identifies its runtime
/// requirement in the panic message.
#[test]
#[should_panic(expected = "cannot create Arc-wrapped Tokio monitor")]
fn test_arc_tokio_monitor_current_panics_outside_runtime() {
    let _monitor = ArcTokioMonitor::current(false);
}

#[test]
fn test_arc_tokio_monitor_with_timer_preserves_timer_domain() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = ArcTokioMonitor::with_timer(1usize, clock.new_timer());

    assert_eq!(clock.now().domain(), monitor.timer().clock().now().domain());
}

#[tokio::test(start_paused = true)]
async fn test_arc_tokio_monitor_preserves_inner_arc_identity() {
    let inner = Arc::new(TokioMonitor::current(1usize));
    let monitor = ArcTokioMonitor::from_arc(Arc::clone(&inner));

    assert!(Arc::ptr_eq(&inner, monitor.as_arc()));

    let cloned = monitor.clone();
    assert!(Arc::ptr_eq(monitor.as_arc(), cloned.as_arc()));
    cloned.with_write_async(|value| *value += 1).await;
    let recovered = cloned.into_arc();

    assert!(Arc::ptr_eq(&inner, &recovered));
    assert_eq!(monitor.with_read_async(|value| *value).await, 2);
}

#[tokio::test(start_paused = true)]
async fn test_arc_tokio_monitor_clone_shares_state() {
    let monitor = ArcTokioMonitor::current(Vec::<i32>::new());
    let cloned = monitor.clone();

    cloned.with_write_async(|items| items.push(7)).await;

    assert_eq!(
        monitor.with_read_async(|items| items.clone()).await,
        vec![7]
    );
}

#[tokio::test(start_paused = true)]
async fn test_arc_tokio_monitor_helpers_delegate_to_inner_monitor() {
    let monitor = ArcTokioMonitor::current(vec![1]);

    monitor.with_write_async(|items| items.push(2)).await;
    assert_eq!(
        monitor.with_read_async(|items| items.clone()).await,
        vec![1, 2]
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
    assert_eq!(
        monitor.as_ref().with_read_async(|items| items.len()).await,
        4
    );
    assert_eq!((*monitor).with_read_async(|items| items.len()).await, 4);

    let empty_monitor = ArcTokioMonitor::current(Vec::<i32>::default());
    assert!(
        empty_monitor
            .with_read_async(|items| items.is_empty())
            .await
    );
}

#[tokio::test(start_paused = true)]
async fn test_arc_tokio_monitor_traits_delegate_to_inner_monitor() {
    let monitor = ArcTokioMonitor::current(vec![1, 2]);

    <ArcTokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <ArcTokioMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    monitor.with_write_async(|items| items.clear()).await;
    let condition_until_wait =
        <ArcTokioMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_until_async(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(condition_until_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut condition_until_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(2))
        .await;
    assert_eq!(condition_until_wait.await, 2);

    let condition_while_wait =
        <ArcTokioMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_while_async(
            &monitor,
            |items| items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(condition_while_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut condition_while_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(1))
        .await;
    assert_eq!(condition_while_wait.await, 1);

    let timeout_until_wait =
        <ArcTokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            &monitor,
            Duration::from_secs(1),
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(timeout_until_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut timeout_until_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(3))
        .await;
    assert_time_result_eq!(
        timeout_until_wait.await,
        Ok(WaitTimeoutResult::Ready(3)),
    );

    let timeout_while_wait =
        <ArcTokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
            &monitor,
            Duration::from_secs(1),
            |items| items.is_empty(),
            |items| items.pop(),
        );
    tokio::pin!(timeout_while_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut timeout_while_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_all_async(|items| items.push(4))
        .await;
    assert_time_result_eq!(
        timeout_while_wait.await,
        Ok(WaitTimeoutResult::Ready(Some(4))),
    );
}

#[tokio::test(start_paused = true)]
async fn test_arc_tokio_monitor_wait_methods_delegate_to_inner_monitor() {
    let monitor = ArcTokioMonitor::current(vec![1, 2]);

    monitor.with_write_async(|items| items.clear()).await;
    let condition_until_wait = monitor.wait_until_async(
        |items| !items.is_empty(),
        |items| items.pop().expect("item should be ready"),
    );
    tokio::pin!(condition_until_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut condition_until_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(2))
        .await;
    assert_eq!(condition_until_wait.await, 2);

    let condition_while_wait = monitor.wait_while_async(
        |items| items.is_empty(),
        |items| items.pop().expect("item should be ready"),
    );
    tokio::pin!(condition_while_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut condition_while_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(1))
        .await;
    assert_eq!(condition_while_wait.await, 1);

    let timeout_until_wait = monitor.wait_until_for_async(
        Duration::from_secs(1),
        |items| !items.is_empty(),
        |items| items.pop().expect("item should be ready"),
    );
    tokio::pin!(timeout_until_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut timeout_until_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(3))
        .await;
    assert_time_result_eq!(
        timeout_until_wait.await,
        Ok(WaitTimeoutResult::Ready(3)),
    );

    let timeout_while_wait = monitor.wait_while_for_async(
        Duration::from_secs(1),
        |items| items.is_empty(),
        |items| items.pop(),
    );
    tokio::pin!(timeout_while_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut timeout_while_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_all_async(|items| items.push(4))
        .await;
    assert_time_result_eq!(
        timeout_while_wait.await,
        Ok(WaitTimeoutResult::Ready(Some(4))),
    );
}

#[tokio::test(start_paused = true)]
async fn test_arc_tokio_monitor_async_wait_until_for_times_out() {
    let monitor = ArcTokioMonitor::current(false);

    assert_time_result_eq!(
        monitor
            .wait_until_for_async(
                Duration::from_millis(1),
                |ready| *ready,
                |_| 7
            )
            .await,
        Ok(WaitTimeoutResult::TimedOut),
    );
}
