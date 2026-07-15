// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`MockMonitor`](qubit_lock::MockMonitor).

use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

#[cfg(feature = "async")]
use std::task::Poll;

use qubit_clock::ManualMonotonicClock;
#[cfg(feature = "async")]
use qubit_lock::{AsyncConditionWaiter, AsyncTimeoutConditionWaiter};
use qubit_lock::{
    ConditionWaiter, MockMonitor, Notifier, TimeoutConditionWaiter, WaitTimeoutResult,
};

/// Verifies that advancing a shared clock from a state closure does not
/// re-enter the monitor state lock indefinitely.
#[test]
fn test_mock_monitor_clock_can_advance_inside_state_closure() {
    const REAL_TIMEOUT: Duration = Duration::from_secs(1);

    let clock = Arc::new(ManualMonotonicClock::new());
    let monitor = Arc::new(MockMonitor::from_clock((), Arc::clone(&clock)));
    let worker_monitor = Arc::clone(&monitor);
    let worker_clock = Arc::clone(&clock);
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        worker_monitor.with_write(|_| {
            worker_clock
                .advance(Duration::from_millis(1))
                .expect("manual clock should advance inside state closure");
        });
        done_tx
            .send(())
            .expect("test should receive closure completion");
    });

    done_rx
        .recv_timeout(REAL_TIMEOUT)
        .expect("clock advance inside state closure should not deadlock");
    worker.join().expect("state closure worker should finish");
}

#[test]
fn test_mock_monitor_from_clock_uses_shared_manual_time() {
    let clock = Arc::new(ManualMonotonicClock::new());
    let monitor = Arc::new(MockMonitor::from_clock(false, Arc::clone(&clock)));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let result = waiter_monitor.wait_until_for(Duration::from_secs(10), |ready| *ready, |_| ());
        done_tx
            .send(result)
            .expect("test should receive wait status");
    });

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    clock
        .advance(Duration::from_secs(10))
        .expect("manual clock should advance");

    assert_eq!(
        WaitTimeoutResult::TimedOut,
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("shared clock should wake mock monitor"),
    );
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_shared_clock_drives_multiple_mock_monitors() {
    let clock = Arc::new(ManualMonotonicClock::new());
    let first = Arc::new(MockMonitor::from_clock((), Arc::clone(&clock)));
    let second = Arc::new(MockMonitor::from_clock((), Arc::clone(&clock)));
    let (done_tx, done_rx) = mpsc::channel();

    let first_waiter = {
        let monitor = Arc::clone(&first);
        let done_tx = done_tx.clone();
        thread::spawn(move || {
            let result = monitor.wait_until_for(Duration::from_secs(5), |_| false, |_| ());
            done_tx
                .send(result)
                .expect("first status should be received");
        })
    };
    let second_waiter = {
        let monitor = Arc::clone(&second);
        thread::spawn(move || {
            let result = monitor.wait_until_for(Duration::from_secs(5), |_| false, |_| ());
            done_tx
                .send(result)
                .expect("second status should be received");
        })
    };

    assert!(first.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    assert!(second.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    clock
        .advance(Duration::from_secs(5))
        .expect("shared manual clock should advance");

    for _ in 0..2 {
        assert_eq!(
            WaitTimeoutResult::TimedOut,
            done_rx
                .recv_timeout(Duration::from_millis(100))
                .expect("both monitors should observe the shared clock"),
        );
    }
    first_waiter.join().expect("first waiter should finish");
    second_waiter.join().expect("second waiter should finish");
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_timeout_uses_shared_manual_time() {
    let clock = Arc::new(ManualMonotonicClock::new());
    let monitor = MockMonitor::from_clock(false, Arc::clone(&clock));
    let mut wait = monitor.wait_until_for_async(Duration::from_secs(10), |ready| *ready, |_| ());
    assert_eq!(0, monitor.pending_timeout_waiters());
    assert!(
        std::future::poll_fn(|context| { Poll::Ready(wait.as_mut().poll(context)) })
            .await
            .is_pending(),
    );
    assert_eq!(1, monitor.pending_timeout_waiters());

    clock
        .advance(Duration::from_secs(10))
        .expect("manual clock should advance");

    assert_eq!(WaitTimeoutResult::TimedOut, wait.await);
    assert_eq!(0, monitor.pending_timeout_waiters());
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_cancelled_async_timeout_unregisters_waiter() {
    let monitor = MockMonitor::new(false);
    let mut wait = monitor.wait_until_for_async(Duration::from_secs(10), |ready| *ready, |_| ());
    assert_eq!(0, monitor.pending_timeout_waiters());
    assert!(
        std::future::poll_fn(|context| { Poll::Ready(wait.as_mut().poll(context)) })
            .await
            .is_pending(),
    );
    assert_eq!(1, monitor.pending_timeout_waiters());

    drop(wait);

    assert_eq!(0, monitor.pending_timeout_waiters());
}

/// Verifies that broadcasting the internal coordination signal does not let
/// an unselected condition waiter observe state changed for `notify_one`.
#[test]
fn test_mock_monitor_notify_one_releases_only_one_blocking_condition_waiter() {
    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    let monitor = Arc::new(MockMonitor::new(false));
    let (done_tx, done_rx) = mpsc::channel();
    let mut waiters = Vec::new();

    for waiter_id in 0..2 {
        let waiter_monitor = Arc::clone(&monitor);
        let done_tx = done_tx.clone();
        waiters.push(thread::spawn(move || {
            let result = waiter_monitor.wait_until_for(WAIT_TIMEOUT, |ready| *ready, |_| waiter_id);
            done_tx
                .send(result)
                .expect("test should receive condition result");
        }));
    }
    drop(done_tx);

    assert!(monitor.wait_for_timeout_waiters(2, Duration::from_secs(1)));
    monitor.with_write_notify_one(|ready| *ready = true);

    assert!(matches!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("one condition waiter should become ready"),
        WaitTimeoutResult::Ready(_),
    ));
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "notify_one should leave the unselected condition waiter blocked",
    );

    monitor.notify_all();
    assert!(matches!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("notify_all should release the remaining waiter"),
        WaitTimeoutResult::Ready(_),
    ));
    for waiter in waiters {
        waiter.join().expect("waiter should finish");
    }
}

#[test]
fn test_mock_monitor_elapsed_helpers_and_conversions() {
    let monitor = MockMonitor::from(false);

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(7))
        .expect("manual clock should advance");
    assert_eq!(monitor.elapsed(), Duration::from_millis(7));

    let result = monitor.with_write_notify_all(|ready| {
        *ready = true;
        11
    });
    assert_eq!(result, 11);
    assert!(monitor.with_read(|ready| *ready));

    let default_monitor = MockMonitor::<Vec<i32>>::default();
    assert!(default_monitor.with_read(|items| items.is_empty()));
}

#[test]
fn test_mock_monitor_traits_delegate_to_monitor_methods() {
    let monitor = MockMonitor::new(vec![1, 2]);

    <MockMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <MockMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

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
    monitor.with_write_notify_all(|ready| *ready = true);

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notification"),
        17,
    );
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

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    assert!(done_rx.try_recv().is_err());

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(50))
        .expect("manual clock should advance");
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

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    monitor.with_write_notify_one(|ready| *ready = true);

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("condition should become ready"),
        WaitTimeoutResult::Ready(7),
    );
    assert!(!monitor.with_read(|ready| *ready));
    waiter.join().expect("waiter should finish");
}

/// Verifies that only the selected async condition waiter may act on the state
/// transition paired with `notify_one`.
#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_notify_one_releases_only_one_async_condition_waiter() {
    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    let monitor = MockMonitor::new(false);
    let mut first_wait = monitor.wait_until_for_async(WAIT_TIMEOUT, |ready| *ready, |_| 1);
    let mut second_wait = monitor.wait_until_for_async(WAIT_TIMEOUT, |ready| *ready, |_| 2);

    for wait in [&mut first_wait, &mut second_wait] {
        assert!(
            std::future::poll_fn(|context| { Poll::Ready(wait.as_mut().poll(context)) })
                .await
                .is_pending(),
        );
    }
    assert_eq!(2, monitor.pending_timeout_waiters());

    monitor.with_write_notify_one(|ready| *ready = true);

    let first_result =
        std::future::poll_fn(|context| Poll::Ready(first_wait.as_mut().poll(context))).await;
    let second_result =
        std::future::poll_fn(|context| Poll::Ready(second_wait.as_mut().poll(context))).await;
    assert_eq!(first_result, Poll::Ready(WaitTimeoutResult::Ready(1)));
    assert!(second_result.is_pending());
    assert_eq!(1, monitor.pending_timeout_waiters());

    monitor.notify_all();
    assert_eq!(second_wait.await, WaitTimeoutResult::Ready(2));
    assert_eq!(0, monitor.pending_timeout_waiters());
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_traits_delegate_to_monitor_methods() {
    let monitor = Arc::new(MockMonitor::new(vec![1, 2]));

    assert_eq!(
        <MockMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_until_async(
            monitor.as_ref(),
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        2,
    );
    assert_eq!(
        <MockMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_while_async(
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
        <MockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            monitor.as_ref(),
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
        .await,
        WaitTimeoutResult::Ready(3),
    );
    assert_eq!(
        <MockMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
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
        <MockMonitor<bool> as AsyncConditionWaiter>::wait_while_async(
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
    monitor.with_write_notify_all(|ready| *ready = true);

    assert_eq!(waiter.await.expect("waiter task should finish"), 17);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_wait_while_for_waits_for_mock_change() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        <MockMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
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
    monitor.with_write_notify_all(|ready| *ready = true);

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
        <MockMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            &monitor,
            Duration::ZERO,
            |ready| *ready,
            |_| 17,
        )
        .await,
        WaitTimeoutResult::TimedOut,
    );
}
