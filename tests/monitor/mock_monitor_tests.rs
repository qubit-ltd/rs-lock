// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
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
use std::task::Poll;

use qubit_clock::ManualMonotonicClock;
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

/// Verifies that advancing a shared clock from a state closure does not
/// re-enter the monitor state lock indefinitely.
#[test]
fn test_mock_monitor_clock_can_advance_inside_state_closure() {
    const REAL_TIMEOUT: Duration = Duration::from_millis(100);

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
        done_tx
            .send(waiter_monitor.wait_for(Duration::from_secs(10)))
            .expect("test should receive wait status");
    });

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    clock
        .advance(Duration::from_secs(10))
        .expect("manual clock should advance");

    assert_eq!(
        WaitTimeoutStatus::TimedOut,
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
            done_tx
                .send(monitor.wait_for(Duration::from_secs(5)))
                .expect("first status should be received");
        })
    };
    let second_waiter = {
        let monitor = Arc::clone(&second);
        thread::spawn(move || {
            done_tx
                .send(monitor.wait_for(Duration::from_secs(5)))
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
            WaitTimeoutStatus::TimedOut,
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
    let mut wait = monitor.wait_for_async(Duration::from_secs(10));
    assert_eq!(0, monitor.pending_timeout_waiters());
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    assert_eq!(1, monitor.pending_timeout_waiters());

    clock
        .advance(Duration::from_secs(10))
        .expect("manual clock should advance");

    assert_eq!(WaitTimeoutStatus::TimedOut, wait.await);
    assert_eq!(0, monitor.pending_timeout_waiters());
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_cancelled_async_timeout_unregisters_waiter() {
    let monitor = MockMonitor::new(false);
    let mut wait = monitor.wait_for_async(Duration::from_secs(10));
    assert_eq!(0, monitor.pending_timeout_waiters());
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    assert_eq!(1, monitor.pending_timeout_waiters());

    drop(wait);

    assert_eq!(0, monitor.pending_timeout_waiters());
}

/// Verifies that an unpolled timeout future cannot steal a notification from
/// a waiter that is already active.
#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_notify_one_prefers_active_async_waiter() {
    let monitor = MockMonitor::new(false);
    let unpolled_wait = monitor.wait_for_async(Duration::from_secs(10));
    let mut active_wait = monitor.wait_for_async(Duration::from_secs(10));
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(active_wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    assert_eq!(1, monitor.pending_timeout_waiters());

    monitor.notify_one();

    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), active_wait)
            .await
            .expect("active waiter should receive the notification"),
        WaitTimeoutStatus::Woken,
    );
    drop(unpolled_wait);
    assert_eq!(0, monitor.pending_timeout_waiters());
}

/// Verifies that cancelling one of multiple active timeout futures removes its
/// waiter ticket without preventing the remaining waiter from being notified.
#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_notify_one_skips_cancelled_async_waiter() {
    let monitor = MockMonitor::new(false);
    let mut cancelled_wait = monitor.wait_for_async(Duration::from_secs(10));
    let mut remaining_wait = monitor.wait_for_async(Duration::from_secs(10));

    for wait in [&mut cancelled_wait, &mut remaining_wait] {
        assert!(
            std::future::poll_fn(|context| {
                Poll::Ready(wait.as_mut().poll(context))
            })
            .await
            .is_pending(),
        );
    }
    assert_eq!(2, monitor.pending_timeout_waiters());

    drop(cancelled_wait);
    assert_eq!(1, monitor.pending_timeout_waiters());
    monitor.notify_one();

    assert_eq!(remaining_wait.await, WaitTimeoutStatus::Woken);
    assert_eq!(0, monitor.pending_timeout_waiters());
}

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

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    assert!(done_rx.try_recv().is_err());

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(99))
        .expect("manual clock should advance");
    assert!(done_rx.try_recv().is_err());

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(1))
        .expect("manual clock should advance");
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

    assert!(monitor.wait_for_timeout_waiters(1, Duration::from_secs(1)));
    monitor.notify_one();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("notification should wake waiter"),
        WaitTimeoutStatus::Woken,
    );
    waiter.join().expect("waiter should finish");
}

/// Verifies that one blocking notification cannot be observed by two timeout
/// waiters after a later manual-clock advance.
#[test]
fn test_mock_monitor_notify_one_wakes_only_one_blocking_timeout_waiter() {
    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    let clock = Arc::new(ManualMonotonicClock::new());
    let monitor = Arc::new(MockMonitor::from_clock((), Arc::clone(&clock)));
    let (done_tx, done_rx) = mpsc::channel();
    let mut waiters = Vec::new();

    for waiter_id in 0..2 {
        let waiter_monitor = Arc::clone(&monitor);
        let done_tx = done_tx.clone();
        waiters.push(thread::spawn(move || {
            let status = waiter_monitor.wait_for(WAIT_TIMEOUT);
            done_tx
                .send((waiter_id, status))
                .expect("test should receive wait status");
        }));
    }
    drop(done_tx);

    assert!(monitor.wait_for_timeout_waiters(2, Duration::from_secs(1)));
    monitor.notify_one();

    let (notified_waiter, notified_status) = done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("one waiter should receive the notification");
    assert_eq!(notified_status, WaitTimeoutStatus::Woken);

    clock
        .advance(WAIT_TIMEOUT)
        .expect("manual clock should advance");
    let (timed_out_waiter, timed_out_status) = done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the other waiter should observe the timeout");
    assert_ne!(notified_waiter, timed_out_waiter);
    assert_eq!(timed_out_status, WaitTimeoutStatus::TimedOut);

    for waiter in waiters {
        waiter.join().expect("waiter should finish");
    }
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
            let result = waiter_monitor.wait_until_for(
                WAIT_TIMEOUT,
                |ready| *ready,
                |_| waiter_id,
            );
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
        <MockMonitor<Vec<i32>> as TimeoutNotificationWaiter>::wait_for(
            &monitor,
            Duration::ZERO,
        ),
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
fn test_mock_monitor_notification_waiter_trait_wait_returns_after_notify() {
    let monitor = Arc::new(MockMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let (done_tx, done_rx) = mpsc::channel();

    let waiter = thread::spawn(move || {
        <MockMonitor<bool> as NotificationWaiter>::wait(
            waiter_monitor.as_ref(),
        );
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
        let result = waiter_monitor.wait_until_for(
            Duration::from_millis(50),
            |ready| *ready,
            |_| 7,
        );
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

#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_async_wait_for_uses_mock_elapsed_time() {
    let monitor = MockMonitor::new(false);
    let mut wait = monitor.wait_for_async(Duration::from_millis(100));
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(99))
        .expect("manual clock should advance");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut wait)
            .await
            .is_err(),
        "mock async timeout should not use real elapsed time",
    );

    monitor
        .monotonic_clock()
        .advance(Duration::from_millis(1))
        .expect("manual clock should advance");
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(50), &mut wait)
            .await
            .expect("mock async wait should complete after mock time advances"),
        WaitTimeoutStatus::TimedOut,
    );
}

/// Verifies that one async notification is consumed by only one timeout
/// waiter while the other waiter remains governed by manual time.
#[cfg(feature = "async")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mock_monitor_notify_one_wakes_only_one_async_timeout_waiter() {
    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    let clock = Arc::new(ManualMonotonicClock::new());
    let monitor = Arc::new(MockMonitor::from_clock((), Arc::clone(&clock)));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut waiters = Vec::new();

    for waiter_id in 0..2 {
        let waiter_monitor = Arc::clone(&monitor);
        let done_tx = done_tx.clone();
        waiters.push(tokio::spawn(async move {
            let status = waiter_monitor.wait_for_async(WAIT_TIMEOUT).await;
            done_tx
                .send((waiter_id, status))
                .expect("test should receive wait status");
        }));
    }
    drop(done_tx);

    assert!(monitor.wait_for_timeout_waiters(2, Duration::from_secs(1)));
    monitor.notify_one();

    let (notified_waiter, notified_status) =
        tokio::time::timeout(Duration::from_secs(1), done_rx.recv())
            .await
            .expect("one waiter should receive the notification")
            .expect("wait status channel should remain open");
    assert_eq!(notified_status, WaitTimeoutStatus::Woken);

    clock
        .advance(WAIT_TIMEOUT)
        .expect("manual clock should advance");
    let (timed_out_waiter, timed_out_status) =
        tokio::time::timeout(Duration::from_secs(1), done_rx.recv())
            .await
            .expect("the other waiter should observe the timeout")
            .expect("wait status channel should remain open");
    assert_ne!(notified_waiter, timed_out_waiter);
    assert_eq!(timed_out_status, WaitTimeoutStatus::TimedOut);

    for waiter in waiters {
        waiter.await.expect("waiter task should finish");
    }
}

/// Verifies that only the selected async condition waiter may act on the state
/// transition paired with `notify_one`.
#[cfg(feature = "async")]
#[tokio::test]
async fn test_mock_monitor_notify_one_releases_only_one_async_condition_waiter()
{
    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    let monitor = MockMonitor::new(false);
    let mut first_wait =
        monitor.wait_until_for_async(WAIT_TIMEOUT, |ready| *ready, |_| 1);
    let mut second_wait =
        monitor.wait_until_for_async(WAIT_TIMEOUT, |ready| *ready, |_| 2);

    for wait in [&mut first_wait, &mut second_wait] {
        assert!(
            std::future::poll_fn(|context| {
                Poll::Ready(wait.as_mut().poll(context))
            })
            .await
            .is_pending(),
        );
    }
    assert_eq!(2, monitor.pending_timeout_waiters());

    monitor.with_write_notify_one(|ready| *ready = true);

    let first_result = std::future::poll_fn(|context| {
        Poll::Ready(first_wait.as_mut().poll(context))
    })
    .await;
    let second_result = std::future::poll_fn(|context| {
        Poll::Ready(second_wait.as_mut().poll(context))
    })
    .await;
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

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = tokio::spawn(async move {
        <MockMonitor<Vec<i32>> as AsyncNotificationWaiter>::wait_async(
            waiter_monitor.as_ref(),
        )
        .await;
    });
    tokio::task::yield_now().await;
    <MockMonitor<Vec<i32>> as Notifier>::notify_all(monitor.as_ref());
    tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .expect("async notification wait should complete")
        .expect("waiter task should finish");

    let wait = <MockMonitor<Vec<i32>> as AsyncTimeoutNotificationWaiter>::wait_for_async(
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
