// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ArcStdMonitor`](qubit_lock::ArcStdMonitor).

use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use qubit_clock::{ManualMonotonicClock, MonotonicClock};
use qubit_lock::{
    ArcStdMonitor, ConditionWaiter, Notifier, StdMonitor, TimeoutConditionWaiter, WaitTimeoutResult,
};

arc_blocking_monitor_contract_tests!(arc_std_monitor_contract, ArcStdMonitor, StdMonitor);

#[test]
fn test_arc_std_monitor_with_timer_preserves_timer_domain() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = ArcStdMonitor::with_timer(1usize, clock.new_timer());

    assert_eq!(clock.now().domain(), monitor.timer().clock().now().domain());
}

#[test]
fn test_arc_std_monitor_preserves_inner_arc_identity() {
    let inner = Arc::new(StdMonitor::new(1usize));
    let monitor = ArcStdMonitor::from_arc(Arc::clone(&inner));

    assert!(Arc::ptr_eq(&inner, monitor.as_arc()));

    let cloned = monitor.clone();
    assert!(Arc::ptr_eq(monitor.as_arc(), cloned.as_arc()));
    cloned.with_write(|value| *value += 1);
    let recovered = cloned.into_arc();

    assert!(Arc::ptr_eq(&inner, &recovered));
    assert_eq!(monitor.with_read(|value| *value), 2);
}

#[test]
fn test_arc_std_monitor_new_read_write_updates_state() {
    let monitor = ArcStdMonitor::new(vec![1, 2, 3]);

    monitor.with_write(|items| {
        items.push(4);
    });

    assert_eq!(monitor.with_read(|items| items.clone()), vec![1, 2, 3, 4]);
}

#[test]
fn test_arc_std_monitor_default_uses_default_value() {
    let monitor = ArcStdMonitor::<Vec<i32>>::default();

    assert!(monitor.with_read(|items| items.is_empty()));
}

#[test]
fn test_arc_std_monitor_from_uses_supplied_value() {
    let monitor = ArcStdMonitor::from(vec![1, 2, 3]);

    assert_eq!(monitor.with_read(|items| items.len()), 3);
}

#[test]
fn test_arc_std_monitor_clone_shares_state() {
    let monitor = ArcStdMonitor::new(1usize);
    let cloned = monitor.clone();

    cloned.with_write(|value| {
        *value += 1;
    });

    assert_eq!(monitor.with_read(|value| *value), 2);
}

#[test]
fn test_arc_std_monitor_write_notify_one_updates_state() {
    let monitor = ArcStdMonitor::new(Vec::<i32>::new());

    let len = monitor.with_write_notify_one(|items| {
        items.push(7);
        items.len()
    });

    assert_eq!(len, 1);
    assert_eq!(monitor.with_read(|items| items.clone()), vec![7]);
}

#[test]
fn test_arc_std_monitor_write_notify_all_updates_state() {
    let monitor = ArcStdMonitor::new(false);

    let ready = monitor.with_write_notify_all(|ready| {
        *ready = true;
        *ready
    });

    assert!(ready);
    assert!(monitor.with_read(|ready| *ready));
}

#[test]
fn test_arc_std_monitor_lock_guard_updates_state() {
    let monitor = ArcStdMonitor::new(Vec::new());

    {
        let mut items = monitor.lock();
        items.push(1);
        items.push(2);
    }

    assert_eq!(monitor.with_read(|items| items.clone()), vec![1, 2]);
}

#[test]
fn test_arc_std_monitor_deref_and_as_ref_expose_monitor_api() {
    let monitor = ArcStdMonitor::new(1);

    {
        let mut value = (*monitor).lock();
        *value += 1;
    }

    monitor.as_ref().with_write(|value| {
        *value += 1;
    });

    assert_eq!(monitor.with_read(|value| *value), 3);
}

#[test]
fn test_arc_std_monitor_traits_delegate_to_monitor_methods() {
    let monitor = ArcStdMonitor::new(vec![1, 2]);

    <ArcStdMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <ArcStdMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    assert_eq!(
        <ArcStdMonitor<Vec<i32>> as ConditionWaiter>::wait_until(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        2,
    );
    assert_eq!(
        <ArcStdMonitor<Vec<i32>> as ConditionWaiter>::wait_while(
            &monitor,
            |items| items.is_empty(),
            |items| {
                items.push(3);
                items.len()
            },
        ),
        2,
    );
    assert_time_result_eq!(
        <ArcStdMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        Ok(WaitTimeoutResult::Ready(3)),
    );
    assert_time_result_eq!(
        <ArcStdMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_while_for(
            &monitor,
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        Ok(WaitTimeoutResult::Ready(Some(1))),
    );
}

#[test]
fn test_arc_std_monitor_wait_until_blocks_until_notify_one() {
    let monitor = ArcStdMonitor::new(false);
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        let result = waiter_monitor.wait_until(
            move |ready| {
                if !*ready && let Some(checked_tx) = checked_tx.take() {
                    checked_tx
                        .send(())
                        .expect("test should observe predicate check");
                }
                *ready
            },
            |ready| {
                *ready = false;
                42
            },
        );
        done_tx
            .send(result)
            .expect("test should receive waiter result");
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check the initial state within timeout");
    drop(monitor.lock());

    monitor.with_write(|ready| {
        *ready = true;
    });
    monitor.notify_one();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notification"),
        42,
    );
    waiter.join().expect("waiter should not panic");
    assert!(!monitor.with_read(|ready| *ready));
}

#[test]
fn test_arc_std_monitor_wait_until_for_delegates_to_monitor() {
    let monitor = ArcStdMonitor::new(false);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        started_tx
            .send(())
            .expect("test should observe waiter start");
        let notified = waiter_monitor.wait_until_for(
            Duration::from_secs(1),
            |ready| *ready,
            |ready| {
                *ready = false;
                10
            },
        );
        done_tx
            .send(notified)
            .expect("test should receive waiter result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should start within timeout");

    monitor.with_write(|ready| *ready = true);
    monitor.notify_all();

    assert_time_result_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after predicate becomes true"),
        Ok(WaitTimeoutResult::Ready(10)),
    );
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_arc_std_monitor_wait_while_delegates_to_monitor() {
    let monitor = ArcStdMonitor::new(Vec::<i32>::new());
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        let result = waiter_monitor.wait_while(
            move |items| {
                if items.is_empty()
                    && let Some(checked_tx) = checked_tx.take()
                {
                    checked_tx
                        .send(())
                        .expect("test should observe predicate check");
                }
                items.is_empty()
            },
            |items| items.pop().expect("item should be ready"),
        );
        done_tx
            .send(result)
            .expect("test should receive waiter result");
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check the initial state within timeout");
    drop(monitor.lock());

    monitor.with_write(|items| items.push(7));
    monitor.notify_one();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notification"),
        7,
    );
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_arc_std_monitor_wait_while_for_returns_ready_when_predicate_clears() {
    let monitor = ArcStdMonitor::new(Vec::<i32>::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        started_tx
            .send(())
            .expect("test should observe waiter start");
        let result = waiter_monitor.wait_while_for(
            Duration::from_secs(1),
            |items| items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
        done_tx
            .send(result)
            .expect("test should receive waiter result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should start within timeout");

    monitor.with_write(|items| items.push(9));
    monitor.notify_all();

    assert_time_result_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after predicate becomes ready"),
        Ok(WaitTimeoutResult::Ready(9)),
    );
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_arc_std_monitor_wait_while_for_returns_timed_out() {
    let monitor = ArcStdMonitor::new(Vec::<i32>::new());

    assert_time_result_eq!(
        monitor.wait_while_for(
            Duration::from_millis(30),
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        Ok(WaitTimeoutResult::TimedOut),
    );
}

#[test]
fn test_arc_std_monitor_notify_all_wakes_multiple_waiters() {
    let monitor = ArcStdMonitor::new(false);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let mut waiters = Vec::new();

    for id in 0..2 {
        let waiter_monitor = monitor.clone();
        let waiter_started_tx = started_tx.clone();
        let waiter_done_tx = done_tx.clone();
        waiters.push(thread::spawn(move || {
            waiter_started_tx
                .send(())
                .expect("test should observe waiter start");
            waiter_monitor.wait_until(
                |ready| *ready,
                |ready| {
                    assert!(*ready);
                    id
                },
            );
            waiter_done_tx
                .send(id)
                .expect("test should receive waiter result");
        }));
    }
    drop(started_tx);
    drop(done_tx);

    for _ in 0..2 {
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should start within timeout");
    }
    assert!(matches!(
        started_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty),
    ));

    monitor.with_write(|ready| {
        *ready = true;
    });
    monitor.notify_all();

    let mut completed = vec![
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first waiter should finish after notification"),
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second waiter should finish after notification"),
    ];
    completed.sort_unstable();
    assert_eq!(completed, vec![0, 1]);

    for waiter in waiters {
        waiter.join().expect("waiter should not panic");
    }
}
