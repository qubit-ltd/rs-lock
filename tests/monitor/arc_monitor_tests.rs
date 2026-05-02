/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ArcMonitor`](qubit_lock::lock::ArcMonitor).

use std::{
    sync::mpsc,
    thread,
    time::Duration,
};

use qubit_lock::lock::{
    ArcMonitor,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

#[test]
fn test_arc_monitor_new_read_write_updates_state() {
    let monitor = ArcMonitor::new(vec![1, 2, 3]);

    monitor.write(|items| {
        items.push(4);
    });

    assert_eq!(monitor.read(|items| items.clone()), vec![1, 2, 3, 4]);
}

#[test]
fn test_arc_monitor_default_uses_default_value() {
    let monitor = ArcMonitor::<Vec<i32>>::default();

    assert!(monitor.read(|items| items.is_empty()));
}

#[test]
fn test_arc_monitor_clone_shares_state() {
    let monitor = ArcMonitor::new(1usize);
    let cloned = monitor.clone();

    cloned.write(|value| {
        *value += 1;
    });

    assert_eq!(monitor.read(|value| *value), 2);
}

#[test]
fn test_arc_monitor_lock_guard_updates_state() {
    let monitor = ArcMonitor::new(Vec::new());

    {
        let mut items = monitor.lock();
        items.push(1);
        items.push(2);
    }

    assert_eq!(monitor.read(|items| items.clone()), vec![1, 2]);
}

#[test]
fn test_arc_monitor_wait_until_blocks_until_notify_one() {
    let monitor = ArcMonitor::new(false);
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

    monitor.write(|ready| {
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
    assert!(!monitor.read(|ready| *ready));
}

#[test]
fn test_arc_monitor_wait_notify_returns_timed_out() {
    let monitor = ArcMonitor::new(false);

    assert_eq!(
        monitor.wait_notify(Duration::from_millis(30)),
        WaitTimeoutStatus::TimedOut,
    );
}

#[test]
fn test_arc_monitor_wait_timeout_until_delegates_to_monitor() {
    let monitor = ArcMonitor::new(false);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        started_tx
            .send(())
            .expect("test should observe waiter start");
        let notified = waiter_monitor.wait_timeout_until(
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

    monitor.write(|ready| *ready = true);
    monitor.notify_all();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after predicate becomes true"),
        WaitTimeoutResult::Ready(10),
    );
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_arc_monitor_wait_while_delegates_to_monitor() {
    let monitor = ArcMonitor::new(Vec::<i32>::new());
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

    monitor.write(|items| items.push(7));
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
fn test_arc_monitor_wait_timeout_while_returns_ready_when_predicate_clears() {
    let monitor = ArcMonitor::new(Vec::<i32>::new());
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = monitor.clone();
    let waiter = thread::spawn(move || {
        started_tx
            .send(())
            .expect("test should observe waiter start");
        let result = waiter_monitor.wait_timeout_while(
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

    monitor.write(|items| items.push(9));
    monitor.notify_all();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after predicate becomes ready"),
        WaitTimeoutResult::Ready(9),
    );
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_arc_monitor_wait_timeout_while_returns_timed_out() {
    let monitor = ArcMonitor::new(Vec::<i32>::new());

    assert_eq!(
        monitor.wait_timeout_while(
            Duration::from_millis(30),
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        WaitTimeoutResult::TimedOut,
    );
}

#[test]
fn test_arc_monitor_notify_all_wakes_multiple_waiters() {
    let monitor = ArcMonitor::new(false);
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

    monitor.write(|ready| {
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
