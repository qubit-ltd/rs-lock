/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`StdMonitor`](qubit_lock::lock::StdMonitor).

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use qubit_lock::lock::{
    StdMonitor,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

#[test]
fn test_std_monitor_new_read_write_updates_state() {
    let monitor = StdMonitor::new(vec![1, 2, 3]);

    monitor.write(|items| {
        items.push(4);
    });

    assert_eq!(monitor.read(|items| items.clone()), vec![1, 2, 3, 4]);
}

#[test]
fn test_std_monitor_default_uses_default_value() {
    let monitor = StdMonitor::<Vec<i32>>::default();

    assert!(monitor.read(|items| items.is_empty()));
}

#[test]
fn test_std_monitor_wait_until_returns_when_predicate_is_ready() {
    let monitor = StdMonitor::new(3);

    let result = monitor.wait_until(
        |value| *value >= 3,
        |value| {
            *value += 1;
            *value
        },
    );

    assert_eq!(result, 4);
    assert_eq!(monitor.read(|value| *value), 4);
}

#[test]
fn test_std_monitor_wait_while_returns_when_predicate_is_false() {
    let monitor = StdMonitor::new(vec![1, 2, 3]);

    let result = monitor.wait_while(
        |items| items.is_empty(),
        |items| {
            items.push(4);
            items.len()
        },
    );

    assert_eq!(result, 4);
    assert_eq!(monitor.read(|items| items.clone()), vec![1, 2, 3, 4]);
}

#[test]
fn test_std_monitor_wait_until_blocks_until_notify_one() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
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
fn test_std_monitor_wait_notify_returns_timed_out() {
    let monitor = StdMonitor::new(false);

    let status = monitor.wait_notify(Duration::from_millis(30));

    assert_eq!(status, WaitTimeoutStatus::TimedOut);
}

#[test]
fn test_std_monitor_guard_wait_timeout_returns_woken_when_notified() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (waiting_tx, waiting_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let guard = waiter_monitor.lock();
        waiting_tx
            .send(())
            .expect("test should observe waiter before wait");
        let (_guard, notified) = guard.wait_timeout(Duration::from_secs(5));
        done_tx
            .send(notified)
            .expect("test should receive waiter result");
    });

    waiting_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should reach wait setup within timeout");

    // Reacquiring the monitor lock proves the waiter entered the condvar wait
    // and released the mutex, so the notification cannot be sent too early.
    drop(monitor.lock());
    monitor.notify_one();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notify"),
        WaitTimeoutStatus::Woken,
    );
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_std_monitor_wait_timeout_while_returns_timed_out_when_timeout() {
    let monitor = StdMonitor::new(false);

    let result = monitor.wait_timeout_while(Duration::from_millis(20), |ready| !*ready, |_| ());

    assert_eq!(result, WaitTimeoutResult::TimedOut);
}

#[test]
fn test_std_monitor_wait_timeout_until_returns_timed_out_when_timeout() {
    let monitor = StdMonitor::new(false);

    let result = monitor.wait_timeout_until(Duration::from_millis(20), |ready| *ready, |_| ());

    assert_eq!(result, WaitTimeoutResult::TimedOut);
}

#[test]
fn test_std_monitor_wait_timeout_until_returns_result_when_predicate_true() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        started_tx
            .send(())
            .expect("test should observe waiter start");
        let result = waiter_monitor.wait_timeout_until(
            Duration::from_secs(1),
            |ready| *ready,
            |ready| {
                *ready = false;
                7
            },
        );
        done_tx
            .send(result)
            .expect("test should receive waiter result");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should start within timeout");
    monitor.write(|ready| {
        *ready = true;
    });
    monitor.notify_one();

    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notification"),
        WaitTimeoutResult::Ready(7),
    );
    waiter.join().expect("waiter should not panic");
    assert!(!monitor.read(|ready| *ready));
}

#[test]
fn test_std_monitor_wait_until_ignores_notification_until_predicate_true() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until(
            move |ready| {
                if !*ready {
                    checked_tx
                        .send(())
                        .expect("test should observe predicate check");
                }
                *ready
            },
            |ready| {
                assert!(*ready);
            },
        );
        done_tx.send(()).expect("test should receive waiter result");
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check the initial state within timeout");
    drop(monitor.lock());
    monitor.notify_all();
    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should recheck after notification");
    drop(monitor.lock());

    monitor.write(|ready| {
        *ready = true;
    });
    monitor.notify_all();

    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should finish when predicate becomes true");
    waiter.join().expect("waiter should not panic");
}

#[test]
fn test_std_monitor_notify_all_wakes_all_ready_waiters() {
    const WAITER_COUNT: usize = 3;

    let monitor = Arc::new(StdMonitor::new(0usize));
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let mut waiters = Vec::with_capacity(WAITER_COUNT);

    for _ in 0..WAITER_COUNT {
        let waiter_monitor = Arc::clone(&monitor);
        let started_tx = started_tx.clone();
        let done_tx = done_tx.clone();
        waiters.push(thread::spawn(move || {
            started_tx
                .send(())
                .expect("test should observe waiter start");
            waiter_monitor.wait_until(
                |permits| *permits > 0,
                |permits| {
                    *permits -= 1;
                },
            );
            done_tx.send(()).expect("test should receive waiter result");
        }));
    }

    for _ in 0..WAITER_COUNT {
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should start within timeout");
    }

    monitor.write(|permits| {
        *permits = WAITER_COUNT;
    });
    monitor.notify_all();

    for _ in 0..WAITER_COUNT {
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notify_all");
    }
    for waiter in waiters {
        waiter.join().expect("waiter should not panic");
    }
    assert_eq!(monitor.read(|permits| *permits), 0);
}

#[test]
fn test_std_monitor_remains_usable_after_panic_while_locked() {
    let monitor = Arc::new(StdMonitor::new(0usize));
    let poison_monitor = Arc::clone(&monitor);

    let poisoner = thread::spawn(move || {
        poison_monitor.write(|value| {
            *value = 7;
            panic!("intentional panic while holding monitor");
        });
    });

    assert!(poisoner.join().is_err());
    assert_eq!(monitor.read(|value| *value), 7);

    monitor.write(|value| {
        *value += 1;
    });

    assert_eq!(monitor.read(|value| *value), 8);
}

#[test]
fn test_std_monitor_wait_until_continues_after_panic_while_locked() {
    let monitor = Arc::new(StdMonitor::new(false));
    let poison_monitor = Arc::clone(&monitor);

    let poisoner = thread::spawn(move || {
        poison_monitor.write(|ready| {
            *ready = false;
            panic!("intentional panic while holding monitor");
        });
    });
    assert!(poisoner.join().is_err());

    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        waiter_monitor.wait_until(
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
            },
        );
        done_tx.send(()).expect("test should receive waiter result");
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check the initial state within timeout");
    drop(monitor.lock());

    monitor.write(|ready| {
        *ready = true;
    });
    monitor.notify_all();

    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should finish after monitor remains usable");
    waiter.join().expect("waiter should not panic");
    assert!(!monitor.read(|ready| *ready));
}
