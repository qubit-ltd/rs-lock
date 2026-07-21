// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`StdMonitor`](qubit_lock::StdMonitor).

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
};
use qubit_lock::{
    ConditionWaiter,
    Notifier,
    StdMonitor,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

use super::failing_timer_tests::{
    CompletionFailingTimer,
    FailingTimer,
    assert_backend_unavailable,
};

#[test]
fn test_std_monitor_new_read_write_updates_state() {
    let monitor = StdMonitor::new(vec![1, 2, 3]);

    monitor.with_write(|items| {
        items.push(4);
    });

    assert_eq!(monitor.with_read(|items| items.clone()), vec![1, 2, 3, 4]);
}

#[test]
fn test_std_monitor_timed_predicate_wait_propagates_completion_error() {
    let monitor =
        StdMonitor::with_timer(false, Arc::new(CompletionFailingTimer::new()));

    let result =
        monitor.wait_until_for(Duration::from_secs(1), |ready| *ready, |_| ());

    let error = result.expect_err("failing Timer should fail completion");
    assert_backend_unavailable(error);
}

#[test]
fn test_std_monitor_write_notify_one_updates_state_and_wakes_waiter() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        let result = waiter_monitor.wait_until(
            move |ready| {
                if !*ready && let Some(checked_tx) = checked_tx.take() {
                    checked_tx.send(()).expect(
                        "test should observe waiter before notification",
                    );
                }
                *ready
            },
            |ready| {
                *ready = false;
                7
            },
        );
        done_tx
            .send(result)
            .expect("test should receive waiter result");
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check state before notification");
    drop(monitor.lock());

    let write_result = monitor.with_write_notify_one(|ready| {
        *ready = true;
        5
    });

    assert_eq!(write_result, 5);
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after with_write_notify_one"),
        7,
    );
    waiter.join().expect("waiter should not panic");
    assert!(!monitor.with_read(|ready| *ready));
}

#[test]
fn test_std_monitor_notify_one_wakes_exactly_one_waiter() {
    let monitor = Arc::new(StdMonitor::new(0_usize));
    let (first_checked_tx, first_checked_rx) = mpsc::channel();
    let (second_checked_tx, second_checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let first_monitor = Arc::clone(&monitor);
    let first_done_tx = done_tx.clone();
    let first_waiter = thread::spawn(move || {
        let mut checked_tx = Some(first_checked_tx);
        first_monitor.wait_until(
            move |available| {
                if *available == 0
                    && let Some(checked_tx) = checked_tx.take()
                {
                    checked_tx
                        .send(())
                        .expect("test should observe first waiter");
                }
                *available > 0
            },
            |available| *available -= 1,
        );
        first_done_tx
            .send(())
            .expect("test should receive first waiter result");
    });

    let second_monitor = Arc::clone(&monitor);
    let second_waiter = thread::spawn(move || {
        let mut checked_tx = Some(second_checked_tx);
        second_monitor.wait_until(
            move |available| {
                if *available == 0
                    && let Some(checked_tx) = checked_tx.take()
                {
                    checked_tx
                        .send(())
                        .expect("test should observe second waiter");
                }
                *available > 0
            },
            |available| *available -= 1,
        );
        done_tx
            .send(())
            .expect("test should receive second waiter result");
    });

    first_checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first waiter should check state");
    second_checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second waiter should check state");
    drop(monitor.lock());

    monitor.with_write(|available| *available = 1);
    monitor.notify_one();
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("one waiter should finish after notify_one");
    assert!(
        done_rx.try_recv().is_err(),
        "notify_one must not finish both registered waiters"
    );

    monitor.with_write_notify_all(|available| *available = 1);
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("remaining waiter should finish during cleanup");
    first_waiter.join().expect("first waiter should not panic");
    second_waiter
        .join()
        .expect("second waiter should not panic");
}

#[test]
fn test_std_monitor_write_notify_all_wakes_all_waiters() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (first_checked_tx, first_checked_rx) = mpsc::channel();
    let (second_checked_tx, second_checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let first_monitor = Arc::clone(&monitor);
    let first_done_tx = done_tx.clone();
    let first_waiter = thread::spawn(move || {
        let mut checked_tx = Some(first_checked_tx);
        first_monitor.wait_until(
            move |ready| {
                if !*ready && let Some(checked_tx) = checked_tx.take() {
                    checked_tx
                        .send(())
                        .expect("test should observe first waiter");
                }
                *ready
            },
            |_| (),
        );
        first_done_tx
            .send(())
            .expect("test should receive first waiter result");
    });

    let second_monitor = Arc::clone(&monitor);
    let second_waiter = thread::spawn(move || {
        let mut checked_tx = Some(second_checked_tx);
        second_monitor.wait_until(
            move |ready| {
                if !*ready && let Some(checked_tx) = checked_tx.take() {
                    checked_tx
                        .send(())
                        .expect("test should observe second waiter");
                }
                *ready
            },
            |_| (),
        );
        done_tx
            .send(())
            .expect("test should receive second waiter result");
    });

    first_checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first waiter should check state before notification");
    second_checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second waiter should check state before notification");
    drop(monitor.lock());

    let write_result = monitor.with_write_notify_all(|ready| {
        *ready = true;
        2
    });

    assert_eq!(write_result, 2);
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first waiter should finish after with_write_notify_all");
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second waiter should finish after with_write_notify_all");
    first_waiter.join().expect("first waiter should not panic");
    second_waiter
        .join()
        .expect("second waiter should not panic");
}

#[test]
fn test_std_monitor_default_uses_default_value() {
    let monitor = StdMonitor::<Vec<i32>>::default();

    assert!(monitor.with_read(|items| items.is_empty()));
}

#[test]
fn test_std_monitor_from_uses_supplied_value() {
    let monitor = StdMonitor::from(vec![1, 2, 3]);

    assert_eq!(monitor.with_read(|items| items.len()), 3);
}

#[test]
fn test_std_monitor_traits_delegate_to_monitor_methods() {
    let monitor = StdMonitor::new(vec![1, 2]);

    <StdMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <StdMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    assert_eq!(
        <StdMonitor<Vec<i32>> as ConditionWaiter>::wait_until(
            &monitor,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        2,
    );
    assert_eq!(
        <StdMonitor<Vec<i32>> as ConditionWaiter>::wait_while(
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
        <StdMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_until_for(
            &monitor,
            Duration::ZERO,
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        ),
        Ok(WaitTimeoutResult::Ready(3)),
    );
    assert_time_result_eq!(
        <StdMonitor<Vec<i32>> as TimeoutConditionWaiter>::wait_while_for(
            &monitor,
            Duration::ZERO,
            |items| items.is_empty(),
            |items| items.pop(),
        ),
        Ok(WaitTimeoutResult::Ready(Some(1))),
    );
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
    assert_eq!(monitor.with_read(|value| *value), 4);
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
    assert_eq!(monitor.with_read(|items| items.clone()), vec![1, 2, 3, 4]);
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
fn test_std_monitor_guard_wait_for_returns_woken_when_notified() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (waiting_tx, waiting_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut guard = waiter_monitor.lock();
        waiting_tx
            .send(())
            .expect("test should observe waiter before wait");
        let notified = guard
            .wait_for(Duration::from_secs(5))
            .expect("standard Timer should register");
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
fn test_std_monitor_wait_while_for_returns_timed_out_when_timeout() {
    let monitor = StdMonitor::new(false);

    let result = monitor.wait_while_for(
        Duration::from_millis(20),
        |ready| !*ready,
        |_| (),
    );

    assert_time_result_eq!(result, Ok(WaitTimeoutResult::TimedOut));
}

#[test]
fn test_std_monitor_timed_predicate_wait_propagates_timer_error() {
    let monitor = StdMonitor::with_timer(false, Arc::new(FailingTimer::new()));

    let result =
        monitor.wait_until_for(Duration::from_secs(1), |ready| *ready, |_| ());

    let error = result.expect_err("failing Timer should reject registration");
    assert_backend_unavailable(error);
}

#[test]
fn test_std_monitor_uses_injected_manual_timer_without_real_delay() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(StdMonitor::with_timer(false, clock.new_timer()));
    assert_eq!(clock.now().domain(), monitor.timer().clock().now().domain(),);
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until_for(
            Duration::from_secs(8),
            |ready| *ready,
            |_| (),
        )
    });

    let _reached = clock
        .advance_to_next_deadline_after_waiters(1, Duration::from_secs(1))
        .expect("monitor deadline should be registered");

    assert_time_result_eq!(
        Ok(WaitTimeoutResult::TimedOut),
        waiter.join().expect("waiter should finish"),
    );
}

#[test]
fn test_std_monitor_wait_until_for_returns_timed_out_when_timeout() {
    let monitor = StdMonitor::new(false);

    let result = monitor.wait_until_for(
        Duration::from_millis(20),
        |ready| *ready,
        |_| (),
    );

    assert_time_result_eq!(result, Ok(WaitTimeoutResult::TimedOut));
}

/// Verifies that initial lock contention does not consume timeout budget.
#[test]
fn test_std_monitor_wait_while_for_excludes_initial_lock_contention_from_timeout()
 {
    const WAIT_TIMEOUT: Duration = Duration::from_millis(20);

    let monitor = Arc::new(StdMonitor::new(false));
    let guard = monitor.lock();
    let (started_tx, started_rx) = mpsc::channel();
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        started_tx.send(()).expect("test should observe wait start");
        let mut checked_tx = Some(checked_tx);
        let result = waiter_monitor.wait_while_for(
            WAIT_TIMEOUT,
            move |ready| {
                if let Some(checked_tx) = checked_tx.take() {
                    thread::sleep(WAIT_TIMEOUT.saturating_mul(3));
                    checked_tx.send(()).expect(
                        "test should observe the initial predicate check",
                    );
                }
                !*ready
            },
            |_| (),
        );
        done_tx
            .send(result)
            .expect("test should receive wait result");
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should begin while the state lock is held");
    thread::sleep(WAIT_TIMEOUT.saturating_mul(3));
    drop(guard);

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should check the predicate after acquiring the lock");
    drop(monitor.lock());
    monitor.with_write_notify_one(|ready| *ready = true);

    assert_time_result_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should retain a fresh condition-wait budget"),
        Ok(WaitTimeoutResult::Ready(())),
    );
    waiter.join().expect("waiter should finish");
}

/// Verifies that zero timeout evaluates the initial predicate exactly once.
#[test]
fn test_std_monitor_wait_while_for_zero_timeout_checks_predicate_once() {
    let monitor = StdMonitor::new(false);
    let mut checks = 0;

    let result = monitor.wait_while_for(
        Duration::ZERO,
        |ready| {
            checks += 1;
            !*ready
        },
        |_| (),
    );

    assert_time_result_eq!(result, Ok(WaitTimeoutResult::TimedOut));
    assert_eq!(checks, 1);
}

/// Verifies that readiness wins the final locked timeout check.
#[test]
fn test_std_monitor_wait_while_for_timeout_final_predicate_wins() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        let result = waiter_monitor.wait_while_for(
            Duration::from_millis(20),
            move |ready| {
                if let Some(checked_tx) = checked_tx.take() {
                    checked_tx.send(()).expect(
                        "test should observe the initial predicate check",
                    );
                }
                !*ready
            },
            |_| 7,
        );
        done_tx
            .send(result)
            .expect("test should receive wait result");
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should perform the initial predicate check");
    drop(monitor.lock());
    monitor.with_write(|ready| *ready = true);

    assert_time_result_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ready predicate should win the final timeout check"),
        Ok(WaitTimeoutResult::Ready(7)),
    );
    waiter.join().expect("waiter should finish");
}

#[test]
fn test_std_monitor_wait_until_for_returns_result_when_predicate_true() {
    let monitor = Arc::new(StdMonitor::new(false));
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        started_tx
            .send(())
            .expect("test should observe waiter start");
        let result = waiter_monitor.wait_until_for(
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
    monitor.with_write(|ready| {
        *ready = true;
    });
    monitor.notify_one();

    assert_time_result_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should finish after notification"),
        Ok(WaitTimeoutResult::Ready(7)),
    );
    waiter.join().expect("waiter should not panic");
    assert!(!monitor.with_read(|ready| *ready));
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

    monitor.with_write(|ready| {
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

    monitor.with_write(|permits| {
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
    assert_eq!(monitor.with_read(|permits| *permits), 0);
}

#[test]
fn test_std_monitor_remains_usable_after_panic_while_locked() {
    let monitor = Arc::new(StdMonitor::new(0usize));
    let poison_monitor = Arc::clone(&monitor);

    let poisoner = thread::spawn(move || {
        poison_monitor.with_write(|value| {
            *value = 7;
            panic!("intentional panic while holding monitor");
        });
    });

    assert!(poisoner.join().is_err());
    assert_eq!(monitor.with_read(|value| *value), 7);

    monitor.with_write(|value| {
        *value += 1;
    });

    assert_eq!(monitor.with_read(|value| *value), 8);
}

#[test]
fn test_std_monitor_wait_until_continues_after_panic_while_locked() {
    let monitor = Arc::new(StdMonitor::new(false));
    let poison_monitor = Arc::clone(&monitor);

    let poisoner = thread::spawn(move || {
        poison_monitor.with_write(|ready| {
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

    monitor.with_write(|ready| {
        *ready = true;
    });
    monitor.notify_all();

    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter should finish after monitor remains usable");
    waiter.join().expect("waiter should not panic");
    assert!(!monitor.with_read(|ready| *ready));
}
