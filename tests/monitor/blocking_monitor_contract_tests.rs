// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared conformance tests for the synchronous monitor implementations.

/// Generates the common contract tests for owned blocking monitors.
macro_rules! blocking_monitor_contract_tests {
    ($module:ident, $monitor:ident) => {
        mod $module {
            use std::cell::Cell;
            use std::sync::Arc;
            use std::sync::mpsc;
            use std::sync::mpsc::Receiver;
            use std::thread;
            use std::thread::JoinHandle;
            use std::time::Duration;

            use qubit_clock::ManualMonotonicClock;
            use qubit_clock::MonotonicClock;
            use qubit_clock::TimeError;
            use qubit_lock::WaitTimeoutResult;
            use qubit_lock::WaitTimeoutStatus;
            use qubit_lock::$monitor;

            use crate::monitor::failing_timer_tests::DeadlineSignalingTimer;
            use crate::monitor::failing_timer_tests::GatedCompletionTimer;
            use crate::monitor::failing_timer_tests::assert_backend_unavailable;
            use crate::monitor::failing_timer_tests::completion_failing_timer;
            use crate::monitor::failing_timer_tests::registration_failing_timer;

            const REAL_LIMIT: Duration = Duration::from_secs(5);

            fn assert_completion_error_after_unlock<W>(wait: W)
            where
                W: FnOnce(Arc<$monitor<usize>>) -> Result<(), TimeError> + Send + 'static,
            {
                let (timer, gate, first_poll_rx) = GatedCompletionTimer::new(completion_failing_timer());
                let monitor = Arc::new($monitor::with_timer(0usize, Arc::new(timer)));
                let (wait_result_tx, wait_result_rx) = mpsc::channel();
                let wait_monitor = Arc::clone(&monitor);
                let mut first_waiter = Some(thread::spawn(move || {
                    let result = wait(wait_monitor);
                    let _ = wait_result_tx.send(result);
                }));

                let first_polled = first_poll_rx.recv_timeout(REAL_LIMIT).is_ok();
                let (probe_tx, probe_rx) = mpsc::channel();
                let probe_monitor = Arc::clone(&monitor);
                let mut probe = Some(thread::spawn(move || {
                    probe_monitor.with_write(|state| *state = 1);
                    let _ = probe_tx.send(());
                }));
                let lock_handoff = probe_rx.recv_timeout(REAL_LIMIT).is_ok();
                let ended_before_gate = wait_result_rx.try_recv().ok();
                let ended_before_gate_was_set = ended_before_gate.is_some();
                gate.open();
                let first_result = ended_before_gate.or_else(|| wait_result_rx.recv_timeout(REAL_LIMIT).ok());

                let first_waiter_done = first_result.is_some();
                let first_error = match first_result {
                    Some(Err(error)) => Some(error),
                    _ => None,
                };
                if first_waiter_done {
                    first_waiter
                        .take()
                        .expect("error waiter handle should exist")
                        .join()
                        .expect("completed error waiter should not panic");
                }
                if lock_handoff {
                    probe
                        .take()
                        .expect("lock probe handle should exist")
                        .join()
                        .expect("completed lock probe should not panic");
                }

                let mut second_waiter = None;
                let mut notifier = None;
                let mut notifier_done_rx = None;
                let mut second_done_rx = None;
                let mut single_notification_completed = false;
                if first_error.is_some() && lock_handoff && !ended_before_gate_was_set {
                    let (checked_tx, checked_rx) = mpsc::channel();
                    let (done_tx, done_rx) = mpsc::channel();
                    let second_monitor = Arc::clone(&monitor);
                    second_waiter = Some(thread::spawn(move || {
                        let mut checked_tx = Some(checked_tx);
                        second_monitor.wait_until_ready(move |state| {
                            if *state != 3 {
                                if let Some(checked_tx) = checked_tx.take() {
                                    let _ = checked_tx.send(());
                                }
                            }
                            *state == 3
                        });
                        let _ = done_tx.send(());
                    }));
                    second_done_rx = Some(done_rx);
                    let checked = checked_rx.recv_timeout(REAL_LIMIT).is_ok();
                    if checked {
                        let notify_monitor = Arc::clone(&monitor);
                        let (notifier_done_tx, done_rx) = mpsc::channel();
                        notifier = Some(thread::spawn(move || {
                            notify_monitor.with_write_notify_one(|state| *state = 3);
                            let _ = notifier_done_tx.send(());
                        }));
                        notifier_done_rx = Some(done_rx);
                        single_notification_completed = second_done_rx
                            .as_ref()
                            .expect("second waiter completion channel should exist")
                            .recv_timeout(REAL_LIMIT)
                            .is_ok();
                    }
                }

                let second_was_started = second_waiter.is_some();
                if !single_notification_completed {
                    gate.open();
                    let cleanup_monitor = Arc::clone(&monitor);
                    let (cleanup_tx, cleanup_rx) = mpsc::channel();
                    let cleanup = thread::spawn(move || {
                        cleanup_monitor.with_write_notify_all(|state| *state = 3);
                        let _ = cleanup_tx.send(());
                    });
                    if cleanup_rx.recv_timeout(REAL_LIMIT).is_ok() {
                        cleanup.join().expect("completed cleanup should not panic");
                    }
                    if first_waiter.is_some() && wait_result_rx.recv_timeout(REAL_LIMIT).is_ok() {
                        first_waiter
                            .take()
                            .expect("error waiter handle should exist")
                            .join()
                            .expect("cleaned error waiter should not panic");
                    }
                    if probe.is_some() && probe_rx.recv_timeout(REAL_LIMIT).is_ok() {
                        probe
                            .take()
                            .expect("lock probe handle should exist")
                            .join()
                            .expect("cleaned lock probe should not panic");
                    }
                    if let (Some(rx), Some(waiter)) = (second_done_rx.as_ref(), second_waiter.take()) {
                        if rx.recv_timeout(REAL_LIMIT).is_ok() {
                            waiter.join().expect("cleaned second waiter should not panic");
                        }
                    }
                } else {
                    if let Some(waiter) = second_waiter.take() {
                        waiter.join().expect("single-notified waiter should not panic");
                    }
                }
                if let (Some(rx), Some(notifier)) = (notifier_done_rx.as_ref(), notifier) {
                    if rx.recv_timeout(REAL_LIMIT).is_ok() {
                        notifier
                            .join()
                            .expect("completed notification worker should not panic");
                    }
                }

                assert!(
                    first_polled,
                    "timed waiter should poll the closed completion gate"
                );
                assert!(
                    lock_handoff,
                    "probe should acquire the state lock while the waiter is pending"
                );
                assert!(
                    !ended_before_gate_was_set,
                    "waiter should remain pending until completion gate opens"
                );
                let first_error = first_error.expect("completion error should be propagated");
                assert_backend_unavailable(first_error);
                assert!(
                    second_was_started && single_notification_completed,
                    "next waiter should complete after exactly one notification, without cleanup broadcast"
                );
            }

            #[test]
            fn test_relative_wait_propagates_error_after_unlock() {
                assert_completion_error_after_unlock(|monitor| {
                    monitor
                        .wait_until_for(
                            Duration::from_secs(1),
                            |_| false,
                            |_| panic!("action must not run after Timer failure"),
                        )
                        .map(|_| ())
                });
            }

            #[test]
            fn test_deadline_wait_propagates_error_after_unlock() {
                assert_completion_error_after_unlock(|monitor| {
                    let deadline = monitor
                        .timer()
                        .clock()
                        .now()
                        .checked_add(Duration::from_secs(1))
                        .expect("test deadline should be representable");
                    monitor
                        .wait_while_with_deadline(
                            deadline,
                            |_| true,
                            |_| panic!("action must not run after Timer failure"),
                        )
                        .map(|_| ())
                });
            }

            #[test]
            fn test_guard_reacquires_after_completion_error() {
                assert_completion_error_after_unlock(|monitor| {
                    let mut guard = monitor.lock();
                    let error = guard
                        .wait_for(Duration::from_secs(1))
                        .expect_err("controlled Timer should fail after state lock release");
                    assert_eq!(*guard, 1, "guard should observe lock-handoff probe write");
                    *guard = 2;
                    assert_eq!(*guard, 2, "guard should remain writable after completion error");
                    Err(error)
                });
            }

            fn release_and_join_completed_waiters(
                monitor: &$monitor<bool>,
                done_rx: &Receiver<usize>,
                waiters: Vec<JoinHandle<()>>,
                completed: &mut [bool; 2],
            ) {
                monitor.with_write_notify_all(|ready| *ready = true);
                let remaining = completed.iter().filter(|done| !**done).count();
                for _ in 0..remaining {
                    let Ok(waiter_id) = done_rx.recv_timeout(Duration::from_secs(5)) else {
                        break;
                    };
                    completed[waiter_id] = true;
                }
                for (waiter_id, waiter) in waiters.into_iter().enumerate() {
                    if completed[waiter_id] {
                        waiter.join().expect("completed contract waiter should not panic");
                    }
                }
            }

            #[test]
            fn test_new_read_write_updates_state() {
                let monitor = $monitor::new(vec![1, 2, 3]);

                monitor.with_write(|items| items.push(4));

                assert_eq!(monitor.with_read(|items| items.clone()), vec![1, 2, 3, 4]);
            }

            #[test]
            fn test_default_and_from_preserve_state() {
                let default_monitor = $monitor::<Vec<i32>>::default();
                let supplied_monitor = $monitor::from(vec![1, 2, 3]);

                assert!(default_monitor.with_read(|items| items.is_empty()));
                assert_eq!(supplied_monitor.with_read(|items| items.len()), 3);
            }

            #[test]
            fn test_write_notify_one_wakes_registered_waiter() {
                let monitor = Arc::new($monitor::new(false));
                let (checked_tx, checked_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let waiter_monitor = Arc::clone(&monitor);
                let waiter = thread::spawn(move || {
                    let mut checked_tx = Some(checked_tx);
                    waiter_monitor.wait_until(
                        move |ready| {
                            if !*ready {
                                if let Some(checked_tx) = checked_tx.take() {
                                    checked_tx
                                        .send(())
                                        .expect("contract should observe predicate check");
                                }
                            }
                            *ready
                        },
                        |_| {
                            done_tx.send(()).expect("contract waiter should complete");
                        },
                    );
                });

                if let Err(error) = checked_rx.recv_timeout(Duration::from_secs(5)) {
                    monitor.with_write_notify_all(|ready| *ready = true);
                    if done_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
                        waiter.join().expect("released contract waiter should not panic");
                    }
                    panic!("contract waiter should inspect initial state before notification: {error}");
                }
                monitor.with_write_notify_one(|ready| *ready = true);

                if let Err(error) = done_rx.recv_timeout(Duration::from_secs(5)) {
                    monitor.with_write_notify_all(|ready| *ready = true);
                    if done_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
                        waiter.join().expect("released contract waiter should not panic");
                    }
                    panic!("contract waiter should complete after notify_one: {error}");
                }
                waiter.join().expect("contract waiter should not panic");
            }

            #[test]
            fn test_write_notify_all_wakes_registered_waiters() {
                let monitor = Arc::new($monitor::new(false));
                let (checked_tx, checked_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel::<usize>();
                let waiters = (0..2)
                    .map(|waiter_id| {
                        let waiter_monitor = Arc::clone(&monitor);
                        let checked_tx = checked_tx.clone();
                        let done_tx = done_tx.clone();
                        thread::spawn(move || {
                            let mut checked_tx = Some(checked_tx);
                            waiter_monitor.wait_until(
                                move |ready| {
                                    if !*ready {
                                        if let Some(checked_tx) = checked_tx.take() {
                                            checked_tx
                                                .send(())
                                                .expect("contract should observe predicate check");
                                        }
                                    }
                                    *ready
                                },
                                |_| {
                                    done_tx.send(waiter_id).expect("contract waiter should complete");
                                },
                            );
                        })
                    })
                    .collect::<Vec<_>>();

                let mut completed = [false; 2];
                for _ in 0..2 {
                    if let Err(error) = checked_rx.recv_timeout(Duration::from_secs(5)) {
                        release_and_join_completed_waiters(&monitor, &done_rx, waiters, &mut completed);
                        panic!("contract waiters should inspect initial state before notify_all: {error}");
                    }
                }
                monitor.with_write_notify_all(|ready| *ready = true);

                for _ in 0..2 {
                    match done_rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(waiter_id) => completed[waiter_id] = true,
                        Err(error) => {
                            release_and_join_completed_waiters(&monitor, &done_rx, waiters, &mut completed);
                            panic!("contract waiters should complete after notify_all: {error}");
                        }
                    }
                }
                for (waiter_id, waiter) in waiters.into_iter().enumerate() {
                    assert!(completed[waiter_id]);
                    waiter.join().expect("contract waiter should not panic");
                }
            }

            #[test]
            fn test_wait_until_ready_blocks_until_notify_one() {
                let monitor = Arc::new($monitor::new(false));
                let (checked_tx, checked_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let waiter_monitor = Arc::clone(&monitor);
                let waiter = thread::spawn(move || {
                    let mut checked_tx = Some(checked_tx);
                    waiter_monitor.wait_until_ready(move |ready| {
                        if !*ready {
                            if let Some(sender) = checked_tx.take() {
                                let _ = sender.send(());
                            }
                        }
                        *ready
                    });
                    let _ = done_tx.send(());
                });
                checked_rx
                    .recv_timeout(REAL_LIMIT)
                    .expect("waiter should inspect the blocking predicate");

                let (notified_tx, notified_rx) = mpsc::channel();
                let notifier = thread::spawn(move || {
                    monitor.with_write_notify_one(|ready| *ready = true);
                    let _ = notified_tx.send(());
                });
                let notified = notified_rx.recv_timeout(REAL_LIMIT);
                let completed = done_rx.recv_timeout(REAL_LIMIT);
                if notified.is_ok() {
                    notifier.join().expect("notifier should not panic");
                }
                if completed.is_ok() {
                    waiter.join().expect("waiter should not panic");
                }
                assert_eq!(notified, Ok(()), "notification should return");
                assert_eq!(completed, Ok(()), "registered ready wait should finish");
            }

            #[test]
            fn test_wait_until_ready_returns_when_predicate_is_ready() {
                let monitor = $monitor::new(true);

                monitor.wait_until_ready(|ready| *ready);
            }

            #[test]
            fn test_wait_until_ready_for_preserves_ready_and_timeout_outcomes() {
                let timed_out = $monitor::new(false);
                let ready = $monitor::new(true);

                assert_eq!(
                    timed_out
                        .wait_until_ready_for(Duration::ZERO, |ready| *ready)
                        .expect("zero timeout should not register a Timer"),
                    WaitTimeoutResult::TimedOut,
                );
                assert_eq!(
                    ready
                        .wait_until_ready_for(Duration::ZERO, |ready| *ready)
                        .expect("ready predicate should not register a Timer"),
                    WaitTimeoutResult::Ready(()),
                );
            }

            #[test]
            fn test_total_timeout_preserves_ready_and_timeout_outcomes() {
                let timed_out = $monitor::new(false);
                let ready = $monitor::new(true);

                assert_eq!(
                    timed_out
                        .wait_until_ready_with_total_timeout(Duration::ZERO, |ready| *ready,)
                        .expect("zero total timeout should be representable"),
                    WaitTimeoutResult::TimedOut,
                );
                assert_eq!(
                    ready
                        .wait_until_ready_with_total_timeout(Duration::ZERO, |ready| *ready,)
                        .expect("ready predicate should win at the deadline"),
                    WaitTimeoutResult::Ready(()),
                );
            }

            #[test]
            fn test_total_timeout_runs_action_only_when_ready() {
                let ready = $monitor::new(false);
                let timed_out = $monitor::new(true);
                let timed_out_action_called = Cell::new(false);

                assert_eq!(
                    ready
                        .wait_while_with_total_timeout(Duration::ZERO, |waiting| *waiting, |_| 7,)
                        .expect("ready action should not register a Timer"),
                    WaitTimeoutResult::Ready(7),
                );
                assert_eq!(
                    timed_out
                        .wait_while_with_total_timeout(
                            Duration::ZERO,
                            |waiting| *waiting,
                            |_| {
                                timed_out_action_called.set(true);
                                7
                            },
                        )
                        .expect("zero total timeout should be representable"),
                    WaitTimeoutResult::TimedOut,
                );
                assert!(!timed_out_action_called.get());
            }

            #[test]
            fn test_total_timeout_includes_initial_lock_contention() {
                let timeout = Duration::from_secs(1);
                let real_limit = Duration::from_secs(5);
                let clock = ManualMonotonicClock::new_shared();
                let (sampled_tx, sampled_rx) = mpsc::channel();
                let timer = DeadlineSignalingTimer::new(clock.new_timer(), Arc::clone(&clock), sampled_tx);
                let monitor = Arc::new($monitor::with_timer(false, Arc::new(timer)));
                let guard = monitor.lock();
                let waiter_monitor = Arc::clone(&monitor);
                let (done_tx, done_rx) = mpsc::channel();
                let waiter = thread::spawn(move || {
                    let result = waiter_monitor.wait_until_ready_with_total_timeout(timeout, |ready| *ready);
                    done_tx
                        .send(result)
                        .expect("controller should receive wait result");
                });
                let sampled = sampled_rx.recv_timeout(real_limit);
                let advance_result = if sampled.is_ok() {
                    Some(clock.advance(timeout))
                } else {
                    None
                };
                drop(guard);
                if sampled.is_err() || advance_result.as_ref().is_some_and(Result::is_err) {
                    monitor.with_write_notify_all(|ready| *ready = true);
                }
                let completed = done_rx.recv_timeout(real_limit);
                let completed_on_time = completed.is_ok();
                let result = match completed {
                    Ok(result) => Ok(result),
                    Err(_) => {
                        monitor.with_write_notify_all(|ready| *ready = true);
                        done_rx.recv_timeout(real_limit)
                    }
                };
                if result.is_ok() {
                    waiter
                        .join()
                        .expect("completed total-timeout waiter should not panic");
                }
                let result = result.expect("total-timeout waiter should finish during cleanup");
                assert!(
                    sampled.is_ok(),
                    "total deadline must be sampled before acquiring state lock"
                );
                advance_result
                    .expect("sampling should allow clock advance")
                    .expect("manual clock should reach the deadline");
                assert!(completed_on_time, "total-timeout waiter required cleanup");
                assert_eq!(
                    result.expect("manual Timer should complete"),
                    WaitTimeoutResult::TimedOut
                );
            }

            #[test]
            fn test_total_timeout_reports_overflow_before_predicate() {
                let monitor = $monitor::new(false);
                let predicate_called = Cell::new(false);

                let result = monitor.wait_until_ready_with_total_timeout(Duration::MAX, |_| {
                    predicate_called.set(true);
                    false
                });

                assert!(matches!(result, Err(TimeError::InstantOverflow)));
                assert!(!predicate_called.get());
            }

            #[test]
            fn test_total_timeout_propagates_timer_registration_error() {
                let monitor = $monitor::with_timer(false, Arc::new(registration_failing_timer()));

                let error = monitor
                    .wait_until_ready_with_total_timeout(Duration::from_secs(1), |ready| *ready)
                    .expect_err("registration failure should be propagated");

                assert_backend_unavailable(error);
            }

            #[test]
            fn test_total_timeout_propagates_timer_completion_error() {
                let monitor = $monitor::with_timer(false, Arc::new(completion_failing_timer()));

                let error = monitor
                    .wait_until_ready_with_total_timeout(Duration::from_secs(1), |ready| *ready)
                    .expect_err("completion failure should be propagated");

                assert_backend_unavailable(error);
            }

            #[test]
            fn test_timed_wait_uses_injected_timer_domain() {
                let clock = ManualMonotonicClock::new_shared();
                let monitor = $monitor::with_timer(false, clock.new_timer());
                assert_eq!(clock.now().domain(), monitor.timer().clock().now().domain());

                assert!(
                    matches!(
                        monitor.wait_until_for(Duration::ZERO, |ready| *ready, |_| ()),
                        Ok(WaitTimeoutResult::TimedOut),
                    ),
                    "contract wait should time out without waiting on real time",
                );
            }

            #[test]
            fn test_guard_wait_for_reports_timeout() {
                let monitor = $monitor::new(false);
                let mut guard = monitor.lock();

                let status = guard
                    .wait_for(Duration::from_millis(1))
                    .expect("standard Timer should register");

                assert_eq!(status, WaitTimeoutStatus::TimedOut);
                assert!(!*guard);
            }
        }
    };
}
