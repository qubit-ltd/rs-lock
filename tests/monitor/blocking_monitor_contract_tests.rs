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
            use std::{
                cell::Cell,
                sync::{Arc, mpsc},
                thread,
                time::Duration,
            };

            use qubit_clock::{ManualMonotonicClock, MonotonicClock};
            use qubit_lock::{WaitTimeoutResult, WaitTimeoutStatus, $monitor};

            use crate::monitor::failing_timer_tests::{
                DeadlineSignalingTimer,
                assert_backend_unavailable,
                completion_failing_timer,
                registration_failing_timer,
            };

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

                checked_rx
                    .recv()
                    .expect("contract waiter should inspect initial state");
                monitor.with_write_notify_one(|ready| *ready = true);

                done_rx
                    .recv()
                    .expect("contract waiter should receive notification");
                waiter.join().expect("contract waiter should not panic");
            }

            #[test]
            fn test_write_notify_all_wakes_registered_waiters() {
                let monitor = Arc::new($monitor::new(false));
                let (checked_tx, checked_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let waiters = (0..2)
                    .map(|_| {
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
                                    done_tx.send(()).expect("contract waiter should complete");
                                },
                            );
                        })
                    })
                    .collect::<Vec<_>>();

                for _ in 0..2 {
                    checked_rx
                        .recv()
                        .expect("contract waiter should inspect initial state");
                }
                monitor.with_write_notify_all(|ready| *ready = true);

                for _ in 0..2 {
                    done_rx
                        .recv()
                        .expect("contract waiter should receive notification");
                }
                for waiter in waiters {
                    waiter.join().expect("contract waiter should not panic");
                }
            }

            #[test]
            fn test_wait_until_ready_blocks_until_notify_one() {
                let monitor = Arc::new($monitor::new(false));
                let waiter_monitor = Arc::clone(&monitor);
                let waiter = thread::spawn(move || {
                    waiter_monitor.wait_until_ready(|ready| *ready);
                });

                monitor.with_write_notify_one(|ready| *ready = true);

                waiter.join().expect("contract waiter should not panic");
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
                        .wait_until_ready_with_total_timeout(
                            Duration::ZERO,
                            |ready| *ready,
                        )
                        .expect("zero total timeout should be representable"),
                    WaitTimeoutResult::TimedOut,
                );
                assert_eq!(
                    ready
                        .wait_until_ready_with_total_timeout(
                            Duration::ZERO,
                            |ready| *ready,
                        )
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
                        .wait_while_with_total_timeout(
                            Duration::ZERO,
                            |waiting| *waiting,
                            |_| 7,
                        )
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
                let clock = ManualMonotonicClock::new_shared();
                let (sampled_tx, sampled_rx) = mpsc::channel();
                let timer = DeadlineSignalingTimer::new(
                    clock.new_timer(),
                    Arc::clone(&clock),
                    sampled_tx,
                );
                let monitor = Arc::new($monitor::with_timer(
                    false,
                    Arc::new(timer),
                ));
                let guard = monitor.lock();
                let waiter_monitor = Arc::clone(&monitor);
                let waiter = thread::spawn(move || {
                    waiter_monitor.wait_until_ready_with_total_timeout(
                        timeout,
                        |ready| *ready,
                    )
                });

                sampled_rx
                    .recv()
                    .expect("waiter should sample its total deadline");
                clock
                    .advance(timeout)
                    .expect("manual clock should reach the total deadline");
                drop(guard);

                assert_eq!(
                    waiter
                        .join()
                        .expect("total-timeout waiter should not panic")
                        .expect("manual Timer should complete successfully"),
                    WaitTimeoutResult::TimedOut,
                );
            }

            #[test]
            fn test_total_timeout_reports_overflow_before_predicate() {
                let monitor = $monitor::new(false);
                let predicate_called = Cell::new(false);

                let result = monitor.wait_until_ready_with_total_timeout(
                    Duration::MAX,
                    |_| {
                        predicate_called.set(true);
                        false
                    },
                );

                assert!(matches!(
                    result,
                    Err(qubit_clock::TimeError::InstantOverflow)
                ));
                assert!(!predicate_called.get());
            }

            #[test]
            fn test_total_timeout_propagates_timer_registration_error() {
                let monitor = $monitor::with_timer(
                    false,
                    Arc::new(registration_failing_timer()),
                );

                let error = monitor
                    .wait_until_ready_with_total_timeout(
                        Duration::from_secs(1),
                        |ready| *ready,
                    )
                    .expect_err("registration failure should be propagated");

                assert_backend_unavailable(error);
            }

            #[test]
            fn test_total_timeout_propagates_timer_completion_error() {
                let monitor = $monitor::with_timer(
                    false,
                    Arc::new(completion_failing_timer()),
                );

                let error = monitor
                    .wait_until_ready_with_total_timeout(
                        Duration::from_secs(1),
                        |ready| *ready,
                    )
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
