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
                WaitTimeoutResult,
                WaitTimeoutStatus,
                $monitor,
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
                            done_tx
                                .send(())
                                .expect("contract waiter should complete");
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
                                            checked_tx.send(()).expect(
                                                "contract should observe predicate check",
                                            );
                                        }
                                    }
                                    *ready
                                },
                                |_| {
                                    done_tx.send(()).expect(
                                        "contract waiter should complete",
                                    );
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

/// Generates the common contract tests for Arc-wrapped blocking monitors.
macro_rules! arc_blocking_monitor_contract_tests {
    ($module:ident, $arc_monitor:ident, $monitor:ident) => {
        mod $module {
            use std::sync::Arc;

            use qubit_clock::{
                ManualMonotonicClock,
                MonotonicClock,
            };
            use qubit_lock::{
                $arc_monitor,
                $monitor,
            };

            #[test]
            fn test_with_timer_preserves_timer_domain() {
                let clock = ManualMonotonicClock::new_shared();
                let monitor =
                    $arc_monitor::with_timer(1usize, clock.new_timer());

                assert_eq!(
                    clock.now().domain(),
                    monitor.timer().clock().now().domain()
                );
            }

            #[test]
            fn test_from_arc_preserves_inner_identity() {
                let inner = Arc::new($monitor::new(1usize));
                let monitor = $arc_monitor::from_arc(Arc::clone(&inner));

                assert!(Arc::ptr_eq(&inner, monitor.as_arc()));
                assert!(Arc::ptr_eq(&inner, &monitor.clone().into_arc()));
            }

            #[test]
            fn test_clone_shares_state_and_monitor_operations() {
                let monitor = $arc_monitor::new(vec![1, 2, 3]);
                let cloned = monitor.clone();

                cloned.with_write_notify_one(|items| items.push(4));

                assert_eq!(
                    monitor.with_read(|items| items.clone()),
                    vec![1, 2, 3, 4]
                );
            }

            #[test]
            fn test_default_and_from_preserve_state() {
                let default_monitor = $arc_monitor::<Vec<i32>>::default();
                let supplied_monitor = $arc_monitor::from(vec![1, 2, 3]);

                assert!(default_monitor.with_read(|items| items.is_empty()));
                assert_eq!(supplied_monitor.with_read(|items| items.len()), 3);
            }
        }
    };
}
