// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Loom models for the synchronous monitor notification handshake.

use loom::{
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    thread,
};
use qubit_lock::test_util::loom::LoomStdMonitor;

/// Verifies the public poison API remains available when Loom substitutes its
/// non-poisoning mutex.
#[test]
fn test_loom_std_monitor_exposes_non_poisoning_status() {
    loom::model(|| {
        let monitor = LoomStdMonitor::new(());

        assert!(!monitor.is_poisoned());
        monitor.clear_poison();
        assert!(!monitor.is_poisoned());
    });
}

/// Models one registered waiter receiving a state-changing notification.
#[test]
fn test_loom_std_monitor_notify_one_releases_registered_waiter() {
    loom::model(|| {
        let monitor = Arc::new(LoomStdMonitor::new(false));
        let predicate_checks = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        let waiter_monitor = Arc::clone(&monitor);
        let waiter_predicate_checks = Arc::clone(&predicate_checks);
        let waiter_completed = Arc::clone(&completed);
        let waiter = thread::spawn(move || {
            waiter_monitor.wait_until(
                |ready| {
                    waiter_predicate_checks.fetch_add(1, Ordering::SeqCst);
                    *ready
                },
                |_| {
                    waiter_completed.fetch_add(1, Ordering::SeqCst);
                },
            );
        });

        while predicate_checks.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        monitor.with_write_notify_one(|ready| *ready = true);

        waiter.join().expect("model waiter should finish");
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    });
}

/// Models a broadcast after two waiters have observed the unavailable state.
#[test]
fn test_loom_std_monitor_notify_all_releases_every_registered_waiter() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let monitor = Arc::new(LoomStdMonitor::new(false));
        let predicate_checks = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::with_capacity(2);

        for _ in 0..2 {
            let waiter_monitor = Arc::clone(&monitor);
            let waiter_predicate_checks = Arc::clone(&predicate_checks);
            let waiter_completed = Arc::clone(&completed);
            waiters.push(thread::spawn(move || {
                waiter_monitor.wait_until(
                    |ready| {
                        waiter_predicate_checks.fetch_add(1, Ordering::SeqCst);
                        *ready
                    },
                    |_| {
                        waiter_completed.fetch_add(1, Ordering::SeqCst);
                    },
                );
            }));
        }

        while predicate_checks.load(Ordering::SeqCst) < 2 {
            thread::yield_now();
        }
        monitor.with_write_notify_all(|ready| *ready = true);

        for waiter in waiters {
            waiter.join().expect("model waiter should finish");
        }
        assert_eq!(completed.load(Ordering::SeqCst), 2);
    });
}

/// Models a memoryless notification that is superseded by the protected state.
#[test]
fn test_loom_std_monitor_state_change_prevents_late_waiter_from_parking() {
    loom::model(|| {
        let monitor = Arc::new(LoomStdMonitor::new(false));
        let completed = Arc::new(AtomicUsize::new(0));

        monitor.with_write_notify_one(|ready| *ready = true);

        let waiter_monitor = Arc::clone(&monitor);
        let waiter_completed = Arc::clone(&completed);
        let waiter = thread::spawn(move || {
            waiter_monitor.wait_until(
                |ready| *ready,
                |_| {
                    waiter_completed.fetch_add(1, Ordering::SeqCst);
                },
            );
        });

        waiter.join().expect("late model waiter should finish");
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    });
}
