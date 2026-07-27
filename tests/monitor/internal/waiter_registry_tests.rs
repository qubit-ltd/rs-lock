// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Loom models for Monitor waiter registration, selection, and cancellation.

use loom::{sync::Arc, thread};
use qubit_lock::test_util::loom::LoomWaiterRegistry;

/// Models notification selection racing with cancellation of another waiter.
#[test]
fn test_loom_waiter_registry_notify_one_races_with_cancellation() {
    loom::model(|| {
        let registry = Arc::new(LoomWaiterRegistry::new());
        let _ = registry.register(10);
        let cancelled_waiter_id = registry.register(20);

        let notifier_registry = Arc::clone(&registry);
        let notifier = thread::spawn(move || notifier_registry.take_one());
        let cancellation_registry = Arc::clone(&registry);
        let cancellation =
            thread::spawn(move || cancellation_registry.unregister(cancelled_waiter_id));

        assert_eq!(
            notifier.join().expect("model notifier should finish"),
            Some(10)
        );
        assert_eq!(
            cancellation
                .join()
                .expect("model cancellation should finish"),
            Some(20),
        );
        assert_eq!(registry.take_one(), None);
    });
}

/// Models notification selection racing with cancellation of the same waiter.
#[test]
fn test_loom_waiter_registry_notify_one_races_with_selected_cancellation() {
    loom::model(|| {
        let registry = Arc::new(LoomWaiterRegistry::new());
        let waiter_id = registry.register(10);

        let notifier_registry = Arc::clone(&registry);
        let notifier = thread::spawn(move || notifier_registry.take_one());
        let cancellation_registry = Arc::clone(&registry);
        let cancellation = thread::spawn(move || cancellation_registry.unregister(waiter_id));

        let selected = notifier.join().expect("model notifier should finish");
        let cancelled = cancellation
            .join()
            .expect("model cancellation should finish");
        assert!(
            (selected == Some(10)) != (cancelled == Some(10)),
            "notification and cancellation must remove the waiter exactly once",
        );
        assert_eq!(registry.take_one(), None);
    });
}

/// Models notification of all waiters racing with cancellation of one waiter.
#[test]
fn test_loom_waiter_registry_notify_all_races_with_cancellation() {
    loom::model(|| {
        let registry = Arc::new(LoomWaiterRegistry::new());
        let first_waiter_id = registry.register(10);
        let _ = registry.register(20);

        let notifier_registry = Arc::clone(&registry);
        let notifier = thread::spawn(move || notifier_registry.take_all());
        let cancellation_registry = Arc::clone(&registry);
        let cancellation = thread::spawn(move || cancellation_registry.unregister(first_waiter_id));

        let selected = notifier.join().expect("model notifier should finish");
        let cancelled = cancellation
            .join()
            .expect("model cancellation should finish");
        let observed_first = selected.contains(&10) || cancelled == Some(10);
        assert!(observed_first, "first waiter must be removed exactly once");
        assert!(selected.contains(&20), "second waiter must be notified");
        assert_eq!(registry.take_one(), None);
    });
}
