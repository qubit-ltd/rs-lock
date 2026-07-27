// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the Loom-facing waiter registry adapter.

use qubit_lock::test_util::loom::LoomWaiterRegistry;

/// Verifies that the adapter exposes FIFO selection and cancellation.
#[test]
fn test_loom_waiter_registry_selects_and_cancels_waiters() {
    loom::model(|| {
        let registry = LoomWaiterRegistry::new();
        let first_waiter_id = registry.register(10);
        let _ = registry.register(20);

        assert_eq!(registry.take_one(), Some(10));
        assert_eq!(registry.unregister(first_waiter_id), None);
        assert_eq!(registry.take_one(), Some(20));
        assert_eq!(registry.take_one(), None);
    });
}

/// Verifies cancellation gaps do not change FIFO selection of remaining
/// waiters.
#[test]
fn test_loom_waiter_registry_preserves_fifo_after_middle_cancellation() {
    loom::model(|| {
        let registry = LoomWaiterRegistry::new();
        let _ = registry.register(10);
        let cancelled_waiter_id = registry.register(20);
        let _ = registry.register(30);

        assert_eq!(registry.unregister(cancelled_waiter_id), Some(20));

        let _ = registry.register(40);
        assert_eq!(registry.take_all(), vec![10, 30, 40]);
    });
}
