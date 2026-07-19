// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for monitor internal-module collaboration through public behavior.

use std::{
    sync::Arc,
    thread,
};

use qubit_lock::ParkingLotMonitor;

/// Verifies that the internal waiter registry and waiter signal cooperate to
/// complete a public predicate wait.
#[test]
fn test_internal_monitor_components_complete_public_predicate_wait() {
    let monitor = Arc::new(ParkingLotMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until(|ready| *ready, |_| ());
    });

    monitor.with_write_notify_one(|ready| *ready = true);
    waiter.join().expect("monitor waiter should complete");
}
