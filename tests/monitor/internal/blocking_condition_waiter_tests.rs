// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the internal blocking condition waiter.

use std::{sync::Arc, thread};

use qubit_lock::StdMonitor;

/// Verifies a blocking waiter rechecks state after notification.
#[test]
fn test_blocking_condition_waiter_observes_ready_state() {
    let monitor = Arc::new(StdMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || waiter_monitor.wait_until(|ready| *ready, |_| 7));

    monitor.with_write_notify_one(|ready| *ready = true);

    assert_eq!(waiter.join().expect("waiter should not panic"), 7);
}
