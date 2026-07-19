// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the blocking condition waiter's state.

use std::{
    sync::Arc,
    thread,
};

use qubit_lock::StdMonitor;

/// Verifies notification state remains visible while the waiter reacquires the
/// monitor lock.
#[test]
fn test_blocking_condition_waiter_state_latches_notification() {
    let monitor = Arc::new(StdMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until(|ready| *ready, |ready| *ready)
    });

    monitor.with_write_notify_one(|ready| *ready = true);

    assert!(waiter.join().expect("waiter should not panic"));
}
