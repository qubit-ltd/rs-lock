// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the blocking waiter registry.

use std::{sync::Arc, thread};

use qubit_lock::ParkingLotMonitor;

/// Verifies notify-all permits every ready registered waiter to complete.
#[test]
fn test_blocking_waiter_registry_notifies_all_ready_waiters() {
    let monitor = Arc::new(ParkingLotMonitor::new(0_usize));
    let waiters = (0..2)
        .map(|_| {
            let monitor = Arc::clone(&monitor);
            thread::spawn(move || {
                monitor.wait_until(
                    |available| *available > 0,
                    |available| {
                        *available -= 1;
                    },
                )
            })
        })
        .collect::<Vec<_>>();

    monitor.with_write_notify_all(|available| *available = 2);

    for waiter in waiters {
        waiter.join().expect("waiter should not panic");
    }
}
