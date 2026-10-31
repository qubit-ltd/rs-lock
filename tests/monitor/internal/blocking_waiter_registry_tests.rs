// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the blocking waiter registry.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_lock::ParkingLotMonitor;

/// Verifies notify-all permits every ready registered waiter to complete.
#[test]
fn test_blocking_waiter_registry_notifies_all_ready_waiters() {
    let real_limit = Duration::from_secs(5);
    let monitor = Arc::new(ParkingLotMonitor::new(0_usize));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiters = (0..2)
        .map(|id| {
            let monitor = Arc::clone(&monitor);
            let checked_tx = checked_tx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let mut checked_tx = Some(checked_tx);
                monitor.wait_until(
                    move |available| {
                        if *available == 0
                            && let Some(sender) = checked_tx.take()
                        {
                            let _ = sender.send(id);
                        }
                        *available > 0
                    },
                    |available| *available -= 1,
                );
                let _ = done_tx.send(id);
            })
        })
        .collect::<Vec<_>>();
    drop(checked_tx);
    drop(done_tx);

    let mut checked = [false; 2];
    for _ in 0..2 {
        let id = checked_rx
            .recv_timeout(real_limit)
            .expect("each waiter should inspect the blocking predicate");
        assert!(
            !checked[id],
            "predicate handshake should identify distinct waiters"
        );
        checked[id] = true;
    }
    assert_eq!(checked, [true; 2]);

    let (notified_tx, notified_rx) = mpsc::channel();
    let notifier = thread::spawn(move || {
        monitor.with_write_notify_all(|available| *available = 2);
        let _ = notified_tx.send(());
    });
    let notified = notified_rx.recv_timeout(real_limit);
    let mut completed = [false; 2];
    let mut unique = true;
    for _ in 0..2 {
        match done_rx.recv_timeout(real_limit) {
            Ok(id) => {
                unique &= !completed[id];
                completed[id] = true;
            }
            Err(_) => break,
        }
    }
    if notified.is_ok() {
        notifier.join().expect("notifier should not panic");
    }
    for (id, waiter) in waiters.into_iter().enumerate() {
        if completed[id] {
            waiter.join().expect("completed waiter should not panic");
        }
    }
    assert_eq!(notified, Ok(()), "broadcast should return");
    assert!(unique, "completion must identify distinct waiters");
    assert_eq!(
        completed, [true; 2],
        "both registered waiters should finish"
    );
}
