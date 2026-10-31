// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for monitor internal-module collaboration through public behavior.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_lock::ParkingLotMonitor;

/// Verifies that the internal waiter registry and waiter signal cooperate to
/// complete a public predicate wait.
#[test]
fn test_internal_monitor_components_complete_public_predicate_wait() {
    let real_limit = Duration::from_secs(5);
    let monitor = Arc::new(ParkingLotMonitor::new(false));
    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        waiter_monitor.wait_until(
            move |ready| {
                if !*ready && let Some(sender) = checked_tx.take() {
                    let _ = sender.send(());
                }
                *ready
            },
            |_| (),
        );
        let _ = done_tx.send(());
    });

    checked_rx
        .recv_timeout(real_limit)
        .expect("waiter should inspect the blocking predicate");
    let (notified_tx, notified_rx) = mpsc::channel();
    let notifier = thread::spawn(move || {
        monitor.with_write_notify_one(|ready| *ready = true);
        let _ = notified_tx.send(());
    });
    let notified = notified_rx.recv_timeout(real_limit);
    let completed = done_rx.recv_timeout(real_limit);
    if notified.is_ok() {
        notifier.join().expect("notifier should not panic");
    }
    if completed.is_ok() {
        waiter.join().expect("waiter should not panic");
    }
    assert_eq!(notified, Ok(()), "notification should return");
    assert_eq!(completed, Ok(()), "registered wait should finish");
}
