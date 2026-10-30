// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by blocking waiter registrations.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_lock::Monitor;
use qubit_lock::StdMonitor;
use qubit_lock::TimedMonitor;
use qubit_lock::WaitTimeoutResult;

const REAL_LIMIT: Duration = Duration::from_secs(5);

fn collect_waiter(done: &mpsc::Receiver<bool>, waiter: JoinHandle<()>) -> bool {
    let value = done
        .recv_timeout(REAL_LIMIT)
        .expect("waiter did not report a result within the real-time limit");
    waiter.join().expect("completed waiter should not panic");
    value
}

fn assert_timeout_unregisters<M>(monitor: Arc<M>, clock: Arc<ManualMonotonicClock>)
where
    M: TimedMonitor<State = bool> + Send + Sync + 'static,
{
    let (first_tx, first_rx) = mpsc::channel();
    let first_monitor = Arc::clone(&monitor);
    let first = thread::spawn(move || {
        let result = first_monitor.wait_until_ready_for(Duration::from_secs(8), |ready| *ready);
        first_tx
            .send(matches!(result, Ok(WaitTimeoutResult::TimedOut)))
            .expect("controller should receive first result");
    });
    let advanced = clock.advance_to_next_deadline_after_waiters(1, REAL_LIMIT);
    let first_timed_out = collect_waiter(&first_rx, first);
    assert!(advanced.is_some(), "first waiter must register before clock advance");
    assert!(first_timed_out, "first waiter must genuinely time out");

    let (checked_tx, checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let second_monitor = Arc::clone(&monitor);
    let second = thread::spawn(move || {
        let mut checked_tx = Some(checked_tx);
        second_monitor.wait_until_ready(|ready| {
            if !*ready && let Some(tx) = checked_tx.take() {
                tx.send(()).expect("controller should observe predicate check");
            }
            *ready
        });
        done_tx.send(true).expect("controller should receive second result");
    });
    checked_rx
        .recv_timeout(REAL_LIMIT)
        .expect("second waiter must observe blocked state");
    let notifier_monitor = Arc::clone(&monitor);
    let (notified_tx, notified_rx) = mpsc::channel();
    let notifier = thread::spawn(move || {
        notifier_monitor.with_write_notify_one(|ready| *ready = true);
        notified_tx
            .send(())
            .expect("controller should observe notification");
    });
    notified_rx
        .recv_timeout(REAL_LIMIT)
        .expect("notifier could not finish the monitor handshake");
    notifier.join().expect("notifier should not panic");
    let completed = collect_waiter(&done_rx, second);
    assert!(completed);
}

#[test]
fn test_blocking_waiter_registration_is_removed_after_timeout() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(StdMonitor::with_timer(false, clock.new_timer()));
    assert_timeout_unregisters(monitor, clock);
}

#[cfg(feature = "parking-lot")]
#[test]
fn test_parking_lot_waiter_registration_is_removed_after_timeout() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(qubit_lock::ParkingLotMonitor::with_timer(false, clock.new_timer()));
    assert_timeout_unregisters(monitor, clock);
}
