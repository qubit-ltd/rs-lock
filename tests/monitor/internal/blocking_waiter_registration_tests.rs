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

fn collect_waiter<M: Monitor<State = bool>>(monitor: &M, done: &mpsc::Receiver<bool>, waiter: JoinHandle<()>) -> bool {
    let observed = done.recv_timeout(REAL_LIMIT);
    let completed_on_time = observed.is_ok();
    let result = match observed {
        Ok(value) => Ok(value),
        Err(_) => {
            monitor.with_write_notify_all(|ready| *ready = true);
            done.recv_timeout(REAL_LIMIT)
        }
    };
    if result.is_ok() {
        waiter.join().expect("completed waiter should not panic");
    }
    let value = result.expect("waiter should finish even during cleanup");
    assert!(
        completed_on_time,
        "waiter required cleanup instead of the intended signal"
    );
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
    if advanced.is_none() {
        monitor.with_write_notify_all(|ready| *ready = true);
    }
    let first_timed_out = collect_waiter(&*monitor, &first_rx, first);
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
    let checked = checked_rx.recv_timeout(REAL_LIMIT);
    if checked.is_ok() {
        monitor.with_write_notify_one(|ready| *ready = true);
    } else {
        monitor.with_write_notify_all(|ready| *ready = true);
    }
    let completed = collect_waiter(&*monitor, &done_rx, second);
    assert!(checked.is_ok(), "second waiter must observe blocked state");
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
