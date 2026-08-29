// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Benchmarks blocking waiter registration, notification, and wake-up.
//!
//! Each fixture owns one long-lived worker. Timing starts only after that
//! worker reports that it holds the state lock, then covers the `Start`
//! command, waiter registration, lock handoff, notification, wake-up, and
//! `Done` acknowledgement.

use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::{self};
use std::thread::JoinHandle;
use std::thread::{self};
use std::time::Duration;
use std::time::Instant;

use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use parking_lot::Condvar;
use parking_lot::Mutex;
use qubit_lock::ParkingLotMonitor;

/// Command sent to a long-lived benchmark worker.
enum WorkerCommand {
    /// Register one blocking wait.
    Start,
    /// Terminate the worker.
    Stop,
}

/// Long-lived worker exercising [`ParkingLotMonitor`] waiter registration.
struct MonitorWaitRegistration {
    monitor: Arc<ParkingLotMonitor<usize>>,
    command_sender: Sender<WorkerCommand>,
    ready_receiver: Receiver<()>,
    done_receiver: Receiver<()>,
    worker: JoinHandle<()>,
}

impl MonitorWaitRegistration {
    /// Starts a worker and waits until it owns the monitor state lock.
    ///
    /// # Returns
    ///
    /// A fixture ready to execute repeated registration round trips.
    fn new() -> Self {
        let monitor = Arc::new(ParkingLotMonitor::new(0));
        let (command_sender, command_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (done_sender, done_receiver) = mpsc::channel();
        let worker_monitor = Arc::clone(&monitor);
        let worker = thread::spawn(move || {
            let mut state = worker_monitor.lock();
            loop {
                ready_sender
                    .send(())
                    .expect("benchmark should observe the ready worker");
                match command_receiver
                    .recv()
                    .expect("benchmark worker should receive a command")
                {
                    WorkerCommand::Start => {
                        state.wait();
                        done_sender.send(()).expect("benchmark should observe the woken worker");
                    }
                    WorkerCommand::Stop => return,
                }
            }
        });
        ready_receiver.recv().expect("benchmark worker should become ready");
        Self {
            monitor,
            command_sender,
            ready_receiver,
            done_receiver,
            worker,
        }
    }

    /// Measures one waiter registration, notification, and wake-up.
    ///
    /// # Returns
    ///
    /// Elapsed time excluding preparation for the next iteration.
    fn round_trip(&self) -> Duration {
        let started = Instant::now();
        self.command_sender
            .send(WorkerCommand::Start)
            .expect("benchmark worker should start a wait");
        self.monitor.with_write_notify_one(|state| {
            *state = state.wrapping_add(1);
        });
        self.done_receiver
            .recv()
            .expect("benchmark worker should finish a wait");
        let elapsed = started.elapsed();
        self.ready_receiver
            .recv()
            .expect("benchmark worker should become ready");
        elapsed
    }

    /// Stops and joins the worker.
    fn stop(self) {
        self.command_sender
            .send(WorkerCommand::Stop)
            .expect("benchmark worker should stop");
        self.worker.join().expect("benchmark worker should not panic");
    }
}

/// Long-lived worker exercising a raw parking-lot condition variable.
struct CondvarWaitRegistration {
    condition: Arc<(Mutex<usize>, Condvar)>,
    command_sender: Sender<WorkerCommand>,
    ready_receiver: Receiver<()>,
    done_receiver: Receiver<()>,
    worker: JoinHandle<()>,
}

impl CondvarWaitRegistration {
    /// Starts a worker and waits until it owns the condition state lock.
    ///
    /// # Returns
    ///
    /// A fixture ready to execute repeated registration round trips.
    fn new() -> Self {
        let condition = Arc::new((Mutex::new(0), Condvar::new()));
        let (command_sender, command_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (done_sender, done_receiver) = mpsc::channel();
        let worker_condition = Arc::clone(&condition);
        let worker = thread::spawn(move || {
            let (state, changed) = &*worker_condition;
            let mut state = state.lock();
            loop {
                ready_sender
                    .send(())
                    .expect("benchmark should observe the ready worker");
                match command_receiver
                    .recv()
                    .expect("benchmark worker should receive a command")
                {
                    WorkerCommand::Start => {
                        changed.wait(&mut state);
                        done_sender.send(()).expect("benchmark should observe the woken worker");
                    }
                    WorkerCommand::Stop => return,
                }
            }
        });
        ready_receiver.recv().expect("benchmark worker should become ready");
        Self {
            condition,
            command_sender,
            ready_receiver,
            done_receiver,
            worker,
        }
    }

    /// Measures one waiter registration, notification, and wake-up.
    ///
    /// # Returns
    ///
    /// Elapsed time excluding preparation for the next iteration.
    fn round_trip(&self) -> Duration {
        let started = Instant::now();
        self.command_sender
            .send(WorkerCommand::Start)
            .expect("benchmark worker should start a wait");
        let (state, changed) = &*self.condition;
        let mut state = state.lock();
        *state = state.wrapping_add(1);
        drop(state);
        changed.notify_one();
        self.done_receiver
            .recv()
            .expect("benchmark worker should finish a wait");
        let elapsed = started.elapsed();
        self.ready_receiver
            .recv()
            .expect("benchmark worker should become ready");
        elapsed
    }

    /// Stops and joins the worker.
    fn stop(self) {
        self.command_sender
            .send(WorkerCommand::Stop)
            .expect("benchmark worker should stop");
        self.worker.join().expect("benchmark worker should not panic");
    }
}

/// Measures the steady-state blocking waiter registration round trip.
///
/// # Parameters
///
/// * `criterion` - Criterion runner receiving the benchmark cases.
fn benchmark_blocking_wait_registration_round_trip(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("blocking_wait_registration_round_trip");

    let monitor = MonitorWaitRegistration::new();
    group.bench_function("parking_lot_monitor", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                elapsed += monitor.round_trip();
            }
            elapsed
        });
    });
    monitor.stop();

    let condvar = CondvarWaitRegistration::new();
    group.bench_function("parking_lot_condvar", |bencher| {
        bencher.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                elapsed += condvar.round_trip();
            }
            elapsed
        });
    });
    condvar.stop();

    group.finish();
}

criterion_group!(benches, benchmark_blocking_wait_registration_round_trip);
criterion_main!(benches);
