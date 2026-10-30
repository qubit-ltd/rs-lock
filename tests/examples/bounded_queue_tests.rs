// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests the notification protocol demonstrated by the bounded queue example.

#[allow(dead_code)]
#[path = "../../examples/bounded_queue.rs"]
mod bounded_queue;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use bounded_queue::QueueState;
use bounded_queue::pop_for;
use bounded_queue::push;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_lock::ConditionWaiter;
use qubit_lock::Monitor;
use qubit_lock::Notifier;
use qubit_lock::ParkingLotMonitor;
use qubit_lock::TimeoutConditionWaiter;
use qubit_lock::WaitTimeoutResult;

/// Counts notifications while delegating queue behavior to a real monitor.
struct CountingMonitor {
    /// Real monitor that owns the example state and waiting protocol.
    inner: ParkingLotMonitor<QueueState<i32>>,
    /// Number of calls made through [`Notifier::notify_all`].
    notify_all_count: AtomicUsize,
    /// One-shot observer for a predicate that finds the queue full.
    blocked_tx: Mutex<Option<Sender<()>>>,
}

impl CountingMonitor {
    /// Creates a monitor around the specified queue state.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial bounded queue state.
    ///
    /// # Returns
    ///
    /// A monitor with no observed notifications.
    fn new(state: QueueState<i32>) -> Self {
        Self {
            inner: ParkingLotMonitor::new(state),
            notify_all_count: AtomicUsize::new(0),
            blocked_tx: Mutex::new(None),
        }
    }

    /// Returns the number of observed all-waiter notifications.
    ///
    /// # Returns
    ///
    /// The current notification count.
    fn notify_all_count(&self) -> usize {
        self.notify_all_count.load(Ordering::SeqCst)
    }
}

impl Notifier for CountingMonitor {
    /// Delegates a one-waiter notification to the real monitor.
    fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Records and delegates an all-waiter notification.
    fn notify_all(&self) {
        self.notify_all_count.fetch_add(1, Ordering::SeqCst);
        self.inner.notify_all();
    }
}

impl ConditionWaiter for CountingMonitor {
    type State = QueueState<i32>;

    /// Delegates an untimed predicate wait to the real monitor.
    fn wait_while<R, P, F>(&self, mut predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.wait_while(
            |state| {
                let waiting = predicate(state);
                if waiting
                    && let Some(sender) = self.blocked_tx.lock().expect("observer mutex should not poison").take()
                {
                    sender.send(()).expect("test should observe blocked producer");
                }
                waiting
            },
            action,
        )
    }
}

impl Monitor for CountingMonitor {
    /// Delegates immutable state access to the real monitor.
    fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&Self::State) -> R,
    {
        self.inner.with_read(f)
    }

    /// Delegates mutable state access to the real monitor.
    fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.with_write(f)
    }
}

impl TimeoutConditionWaiter for CountingMonitor {
    /// Delegates an absolute-deadline predicate wait to the real monitor.
    fn wait_while_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.wait_while_with_deadline(deadline, predicate, action)
    }

    /// Delegates an operation-wide timeout predicate wait to the real monitor.
    fn wait_while_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.wait_while_with_total_timeout(timeout, predicate, action)
    }

    /// Delegates a condition-wait timeout to the real monitor.
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.wait_while_for(timeout, predicate, action)
    }
}

#[test]
fn test_pop_for_notifies_after_releasing_capacity() {
    let capacity = NonZeroUsize::new(1).expect("test capacity must be non-zero");
    let queue = CountingMonitor::new(QueueState::new(capacity));
    assert_eq!(push(&queue, 1), Ok(()));
    let notifications_before_pop = queue.notify_all_count();

    let result = pop_for(&queue, Duration::ZERO).expect("zero-timeout dequeue should use the monitor timer domain");
    assert_eq!(result, WaitTimeoutResult::Ready(Some(1)));
    assert_eq!(
        queue.notify_all_count(),
        notifications_before_pop + 1,
        "a successful timed dequeue must notify producers waiting for capacity",
    );
}

#[test]
fn test_pop_for_wakes_producer_waiting_for_capacity() {
    let capacity = NonZeroUsize::new(1).expect("capacity must be non-zero");
    let queue = Arc::new(CountingMonitor::new(QueueState::new(capacity)));
    assert_eq!(push(queue.as_ref(), 1), Ok(()));
    let (checked_tx, checked_rx) = mpsc::channel();
    *queue.blocked_tx.lock().expect("observer mutex should not poison") = Some(checked_tx);
    let (done_tx, done_rx) = mpsc::channel();
    let producer_queue = Arc::clone(&queue);
    let producer = thread::spawn(move || {
        let result = push(producer_queue.as_ref(), 2);
        done_tx.send(result).expect("test should receive producer result");
    });

    if checked_rx.recv_timeout(Duration::from_secs(5)).is_err() {
        // Release the producer's predicate by removing the known queued item.
        let _ = pop_for(queue.as_ref(), Duration::ZERO);
        queue.notify_all();
        let completion = done_rx.recv_timeout(Duration::from_secs(6));
        if completion.is_ok() {
            producer.join().expect("producer should finish after early cleanup");
        }
        panic!("producer should observe full queue");
    }

    drop(queue.inner.lock());
    let dequeued = pop_for(queue.as_ref(), Duration::ZERO);
    let completion = done_rx.recv_timeout(Duration::from_secs(6));
    if completion.is_err() {
        // Retry notification after the known dequeue so the producer can
        // observe the released capacity even if the first notify was lost.
        queue.notify_all();
        let cleaned = done_rx.recv_timeout(Duration::from_secs(6));
        if cleaned.is_ok() {
            producer.join().expect("producer should finish cleanup");
        }
        panic!("timed dequeue failed to wake registered producer: {completion:?}");
    }
    let completion = completion.expect("producer completion was checked");
    producer.join().expect("producer should finish");
    assert_eq!(
        dequeued.expect("dequeue should succeed"),
        WaitTimeoutResult::Ready(Some(1)),
    );
    assert_eq!(completion, Ok(()),);
}
