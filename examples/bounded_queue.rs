// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runs the closable bounded-queue case study from the user guide.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use qubit_lock::{
    Monitor,
    ParkingLotMonitor,
    TimedMonitor,
    WaitTimeoutResult,
};

/// Holds the queue data and its close state.
pub(crate) struct QueueState<T> {
    items: VecDeque<T>,
    capacity: NonZeroUsize,
    closed: bool,
}

impl<T> QueueState<T> {
    /// Creates an open queue with the specified non-zero capacity.
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            items: VecDeque::new(),
            capacity,
            closed: false,
        }
    }
}

/// Adds an item once the queue has space, or returns it after closure.
pub(crate) fn push<M, T>(queue: &M, item: T) -> Result<(), T>
where
    M: Monitor<State = QueueState<T>> + ?Sized,
{
    let result = queue.wait_until(
        |state| state.closed || state.items.len() < state.capacity.get(),
        |state| {
            if state.closed {
                Err(item)
            } else {
                state.items.push_back(item);
                Ok(())
            }
        },
    );
    if result.is_ok() {
        queue.notify_all();
    }
    result
}

/// Removes an item, waiting until an item is available or the queue closes.
fn pop<M, T>(queue: &M) -> Option<T>
where
    M: Monitor<State = QueueState<T>> + ?Sized,
{
    let item = queue.wait_until(
        |state| state.closed || !state.items.is_empty(),
        |state| state.items.pop_front(),
    );
    if item.is_some() {
        queue.notify_all();
    }
    item
}

/// Waits up to the condition-wait budget for an item or queue closure.
pub(crate) fn pop_for<M, T>(
    queue: &M,
    timeout: Duration,
) -> Result<WaitTimeoutResult<Option<T>>, qubit_clock::TimeError>
where
    M: TimedMonitor<State = QueueState<T>> + ?Sized,
{
    let result = queue.wait_until_for(
        timeout,
        |state| state.closed || !state.items.is_empty(),
        |state| state.items.pop_front(),
    );
    if matches!(&result, Ok(WaitTimeoutResult::Ready(Some(_)))) {
        queue.notify_all();
    }
    result
}

/// Closes the queue and wakes all producers and consumers.
fn close<M, T>(queue: &M)
where
    M: Monitor<State = QueueState<T>> + ?Sized,
{
    queue.with_write_notify_all(|state| state.closed = true);
}

/// Exercises timeout, blocking wakeup, enqueue, dequeue, and closure behavior.
fn main() {
    let capacity =
        NonZeroUsize::new(1).expect("queue capacity must be non-zero");
    let queue = Arc::new(ParkingLotMonitor::new(QueueState::new(capacity)));

    assert!(matches!(
        pop_for(&*queue, Duration::ZERO),
        Ok(WaitTimeoutResult::TimedOut),
    ));

    let (predicate_checked_sender, predicate_checked_receiver) =
        mpsc::sync_channel(0);
    let waiting_queue = Arc::clone(&queue);
    let consumer = thread::spawn(move || {
        let mut predicate_checked_sender = Some(predicate_checked_sender);
        waiting_queue.wait_until(
            |state| {
                if let Some(sender) = predicate_checked_sender.take() {
                    sender
                        .send(())
                        .expect("main thread should observe the empty queue");
                }
                state.closed || !state.items.is_empty()
            },
            |state| state.items.pop_front(),
        )
    });

    predicate_checked_receiver
        .recv()
        .expect("consumer should check the empty queue");
    assert_eq!(push(&*queue, 7), Ok(()));
    assert_eq!(consumer.join().expect("consumer should finish"), Some(7));

    close(&*queue);
    assert_eq!(push(&*queue, 8), Err(8));
    assert_eq!(pop(&*queue), None);
}
