// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by the internal Tokio condition waiter.

use std::{
    future::{
        Future,
        poll_fn,
    },
    task::Poll,
};

use qubit_lock::{
    AsyncConditionWaiter,
    TokioMonitor,
};

/// Verifies a Tokio waiter resumes and rechecks state after notification.
#[tokio::test]
async fn test_tokio_condition_waiter_observes_ready_state() {
    let monitor = TokioMonitor::current(false);
    let mut waiter = Box::pin(monitor.wait_until_async(|ready| *ready, |_| 7));

    poll_fn(|context| {
        assert!(waiter.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;

    monitor
        .with_write_notify_one_async(|ready| *ready = true)
        .await;

    assert_eq!(waiter.await, 7);
}
