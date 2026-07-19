// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public behavior tests backed by Tokio waiter registrations.

use std::time::Duration;

use qubit_lock::{
    AsyncConditionWaiter,
    TokioMonitor,
};

/// Verifies cancelling a pending Tokio wait removes its registration.
#[tokio::test]
async fn test_tokio_waiter_registration_is_removed_after_cancellation() {
    let monitor = TokioMonitor::new(false);
    let mut cancelled =
        Box::pin(monitor.wait_until_async(|ready| *ready, |_| unreachable!()));

    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut cancelled)
            .await
            .is_err()
    );
    drop(cancelled);

    monitor.notify_one();
    monitor.with_write_async(|ready| *ready = true).await;
    assert!(
        monitor
            .wait_until_async(|ready| *ready, |ready| *ready)
            .await
    );
}
