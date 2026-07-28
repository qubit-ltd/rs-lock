// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`SharedAsyncMonitor`](qubit_lock::SharedAsyncMonitor).

use std::sync::Arc;

use qubit_lock::{
    SharedAsyncMonitor,
    TokioMonitor,
};

/// Clones a monitor through the aggregate shared async capability.
fn clone_through_trait<M>(monitor: M) -> M
where
    M: SharedAsyncMonitor<State = bool>,
{
    monitor.clone()
}

#[tokio::test]
/// Verifies a Tokio handle satisfies [`SharedAsyncMonitor`].
async fn test_shared_async_monitor_trait_accepts_tokio_monitor_handle() {
    let monitor = clone_through_trait(Arc::new(TokioMonitor::current(false)));
    assert!(!monitor.with_read_async(|ready| *ready).await);
}

#[tokio::test]
/// Verifies [`Arc`] forwarding also satisfies [`SharedAsyncMonitor`].
async fn test_shared_async_monitor_trait_accepts_arc_forwarding() {
    let monitor = Arc::new(TokioMonitor::current(false));
    let monitor = clone_through_trait(monitor);
    assert!(!monitor.with_read_async(|ready| *ready).await);
}
