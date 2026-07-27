// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`SharedMonitor`](qubit_lock::SharedMonitor).

use qubit_lock::{ArcStdMonitor, SharedMonitor};

/// Clones a monitor through the aggregate shared capability.
fn clone_through_trait<M>(monitor: M) -> M
where
    M: SharedMonitor<State = bool>,
{
    monitor.clone()
}

#[test]
/// Verifies the parking-lot handle satisfies [`SharedMonitor`].
fn test_shared_monitor_trait_accepts_std_monitor_handle() {
    let monitor = clone_through_trait(ArcStdMonitor::new(false));
    assert!(!monitor.with_read(|ready| *ready));
}
