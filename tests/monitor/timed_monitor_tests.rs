// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TimedMonitor`](qubit_lock::TimedMonitor).

use std::sync::Arc;
use std::time::Duration;

use qubit_lock::StdMonitor;
use qubit_lock::TimedMonitor;
use qubit_lock::WaitTimeoutResult;

/// Exercises timed waiting through the aggregate timed capability.
fn wait_through_trait<M>(monitor: &M)
where
    M: TimedMonitor<State = bool>,
{
    assert_time_result_eq!(
        monitor.wait_until_for(Duration::ZERO, |ready| *ready, |_| 7),
        Ok(WaitTimeoutResult::TimedOut),
    );
}

/// Verifies a named shared monitor handle satisfies [`TimedMonitor`].
#[test]
fn test_timed_monitor_trait_accepts_std_monitor() {
    wait_through_trait(&Arc::new(StdMonitor::new(false)));
}

/// Verifies [`Arc`] forwards the aggregate timed monitor capability.
#[test]
fn test_timed_monitor_trait_accepts_arc_wrapped_implementation() {
    wait_through_trait(&Arc::new(StdMonitor::new(false)));
}
