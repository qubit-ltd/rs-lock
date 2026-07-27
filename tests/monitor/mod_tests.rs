// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for monitor module crate-root exports.

use qubit_lock::{Monitor, StdMonitor, TimedMonitor};

/// Verifies that the monitor feature exposes the standard monitor contract.
#[test]
fn test_monitor_module_exports_standard_monitor_contract() {
    fn accepts_monitor<M: Monitor>() {}
    fn accepts_timed_monitor<M: TimedMonitor>() {}

    accepts_monitor::<StdMonitor<usize>>();
    accepts_timed_monitor::<StdMonitor<usize>>();
}
