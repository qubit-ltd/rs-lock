// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests the normal-build synchronization selection through StdMonitor.

use qubit_lock::StdMonitor;

/// Verifies that the normal synchronization selection supports monitor access.
#[test]
fn test_std_monitor_uses_normal_synchronization_primitives() {
    let monitor = StdMonitor::new(1_u8);

    monitor.with_write(|value| *value += 1);

    assert_eq!(monitor.with_read(|value| *value), 2);
}
