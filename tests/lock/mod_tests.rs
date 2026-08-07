// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the lock module's crate-root exports.

use qubit_lock::ExclusiveLock;
use qubit_lock::Lock;
use qubit_lock::ReadWriteLock;

/// Verifies that synchronous lock traits remain available without optional
/// features.
#[test]
fn test_lock_module_exports_synchronous_lock_traits() {
    fn accepts_exclusive_lock<L: ExclusiveLock + ?Sized>() {}
    fn accepts_lock<L: Lock + ?Sized>() {}
    fn accepts_read_write_lock<L: ReadWriteLock + ?Sized>() {}

    accepts_exclusive_lock::<std::sync::Mutex<usize>>();
    accepts_lock::<std::sync::Mutex<usize>>();
    accepts_read_write_lock::<std::sync::RwLock<usize>>();
}
