// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for synchronous read-write lock capabilities.

use std::sync::{
    Arc,
    RwLock,
};

use qubit_lock::ReadWriteLock;

#[test]
fn test_read_write_lock_std_modes_return_native_guards() {
    let lock = RwLock::new(7);

    assert_eq!(*ReadWriteLock::read(&lock), 7);
    *ReadWriteLock::write(&lock) = 11;
    assert_eq!(*ReadWriteLock::read(&lock), 11);
}

#[test]
fn test_read_write_lock_accepts_arc_forwarding() {
    let lock = Arc::new(RwLock::new(1));

    assert_eq!(*ReadWriteLock::read(&lock), 1);
    *ReadWriteLock::write(&lock) = 2;
    assert_eq!(*ReadWriteLock::read(&lock), 2);
}
