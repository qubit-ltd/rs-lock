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

use parking_lot::RwLock as ParkingLotRwLock;
use qubit_lock::{
    Lock,
    ReadWriteLock,
    TryLockError,
};

#[test]
fn test_read_write_lock_std_modes_return_native_guards() {
    let lock = RwLock::new(7);

    assert_eq!(*ReadWriteLock::read(&lock), 7);
    *ReadWriteLock::write(&lock) = 11;
    assert_eq!(*ReadWriteLock::read(&lock), 11);
}

#[test]
fn test_read_write_lock_read_adapter_is_shared() {
    let lock = ParkingLotRwLock::new(());
    let read_lock = ReadWriteLock::read_lock(&lock);
    let first = Lock::lock(&read_lock);

    assert!(Lock::try_lock(&read_lock).is_ok());
    drop(first);
}

#[test]
fn test_read_write_lock_write_adapter_is_exclusive() {
    let lock = ParkingLotRwLock::new(());
    let write_lock = ReadWriteLock::write_lock(&lock);
    let guard = Lock::lock(&write_lock);

    assert!(matches!(
        Lock::try_lock(&write_lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(guard);
    assert!(Lock::try_lock(&write_lock).is_ok());
}

#[test]
fn test_read_write_lock_accepts_arc_forwarding() {
    let lock = Arc::new(RwLock::new(1));

    assert_eq!(*ReadWriteLock::read(&lock), 1);
    *ReadWriteLock::write(&lock) = 2;
    assert_eq!(*ReadWriteLock::read(&lock), 2);
}
