// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for synchronous read-write lock capabilities.

use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

#[cfg(feature = "parking-lot")]
use parking_lot::RwLock as ParkingLotRwLock;
use qubit_lock::ReadWriteLock;
use qubit_lock::TryLockError;

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
    assert_eq!(
        *ReadWriteLock::try_read(&lock).expect("read should succeed"),
        2,
    );
    *ReadWriteLock::try_write(&lock).expect("write should succeed") = 3;
}

#[test]
fn test_read_write_lock_accepts_borrowed_forwarding() {
    let lock = RwLock::new(1);
    let borrowed = &lock;

    assert_eq!(*ReadWriteLock::read(&borrowed), 1);
    *ReadWriteLock::write(&borrowed) = 2;
    assert_eq!(
        *ReadWriteLock::try_read(&borrowed).expect("read should succeed"),
        2,
    );
    *ReadWriteLock::try_write(&borrowed).expect("write should succeed") = 3;
}

#[test]
fn test_read_write_lock_std_try_modes_report_contention() {
    let lock = RwLock::new(0);
    let write_guard = ReadWriteLock::write(&lock);

    assert!(matches!(
        ReadWriteLock::try_read(&lock),
        Err(TryLockError::WouldBlock)
    ));
    assert!(matches!(
        ReadWriteLock::try_write(&lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(write_guard);

    let read_guard = ReadWriteLock::read(&lock);
    assert!(matches!(
        ReadWriteLock::try_write(&lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(read_guard);
}

#[test]
fn test_read_write_lock_std_try_modes_report_poisoning() {
    let lock = Arc::new(RwLock::new(0));
    let poisoned = Arc::clone(&lock);
    let _ = thread::spawn(move || {
        let _guard = ReadWriteLock::write(&*poisoned);
        panic!("poison read-write lock");
    })
    .join();

    assert!(matches!(
        ReadWriteLock::try_read(&*lock),
        Err(TryLockError::Poisoned)
    ));
    assert!(matches!(
        ReadWriteLock::try_write(&*lock),
        Err(TryLockError::Poisoned)
    ));
}

#[test]
#[cfg(feature = "parking-lot")]
fn test_read_write_lock_supports_parking_lot_modes() {
    let lock = ParkingLotRwLock::new(1);

    assert_eq!(*ReadWriteLock::read(&lock), 1);
    *ReadWriteLock::write(&lock) = 2;
    assert_eq!(
        *ReadWriteLock::try_read(&lock).expect("read should succeed"),
        2,
    );
    *ReadWriteLock::try_write(&lock).expect("write should succeed") = 3;

    let write_guard = ReadWriteLock::write(&lock);
    assert!(matches!(
        ReadWriteLock::try_read(&lock),
        Err(TryLockError::WouldBlock)
    ));
    assert!(matches!(
        ReadWriteLock::try_write(&lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(write_guard);
}
