// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for data-independent synchronous RAII locks.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;

#[cfg(feature = "parking-lot")]
use parking_lot::Mutex as ParkingLotMutex;
use qubit_lock::Lock;
use qubit_lock::TryLockError;

/// Acquires and immediately releases any generic synchronous lock.
fn acquire_once<L>(lock: &L)
where
    L: Lock + ?Sized,
{
    let guard = lock.lock();
    drop(guard);
}

#[test]
fn test_lock_std_mutex_releases_on_guard_drop() {
    let lock = Mutex::new(7);
    let guard = Lock::lock(&lock);

    assert!(matches!(Lock::try_lock(&lock), Err(TryLockError::WouldBlock)));
    drop(guard);
    assert!(Lock::try_lock(&lock).is_ok());
}

#[test]
#[cfg(feature = "parking-lot")]
fn test_lock_parking_lot_mutex_releases_during_unwind() {
    let lock = ParkingLotMutex::new(());

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _guard = Lock::lock(&lock);
        panic!("intentional panic while holding lock");
    }));

    assert!(panic.is_err());
    assert!(Lock::try_lock(&lock).is_ok());
}

#[test]
fn test_lock_accepts_arc_and_borrowed_forwarding() {
    let lock = Arc::new(Mutex::new(()));

    acquire_once(&lock);
    acquire_once(&&*lock);
    assert!(Lock::try_lock(&lock).is_ok());
    assert!(Lock::try_lock(&&*lock).is_ok());
}

#[test]
fn test_lock_std_mutex_reports_poisoning() {
    let lock = Arc::new(Mutex::new(()));
    let poisoned = Arc::clone(&lock);
    let _ = std::thread::spawn(move || {
        let _guard = Lock::lock(&*poisoned);
        panic!("poison mutex");
    })
    .join();

    assert!(matches!(Lock::try_lock(&*lock), Err(TryLockError::Poisoned)));
    assert!(catch_unwind(AssertUnwindSafe(|| Lock::lock(&*lock))).is_err());
}
