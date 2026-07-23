// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for exclusive synchronous lock-acquisition modes.

use std::sync::{
    Arc,
    Mutex,
    RwLock,
};

#[cfg(feature = "parking-lot")]
use parking_lot::Mutex as ParkingLotMutex;
use qubit_lock::{
    ExclusiveLock,
    ReadWriteLock,
};

/// Accepts any acquisition mode that promises exclusive entry.
fn require_exclusive<L>(lock: &L)
where
    L: ExclusiveLock + ?Sized,
{
    let guard = lock.lock();
    drop(guard);
}

/// Verifies standard mutexes and forwarding implementations advertise
/// exclusive acquisition.
#[test]
fn test_exclusive_lock_accepts_std_mutex_and_forwarding() {
    let lock = Arc::new(Mutex::new(()));

    require_exclusive(lock.as_ref());
    require_exclusive(&lock);
    require_exclusive(&&*lock);
}

/// Verifies the write-mode adapter advertises exclusive acquisition.
#[test]
fn test_exclusive_lock_accepts_write_lock_adapter() {
    let lock = RwLock::new(());
    let write_lock = ReadWriteLock::write_lock(&lock);

    require_exclusive(&write_lock);
}

/// Verifies parking-lot mutexes advertise exclusive acquisition.
#[cfg(feature = "parking-lot")]
#[test]
fn test_exclusive_lock_accepts_parking_lot_mutex() {
    let lock = ParkingLotMutex::new(());

    require_exclusive(&lock);
}
