/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
// qubit-style: allow explicit-imports
//! # Parking Lot RwLock Tests
//!
//! Tests for the parking_lot::RwLock implementation of the Lock trait.

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use parking_lot::RwLock as ParkingLotRwLock;
use qubit_lock::lock::{
    Lock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod parking_lot_rw_lock_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    #[test]
    fn test_parking_lot_rw_lock_read_write_and_try_success() {
        let rw_lock = ParkingLotRwLock::new(0);

        assert_eq!(Lock::read(&rw_lock, read_i32), 0);
        assert_eq!(Lock::write(&rw_lock, increment_i32), 1);
        assert_eq!(Lock::try_read(&rw_lock, read_i32), Ok(1));
        assert_eq!(Lock::try_write(&rw_lock, increment_i32), Ok(2));
    }

    #[test]
    fn test_parking_lot_rw_lock_try_read_returns_would_block_when_write_locked() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let lock_clone = rw_lock.clone();
        let holder = thread::spawn(move || {
            Lock::write(&*lock_clone, |_| {
                locked_tx
                    .send(())
                    .expect("test should observe held write lock");
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held write lock");
            });
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");
        assert_eq!(
            Lock::try_read(&*rw_lock, read_i32),
            Err(TryLockError::WouldBlock),
        );
        assert_eq!(
            Lock::try_write(&*rw_lock, increment_i32),
            Err(TryLockError::WouldBlock),
        );

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        holder.join().expect("holder should not panic");
    }

    #[test]
    fn test_parking_lot_rw_lock_try_write_returns_would_block_when_read_locked() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let lock_clone = rw_lock.clone();
        let holder = thread::spawn(move || {
            Lock::read(&*lock_clone, |_| {
                locked_tx
                    .send(())
                    .expect("test should observe held read lock");
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held read lock");
            });
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");
        assert_eq!(Lock::try_read(&*rw_lock, read_i32), Ok(0));
        assert_eq!(
            Lock::try_write(&*rw_lock, increment_i32),
            Err(TryLockError::WouldBlock),
        );

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        holder.join().expect("holder should not panic");
    }

    #[test]
    fn test_parking_lot_rw_lock_remains_usable_after_panic() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(0));
        let lock_clone = rw_lock.clone();
        let handle = thread::spawn(move || {
            Lock::write(&*lock_clone, |value| {
                *value += 1;
                panic!("intentional panic while holding the lock");
            });
        });

        let _ = handle.join();

        assert_eq!(Lock::read(&*rw_lock, read_i32), 1);
        assert_eq!(Lock::try_write(&*rw_lock, increment_i32), Ok(2));
    }
}
