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
//! # Parking Lot Mutex Tests
//!
//! Tests for the parking_lot::Mutex implementation of the Lock trait

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use parking_lot::Mutex as ParkingLotMutex;
use qubit_lock::{
    Lock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod parking_lot_mutex_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    #[test]
    fn test_parking_lot_mutex_read_basic() {
        let mutex = ParkingLotMutex::new(42);
        let result = Lock::read(&mutex, |value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_parking_lot_mutex_write_basic() {
        let mutex = ParkingLotMutex::new(0);
        let result = Lock::write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_parking_lot_mutex_read_returns_closure_result() {
        let mutex = ParkingLotMutex::new(vec![1, 2, 3]);

        let length = Lock::read(&mutex, |v| v.len());
        assert_eq!(length, 3);

        let sum = Lock::read(&mutex, |v| v.iter().sum::<i32>());
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_parking_lot_mutex_write_returns_closure_result() {
        let mutex = ParkingLotMutex::new(vec![1, 2, 3]);

        let result = Lock::write(&mutex, |v| {
            v.push(4);
            v.push(5);
            v.iter().map(|&x| x * 2).collect::<Vec<_>>()
        });

        assert_eq!(result, vec![2, 4, 6, 8, 10]);

        // Verify original was modified
        let original = Lock::read(&mutex, |v| v.clone());
        assert_eq!(original, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parking_lot_mutex_try_read_success() {
        let mutex = ParkingLotMutex::new(42);
        let result = Lock::try_read(&mutex, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_parking_lot_mutex_try_write_success() {
        let mutex = ParkingLotMutex::new(42);
        let result = Lock::try_write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_parking_lot_mutex_try_read_returns_would_block_when_locked() {
        let mutex = Arc::new(ParkingLotMutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            Lock::write(&*mutex_clone, |value| {
                *value += 1;
                locked_tx.send(()).expect("test should observe held mutex");
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held mutex");
            });
        });

        // Wait for child thread to acquire the lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        // Try to acquire lock, should return WouldBlock
        let result = Lock::try_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = Lock::try_read(&*mutex, |value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_parking_lot_mutex_try_write_returns_would_block_when_locked() {
        let mutex = Arc::new(ParkingLotMutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            Lock::write(&*mutex_clone, |value| {
                *value += 1;
                locked_tx.send(()).expect("test should observe held mutex");
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held mutex");
            });
        });

        // Wait for child thread to acquire the lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        // Try to acquire write lock, should return WouldBlock
        let result = Lock::try_write(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = Lock::try_write(&*mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(2));
    }

    #[test]
    fn test_parking_lot_mutex_try_methods_cover_shared_function_pointer_paths() {
        let mutex = Arc::new(ParkingLotMutex::new(0));

        assert_eq!(Lock::try_read(&*mutex, read_i32), Ok(0));
        assert_eq!(Lock::try_write(&*mutex, increment_i32), Ok(1));

        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mutex_clone = mutex.clone();
        let handle = thread::spawn(move || {
            Lock::write(&*mutex_clone, |_| {
                locked_tx.send(()).expect("test should observe held mutex");
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held mutex");
            });
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");
        assert_eq!(
            Lock::try_read(&*mutex, read_i32),
            Err(TryLockError::WouldBlock)
        );
        assert_eq!(
            Lock::try_write(&*mutex, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        handle.join().unwrap();
    }

    #[test]
    fn test_parking_lot_mutex_concurrent_access() {
        let mutex = Arc::new(ParkingLotMutex::new(0));
        let mut handles = vec![];

        // Create multiple threads accessing the lock concurrently
        for _ in 0..10 {
            let mutex = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                Lock::write(&*mutex, |value| {
                    *value += 1;
                });
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final value
        let result = Lock::read(&*mutex, |value| *value);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_parking_lot_mutex_with_complex_types() {
        let mutex = ParkingLotMutex::new(String::from("Hello"));

        Lock::write(&mutex, |s| {
            s.push_str(" World");
        });

        let result = Lock::read(&mutex, |s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_parking_lot_mutex_nested_operations() {
        let mutex = ParkingLotMutex::new(vec![1, 2, 3]);

        let result = Lock::write(&mutex, |v| {
            v.push(4);
            v.push(5);
            v.iter().map(|&x| x * 2).collect::<Vec<_>>()
        });

        assert_eq!(result, vec![2, 4, 6, 8, 10]);

        // Verify original was modified
        let original = Lock::read(&mutex, |v| v.clone());
        assert_eq!(original, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parking_lot_mutex_performance_comparison() {
        // This test demonstrates that parking_lot mutex works correctly
        // In a real performance test, we'd compare timing, but here we just
        // ensure correctness under concurrent load
        let mutex = Arc::new(ParkingLotMutex::new(0));
        let mut handles = vec![];

        // Create many threads doing quick operations
        for _ in 0..50 {
            let mutex = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    Lock::write(&*mutex, |value| {
                        *value += 1;
                    });
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify correct result
        let result = Lock::read(&*mutex, |value| *value);
        assert_eq!(result, 5000); // 50 threads × 100 increments each
    }
}
