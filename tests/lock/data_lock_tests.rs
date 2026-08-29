// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # DataLock Trait Tests
//!
//! Tests for the DataLock trait and its implementations.

use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
#[cfg(feature = "parking-lot")]
use std::sync::RwLock;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[cfg(feature = "parking-lot")]
use parking_lot::RwLock as ParkingLotRwLock;
use qubit_lock::DataLock;
use qubit_lock::TryLockError;

fn read_i32(value: &i32) -> i32 {
    *value
}

fn increment_i32(value: &mut i32) -> i32 {
    *value += 1;
    *value
}

/// Increments a value through a generic synchronous lock implementation.
fn increment_through_lock<L>(lock: &L) -> i32
where
    L: DataLock<i32>,
{
    lock.with_write(increment_i32)
}

mod data_lock_trait_tests {
    use super::Arc;
    use super::Barrier;
    use super::DataLock;
    use super::Duration;
    use super::Mutex;
    use super::TryLockError;
    use super::increment_i32;
    use super::increment_through_lock;
    use super::mpsc;
    use super::read_i32;
    use super::thread;

    #[test]
    fn test_lock_trait_accepts_arc_wrapped_implementation() {
        let mutex = Arc::new(Mutex::new(0));

        assert_eq!(increment_through_lock(&mutex), 1);
        assert_eq!(mutex.with_read(read_i32), 1);
        assert_eq!(mutex.try_with_write(increment_i32), Ok(2));
        assert_eq!(mutex.try_with_read(read_i32), Ok(2));
    }

    #[test]
    fn test_data_lock_accepts_borrowed_forwarding() {
        let mutex = Mutex::new(0);
        let borrowed = &mutex;

        assert_eq!(DataLock::with_read(&borrowed, read_i32), 0);
        assert_eq!(DataLock::with_write(&borrowed, increment_i32), 1);
        assert_eq!(DataLock::try_with_read(&borrowed, read_i32), Ok(1));
        assert_eq!(DataLock::try_with_write(&borrowed, increment_i32), Ok(2),);
    }

    #[test]
    fn test_mutex_read_write_basic_operations() {
        let mutex = Mutex::new(0);

        // Test basic lock and modify
        let result = mutex.with_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);

        // Verify the value was persisted
        let result = mutex.with_read(|value| *value);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_mutex_read_returns_closure_result() {
        let mutex = Mutex::new(vec![1, 2, 3]);

        let length = mutex.with_read(|v| v.len());
        assert_eq!(length, 3);

        let sum = mutex.with_read(|v| v.iter().sum::<i32>());
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_try_lock_error_display_messages() {
        assert_eq!(TryLockError::WouldBlock.to_string(), "lock acquisition would block",);
        assert_eq!(TryLockError::Poisoned.to_string(), "lock is poisoned");
    }

    #[test]
    fn test_try_lock_error_implements_std_error() {
        fn assert_std_error<E: std::error::Error>() {}

        assert_std_error::<TryLockError>();
    }

    #[test]
    fn test_mutex_try_read_write_success() {
        let mutex = Mutex::new(42);

        // Should successfully acquire the lock
        let result = mutex.try_with_read(|value| *value);
        assert_eq!(result, Ok(42));

        // Should be able to modify
        let result = mutex.try_with_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_mutex_try_read_returns_would_block_when_locked() {
        let mutex = Arc::new(Mutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            mutex_clone.with_write(|value| {
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
        let result = mutex.try_with_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("holder thread should not panic");

        // Now should be able to successfully acquire the lock
        let result = mutex.try_with_read(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_mutex_concurrent_access() {
        let mutex = Arc::new(Mutex::new(0));
        let mut handles = vec![];

        // Create multiple threads accessing the lock concurrently
        for _ in 0..10 {
            let mutex = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                mutex.with_write(|value| {
                    *value += 1;
                });
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("worker thread should not panic");
        }

        // Verify final value
        let result = mutex.with_read(|value| *value);
        assert_eq!(result, 10);
    }

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_mutex_read_panics_on_poisoned() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Poison the lock by panicking while holding it
        let handle = thread::spawn(move || {
            mutex_clone.with_write(|value| {
                *value += 1;
                barrier_clone.wait();
                panic!("intentional panic to poison the lock");
            });
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Wait for child thread to panic
        let _ = handle.join();

        // Try to acquire poisoned lock, should panic
        mutex.with_read(|_| {});
    }

    #[test]
    fn test_mutex_try_read_returns_poisoned_on_poisoned() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Poison the lock by panicking while holding it
        let handle = thread::spawn(move || {
            mutex_clone.with_write(|value| {
                *value += 1;
                barrier_clone.wait();
                panic!("intentional panic to poison the lock");
            });
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Wait for child thread to panic
        let _ = handle.join();

        // Try to acquire poisoned lock, should return Poisoned
        let result = mutex.try_with_read(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_mutex_read_write_complex_types() {
        let mutex = Mutex::new(String::from("Hello"));

        mutex.with_write(|s| {
            s.push_str(" World");
        });

        let result = mutex.with_read(|s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_mutex_nested_operations() {
        let mutex = Mutex::new(vec![1, 2, 3]);

        let result = mutex.with_write(|v| {
            v.push(4);
            v.push(5);
            v.iter().map(|&x| x * 2).collect::<Vec<_>>()
        });

        assert_eq!(result, vec![2, 4, 6, 8, 10]);

        // Verify original was modified
        let original = mutex.with_read(|v| v.clone());
        assert_eq!(original, vec![1, 2, 3, 4, 5]);
    }

    // Tests for std::sync::Mutex trait implementation
    #[test]
    fn test_std_mutex_read() {
        let mutex = Mutex::new(42);
        let result = DataLock::with_read(&mutex, |value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_std_mutex_write() {
        let mutex = Mutex::new(0);
        let result = DataLock::with_write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_std_mutex_try_read_success() {
        let mutex = Mutex::new(42);
        let result = DataLock::try_with_read(&mutex, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_std_mutex_try_write_success() {
        let mutex = Mutex::new(42);
        let result = DataLock::try_with_write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_std_mutex_try_read_returns_would_block_when_locked_short_path() {
        let mutex = Arc::new(Mutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().expect("holder mutex should not be poisoned");
            locked_tx.send(()).expect("test should observe held mutex");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held mutex");
        });

        // Wait for child thread to acquire the lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        // Try to acquire read lock, should return WouldBlock since it's held by
        // another thread
        let result = DataLock::try_with_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("holder thread should not panic");

        // Now should be able to successfully acquire the lock
        let result = DataLock::try_with_read(&*mutex, |value| *value);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_std_mutex_try_write_returns_would_block_when_locked() {
        let mutex = Arc::new(Mutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().expect("holder mutex should not be poisoned");
            locked_tx.send(()).expect("test should observe held mutex");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held mutex");
        });

        // Wait for child thread to acquire the lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        // Try to acquire write lock, should return WouldBlock since it's held
        // by another thread
        let result = DataLock::try_with_write(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("holder thread should not panic");

        // Now should be able to successfully acquire the lock
        let result = DataLock::try_with_write(&*mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_std_mutex_try_read_returns_would_block_when_locked() {
        let mutex = Arc::new(Mutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().expect("holder mutex should not be poisoned");
            locked_tx.send(()).expect("test should observe held mutex");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held mutex");
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");
        let result = DataLock::try_with_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        handle.join().expect("holder thread should not panic");
    }

    #[test]
    fn test_std_mutex_try_read_returns_poisoned_when_poisoned() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let mut guard = mutex_clone
                .lock()
                .expect("mutex should be unpoisoned before intentional poisoning");
            *guard += 1;
            barrier_clone.wait();
            panic!("intentional panic to poison the lock");
        });

        barrier.wait();
        let _ = handle.join();

        let result = DataLock::try_with_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_mutex_try_write_returns_poisoned_when_poisoned() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let mut guard = mutex_clone
                .lock()
                .expect("mutex should be unpoisoned before intentional poisoning");
            *guard += 1;
            barrier_clone.wait();
            panic!("intentional panic to poison the lock");
        });

        barrier.wait();
        let _ = handle.join();

        let result = DataLock::try_with_write(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_mutex_try_methods_cover_shared_function_pointer_paths() {
        let mutex = Arc::new(Mutex::new(0));

        assert_eq!(DataLock::try_with_read(&*mutex, read_i32), Ok(0));
        assert_eq!(DataLock::try_with_write(&*mutex, increment_i32), Ok(1));

        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mutex_clone = mutex.clone();
        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().expect("holder mutex should not be poisoned");
            locked_tx.send(()).expect("test should observe held mutex");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held mutex");
        });
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");
        assert_eq!(
            DataLock::try_with_read(&*mutex, read_i32),
            Err(TryLockError::WouldBlock)
        );
        assert_eq!(
            DataLock::try_with_write(&*mutex, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        handle.join().expect("holder thread should not panic");

        let poisoned = Arc::new(Mutex::new(0));
        let poisoned_clone = poisoned.clone();
        let handle = thread::spawn(move || {
            let mut guard = poisoned_clone
                .lock()
                .expect("mutex should be unpoisoned before intentional poisoning");
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });
        let _ = handle.join();

        assert_eq!(
            DataLock::try_with_read(&*poisoned, read_i32),
            Err(TryLockError::Poisoned)
        );
        assert_eq!(
            DataLock::try_with_write(&*poisoned, increment_i32),
            Err(TryLockError::Poisoned),
        );
    }
}

#[cfg(feature = "parking-lot")]
mod rwlock_trait_tests {
    use super::Arc;
    use super::DataLock;
    use super::Duration;
    use super::ParkingLotRwLock;
    use super::RwLock;
    use super::TryLockError;
    use super::increment_i32;
    use super::mpsc;
    use super::read_i32;
    use super::thread;

    #[test]
    fn test_rwlock_read_basic() {
        let rw_lock = ParkingLotRwLock::new(42);

        let result = rw_lock.with_read(|value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_rwlock_write_basic() {
        let rw_lock = ParkingLotRwLock::new(0);

        let result = rw_lock.with_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);

        // Verify the value was persisted
        let result = rw_lock.with_read(|value| *value);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_rwlock_concurrent_readers() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(vec![1, 2, 3, 4, 5]));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rw_lock_clone = Arc::clone(&rw_lock);
        let holder = thread::spawn(move || {
            let sum = rw_lock_clone.with_read(|data| {
                locked_tx.send(()).expect("test should observe held read lock");
                let sum = data.iter().sum::<i32>();
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held read lock");
                sum
            });
            assert_eq!(sum, 15);
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");

        let concurrent_sum = rw_lock.try_with_read(|data| data.iter().sum::<i32>());
        assert_eq!(concurrent_sum, Ok(15));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        holder.join().expect("holder thread should not panic");
    }

    #[test]
    fn test_rwlock_write_lock_is_exclusive() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(0));
        let mut handles = vec![];

        // Create multiple writer threads
        for _ in 0..10 {
            let rw_lock = Arc::clone(&rw_lock);
            let handle = thread::spawn(move || {
                rw_lock.with_write(|value| {
                    *value += 1;
                });
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("worker thread should not panic");
        }

        // Verify final value (should be 10 if writes are exclusive)
        let result = rw_lock.with_read(|value| *value);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_rwlock_read_after_write() {
        let rw_lock = ParkingLotRwLock::new(String::from("Hello"));

        // Write operation
        rw_lock.with_write(|s| {
            s.push_str(" World");
        });

        // Read operation should see the change
        let result = rw_lock.with_read(|s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_rwlock_with_complex_types() {
        let rw_lock = ParkingLotRwLock::new(vec![1, 2, 3]);

        // Multiple readers can access concurrently
        let len = rw_lock.with_read(|v| v.len());
        assert_eq!(len, 3);

        // Writer modifies the data
        rw_lock.with_write(|v| {
            v.push(4);
            v.push(5);
        });

        // Reader sees the updated data
        let sum = rw_lock.with_read(|v| v.iter().sum::<i32>());
        assert_eq!(sum, 15);
    }

    #[test]
    fn test_rwlock_read_lock_returns_closure_result() {
        let rw_lock = ParkingLotRwLock::new(vec![10, 20, 30]);

        let result = rw_lock.with_read(|v| v.iter().map(|&x| x * 2).collect::<Vec<_>>());

        assert_eq!(result, vec![20, 40, 60]);

        // Original should be unchanged
        let original = rw_lock.with_read(|v| v.clone());
        assert_eq!(original, vec![10, 20, 30]);
    }

    #[test]
    fn test_rwlock_write_lock_returns_closure_result() {
        let rw_lock = ParkingLotRwLock::new(5);

        let result = rw_lock.with_write(|value| {
            *value *= 2;
            *value
        });

        assert_eq!(result, 10);

        // Verify the value was actually modified
        let current = rw_lock.with_read(|value| *value);
        assert_eq!(current, 10);
    }

    #[test]
    fn test_rwlock_try_read_success() {
        let rw_lock = ParkingLotRwLock::new(42);

        // Should successfully acquire the read lock
        let result = rw_lock.try_with_read(|value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_rwlock_try_write_success() {
        let rw_lock = ParkingLotRwLock::new(42);

        // Should successfully acquire the write lock
        let result = rw_lock.try_with_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_rwlock_try_read_returns_would_block_when_write_locked() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rw_lock_clone = rw_lock.clone();

        // Hold the write lock in another thread
        let handle = thread::spawn(move || {
            rw_lock_clone.with_write(|value| {
                *value += 1;
                locked_tx.send(()).expect("test should observe held write lock");
                release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held write lock");
            });
        });

        // Wait for child thread to acquire the write lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");

        // Try to acquire read lock while write lock is held by another thread
        let result = rw_lock.try_with_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("holder thread should not panic");

        // Now should be able to successfully acquire the read lock
        let result = rw_lock.try_with_read(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_rwlock_try_write_succeeds_after_read_guard_released() {
        let rw_lock = ParkingLotRwLock::new(0);

        // First acquire read lock to ensure it's locked
        let result = rw_lock.try_with_read(|value| *value);
        assert_eq!(result, Ok(0)); // Should succeed initially

        // Now try to acquire write lock while read lock was held (but now
        // released)
        let result = rw_lock.try_with_write(|value| *value);
        assert_eq!(result, Ok(0)); // Should succeed since lock was released
    }

    #[test]
    fn test_rwlock_mixed_read_write_operations() {
        let rw_lock = Arc::new(ParkingLotRwLock::new(0));
        let mut handles = vec![];

        // Create some readers
        for _ in 0..5 {
            let rw_lock = Arc::clone(&rw_lock);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    rw_lock.with_read(|value| {
                        let _ = *value;
                    });
                }
            });
            handles.push(handle);
        }

        // Create some writers
        for _ in 0..5 {
            let rw_lock = Arc::clone(&rw_lock);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    rw_lock.with_write(|value| {
                        *value += 1;
                    });
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().expect("worker thread should not panic");
        }

        // Verify final value
        let result = rw_lock.with_read(|value| *value);
        assert_eq!(result, 50); // 5 writers × 10 increments each
    }

    // Tests for std::sync::RwLock trait implementation
    #[test]
    fn test_std_rwlock_read() {
        let rwlock = RwLock::new(42);
        let result = DataLock::with_read(&rwlock, |value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_std_rwlock_write() {
        let rwlock = RwLock::new(0);
        let result = DataLock::with_write(&rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_std_rwlock_try_read_success() {
        let rwlock = RwLock::new(42);
        let result = DataLock::try_with_read(&rwlock, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_std_rwlock_try_write_success() {
        let rwlock = RwLock::new(42);
        let result = DataLock::try_with_write(&rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_std_rwlock_try_read_returns_would_block_when_write_locked() {
        let rwlock = Arc::new(RwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rwlock_clone = rwlock.clone();

        // Hold the write lock in another thread
        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.write().expect("write-holder lock should not be poisoned");
            locked_tx.send(()).expect("test should observe held write lock");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held write lock");
        });

        // Wait for child thread to acquire the write lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");

        // Try to acquire read lock, should return WouldBlock since write lock
        // is held by another thread
        let result = DataLock::try_with_read(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("write-holder thread should not panic");

        // Now should be able to successfully acquire the read lock
        let result = DataLock::try_with_read(&*rwlock, |value| *value);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_would_block_when_read_locked_short_path() {
        let rwlock = Arc::new(RwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rwlock_clone = rwlock.clone();

        // Hold the read lock in another thread
        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.read().expect("read-holder lock should not be poisoned");
            locked_tx.send(()).expect("test should observe held read lock");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held read lock");
        });

        // Wait for child thread to acquire the read lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");

        // Try to acquire write lock, should return WouldBlock since read lock
        // is held by another thread
        let result = DataLock::try_with_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("read-holder thread should not panic");

        // Now should be able to successfully acquire the write lock
        let result = DataLock::try_with_write(&*rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_would_block_when_write_locked() {
        let rwlock = Arc::new(RwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rwlock_clone = rwlock.clone();

        // Hold the write lock in another thread
        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.write().expect("write-holder lock should not be poisoned");
            locked_tx.send(()).expect("test should observe held write lock");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held write lock");
        });

        // Wait for child thread to acquire the write lock
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");

        // Try to acquire write lock, should return WouldBlock since write lock
        // is held by another thread
        let result = DataLock::try_with_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("write-holder thread should not panic");

        // Now should be able to successfully acquire the write lock
        let result = DataLock::try_with_write(&*rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_would_block_when_read_locked() {
        let rwlock = Arc::new(RwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rwlock_clone = rwlock.clone();

        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.read().expect("read-holder lock should not be poisoned");
            locked_tx.send(()).expect("test should observe held read lock");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held read lock");
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");
        let result = DataLock::try_with_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        handle.join().expect("read-holder thread should not panic");
    }

    #[test]
    fn test_std_rwlock_try_read_returns_poisoned_when_poisoned() {
        let rwlock = Arc::new(RwLock::new(0));

        let rwlock_clone = rwlock.clone();
        let handle = thread::spawn(move || {
            let mut guard = rwlock_clone
                .write()
                .expect("rwlock should be unpoisoned before intentional poisoning");
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });

        let _ = handle.join();

        let result = DataLock::try_with_read(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_poisoned_when_poisoned() {
        let rwlock = Arc::new(RwLock::new(0));

        let rwlock_clone = rwlock.clone();
        let handle = thread::spawn(move || {
            let mut guard = rwlock_clone
                .write()
                .expect("rwlock should be unpoisoned before intentional poisoning");
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });

        let _ = handle.join();

        let result = DataLock::try_with_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_rwlock_try_methods_cover_shared_function_pointer_paths() {
        let rwlock = Arc::new(RwLock::new(0));

        assert_eq!(DataLock::try_with_read(&*rwlock, read_i32), Ok(0));
        assert_eq!(DataLock::try_with_write(&*rwlock, increment_i32), Ok(1));

        let (read_locked_tx, read_locked_rx) = mpsc::channel();
        let (read_release_tx, read_release_rx) = mpsc::channel();
        let read_lock = rwlock.clone();
        let read_holder = thread::spawn(move || {
            let _guard = read_lock.write().expect("write-holder lock should not be poisoned");
            read_locked_tx.send(()).expect("test should observe held write lock");
            read_release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held write lock");
        });
        read_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");
        assert_eq!(
            DataLock::try_with_read(&*rwlock, read_i32),
            Err(TryLockError::WouldBlock)
        );
        read_release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        read_holder
            .join()
            .expect("read-path write-holder thread should not panic");

        let (write_locked_tx, write_locked_rx) = mpsc::channel();
        let (write_release_tx, write_release_rx) = mpsc::channel();
        let write_lock = rwlock.clone();
        let write_holder = thread::spawn(move || {
            let _guard = write_lock.read().expect("read-holder lock should not be poisoned");
            write_locked_tx.send(()).expect("test should observe held read lock");
            write_release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("test should release held read lock");
        });
        write_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");
        assert_eq!(
            DataLock::try_with_write(&*rwlock, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        write_release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        write_holder
            .join()
            .expect("write-path read-holder thread should not panic");

        let poisoned = Arc::new(RwLock::new(0));
        let poisoned_clone = poisoned.clone();
        let handle = thread::spawn(move || {
            let mut guard = poisoned_clone
                .write()
                .expect("rwlock should be unpoisoned before intentional poisoning");
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });
        let _ = handle.join();

        assert_eq!(
            DataLock::try_with_read(&*poisoned, read_i32),
            Err(TryLockError::Poisoned)
        );
        assert_eq!(
            DataLock::try_with_write(&*poisoned, increment_i32),
            Err(TryLockError::Poisoned),
        );
    }
}
