/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Lock Trait Tests
//!
//! Tests for the Lock trait and its implementations for std::sync::Mutex and std::sync::RwLock

use std::{
    sync::{
        Arc,
        Barrier,
    },
    thread,
};

use std::sync::{
    Mutex,
    RwLock,
};

use qubit_lock::lock::{
    ArcRwLock,
    ArcStdMutex,
    Lock,
    TryLockError,
};

fn read_i32(value: &i32) -> i32 {
    *value
}

fn increment_i32(value: &mut i32) -> i32 {
    *value += 1;
    *value
}

#[cfg(test)]
mod lock_trait_tests {
    use super::*;

    #[test]
    fn test_mutex_with_lock_basic_operations() {
        let mutex = ArcStdMutex::new(0);

        // Test basic lock and modify
        let result = mutex.write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);

        // Verify the value was persisted
        let result = mutex.read(|value| *value);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_mutex_with_lock_returns_closure_result() {
        let mutex = ArcStdMutex::new(vec![1, 2, 3]);

        let length = mutex.read(|v| v.len());
        assert_eq!(length, 3);

        let sum = mutex.read(|v| v.iter().sum::<i32>());
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_try_lock_error_display_messages() {
        assert_eq!(
            TryLockError::WouldBlock.to_string(),
            "lock acquisition would block",
        );
        assert_eq!(TryLockError::Poisoned.to_string(), "lock is poisoned");
    }

    #[test]
    fn test_try_lock_error_implements_std_error() {
        fn assert_std_error<E: std::error::Error>() {}

        assert_std_error::<TryLockError>();
    }

    #[test]
    fn test_mutex_try_with_lock_success() {
        let mutex = ArcStdMutex::new(42);

        // Should successfully acquire the lock
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Ok(42));

        // Should be able to modify
        let result = mutex.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_mutex_try_with_lock_returns_would_block_when_locked() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                // Notify main thread
                barrier_clone.wait();
                // Hold the lock for some time
                thread::sleep(std::time::Duration::from_millis(100));
            });
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Try to acquire lock, should return WouldBlock
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_mutex_concurrent_access() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let mut handles = vec![];

        // Create multiple threads accessing the lock concurrently
        for _ in 0..10 {
            let mutex = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                mutex.write(|value| {
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
        let result = mutex.read(|value| *value);
        assert_eq!(result, 10);
    }

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_mutex_with_lock_panics_on_poisoned() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Poison the lock by panicking while holding it
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
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
        mutex.read(|_| {});
    }

    #[test]
    fn test_mutex_try_with_lock_returns_poisoned_on_poisoned() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Poison the lock by panicking while holding it
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
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
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_mutex_with_lock_complex_types() {
        let mutex = ArcStdMutex::new(String::from("Hello"));

        mutex.write(|s| {
            s.push_str(" World");
        });

        let result = mutex.read(|s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_mutex_nested_operations() {
        let mutex = ArcStdMutex::new(vec![1, 2, 3]);

        let result = mutex.write(|v| {
            v.push(4);
            v.push(5);
            v.iter().map(|&x| x * 2).collect::<Vec<_>>()
        });

        assert_eq!(result, vec![2, 4, 6, 8, 10]);

        // Verify original was modified
        let original = mutex.read(|v| v.clone());
        assert_eq!(original, vec![1, 2, 3, 4, 5]);
    }

    // Tests for std::sync::Mutex trait implementation
    #[test]
    fn test_std_mutex_read() {
        let mutex = Mutex::new(42);
        let result = Lock::read(&mutex, |value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_std_mutex_write() {
        let mutex = Mutex::new(0);
        let result = Lock::write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_std_mutex_try_read_success() {
        let mutex = Mutex::new(42);
        let result = Lock::try_read(&mutex, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_std_mutex_try_write_success() {
        let mutex = Mutex::new(42);
        let result = Lock::try_write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_std_mutex_try_read_returns_would_block_when_locked_short_path() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().unwrap();
            // Notify main thread that we have the lock
            barrier_clone.wait();
            // Hold the lock for some time
            thread::sleep(std::time::Duration::from_millis(100));
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Try to acquire read lock, should return WouldBlock since it's held by another thread
        let result = Lock::try_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = Lock::try_read(&*mutex, |value| *value);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_std_mutex_try_write_returns_would_block_when_locked() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().unwrap();
            // Notify main thread that we have the lock
            barrier_clone.wait();
            // Hold the lock for some time
            thread::sleep(std::time::Duration::from_millis(100));
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Try to acquire write lock, should return WouldBlock since it's held by another thread
        let result = Lock::try_write(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = Lock::try_write(&*mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_std_mutex_try_read_returns_would_block_when_locked() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().unwrap();
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(100));
        });

        barrier.wait();
        let result = Lock::try_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        handle.join().unwrap();
    }

    #[test]
    fn test_std_mutex_try_read_returns_poisoned_when_poisoned() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let mut guard = mutex_clone.lock().unwrap();
            *guard += 1;
            barrier_clone.wait();
            panic!("intentional panic to poison the lock");
        });

        barrier.wait();
        let _ = handle.join();

        let result = Lock::try_read(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_mutex_try_write_returns_poisoned_when_poisoned() {
        let mutex = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let mut guard = mutex_clone.lock().unwrap();
            *guard += 1;
            barrier_clone.wait();
            panic!("intentional panic to poison the lock");
        });

        barrier.wait();
        let _ = handle.join();

        let result = Lock::try_write(&*mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_mutex_try_methods_cover_shared_function_pointer_paths() {
        let mutex = Arc::new(Mutex::new(0));

        assert_eq!(Lock::try_read(&*mutex, read_i32), Ok(0));
        assert_eq!(Lock::try_write(&*mutex, increment_i32), Ok(1));

        let barrier = Arc::new(Barrier::new(2));
        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();
        let handle = thread::spawn(move || {
            let _guard = mutex_clone.lock().unwrap();
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(50));
        });
        barrier.wait();
        assert_eq!(
            Lock::try_read(&*mutex, read_i32),
            Err(TryLockError::WouldBlock)
        );
        assert_eq!(
            Lock::try_write(&*mutex, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        handle.join().unwrap();

        let poisoned = Arc::new(Mutex::new(0));
        let poisoned_clone = poisoned.clone();
        let handle = thread::spawn(move || {
            let mut guard = poisoned_clone.lock().unwrap();
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });
        let _ = handle.join();

        assert_eq!(
            Lock::try_read(&*poisoned, read_i32),
            Err(TryLockError::Poisoned)
        );
        assert_eq!(
            Lock::try_write(&*poisoned, increment_i32),
            Err(TryLockError::Poisoned),
        );
    }
}

#[cfg(test)]
mod rwlock_trait_tests {
    use super::*;

    #[test]
    fn test_rwlock_read_basic() {
        let rw_lock = ArcRwLock::new(42);

        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_rwlock_write_basic() {
        let rw_lock = ArcRwLock::new(0);

        let result = rw_lock.write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);

        // Verify the value was persisted
        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_rwlock_concurrent_readers() {
        let rw_lock = Arc::new(ArcRwLock::new(vec![1, 2, 3, 4, 5]));
        let mut handles = vec![];

        // Create multiple reader threads
        for _ in 0..10 {
            let rw_lock = Arc::clone(&rw_lock);
            let handle = thread::spawn(move || {
                rw_lock.read(|data| {
                    // Simulate some read operation
                    thread::sleep(std::time::Duration::from_millis(10));
                    data.iter().sum::<i32>()
                })
            });
            handles.push(handle);
        }

        // All readers should get the same result
        for handle in handles {
            let sum = handle.join().unwrap();
            assert_eq!(sum, 15);
        }
    }

    #[test]
    fn test_rwlock_write_lock_is_exclusive() {
        let rw_lock = Arc::new(ArcRwLock::new(0));
        let mut handles = vec![];

        // Create multiple writer threads
        for _ in 0..10 {
            let rw_lock = Arc::clone(&rw_lock);
            let handle = thread::spawn(move || {
                rw_lock.write(|value| {
                    *value += 1;
                });
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final value (should be 10 if writes are exclusive)
        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_rwlock_read_after_write() {
        let rw_lock = ArcRwLock::new(String::from("Hello"));

        // Write operation
        rw_lock.write(|s| {
            s.push_str(" World");
        });

        // Read operation should see the change
        let result = rw_lock.read(|s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_rwlock_with_complex_types() {
        let rw_lock = ArcRwLock::new(vec![1, 2, 3]);

        // Multiple readers can access concurrently
        let len = rw_lock.read(|v| v.len());
        assert_eq!(len, 3);

        // Writer modifies the data
        rw_lock.write(|v| {
            v.push(4);
            v.push(5);
        });

        // Reader sees the updated data
        let sum = rw_lock.read(|v| v.iter().sum::<i32>());
        assert_eq!(sum, 15);
    }

    #[test]
    fn test_rwlock_read_lock_returns_closure_result() {
        let rw_lock = ArcRwLock::new(vec![10, 20, 30]);

        let result = rw_lock.read(|v| v.iter().map(|&x| x * 2).collect::<Vec<_>>());

        assert_eq!(result, vec![20, 40, 60]);

        // Original should be unchanged
        let original = rw_lock.read(|v| v.clone());
        assert_eq!(original, vec![10, 20, 30]);
    }

    #[test]
    fn test_rwlock_write_lock_returns_closure_result() {
        let rw_lock = ArcRwLock::new(5);

        let result = rw_lock.write(|value| {
            *value *= 2;
            *value
        });

        assert_eq!(result, 10);

        // Verify the value was actually modified
        let current = rw_lock.read(|value| *value);
        assert_eq!(current, 10);
    }

    #[test]
    fn test_rwlock_try_read_success() {
        let rw_lock = ArcRwLock::new(42);

        // Should successfully acquire the read lock
        let result = rw_lock.try_read(|value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_rwlock_try_write_success() {
        let rw_lock = ArcRwLock::new(42);

        // Should successfully acquire the write lock
        let result = rw_lock.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_rwlock_try_read_returns_would_block_when_write_locked() {
        let rw_lock = Arc::new(ArcRwLock::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let rw_lock_clone = rw_lock.clone();
        let barrier_clone = barrier.clone();

        // Hold the write lock in another thread
        let handle = thread::spawn(move || {
            rw_lock_clone.write(|value| {
                *value += 1;
                // Notify main thread
                barrier_clone.wait();
                // Hold the write lock for some time
                thread::sleep(std::time::Duration::from_millis(100));
            });
        });

        // Wait for child thread to acquire the write lock
        barrier.wait();

        // Try to acquire read lock while write lock is held by another thread
        let result = rw_lock.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the read lock
        let result = rw_lock.try_read(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_rwlock_try_write_succeeds_after_read_guard_released() {
        let rw_lock = ArcRwLock::new(0);

        // First acquire read lock to ensure it's locked
        let result = rw_lock.try_read(|value| *value);
        assert_eq!(result, Ok(0)); // Should succeed initially

        // Now try to acquire write lock while read lock was held (but now released)
        let result = rw_lock.try_write(|value| *value);
        assert_eq!(result, Ok(0)); // Should succeed since lock was released
    }

    #[test]
    fn test_rwlock_mixed_read_write_operations() {
        let rw_lock = Arc::new(ArcRwLock::new(0));
        let mut handles = vec![];

        // Create some readers
        for _ in 0..5 {
            let rw_lock = Arc::clone(&rw_lock);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    rw_lock.read(|value| {
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
                    rw_lock.write(|value| {
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

        // Verify final value
        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 50); // 5 writers × 10 increments each
    }

    // Tests for std::sync::RwLock trait implementation
    #[test]
    fn test_std_rwlock_read() {
        let rwlock = RwLock::new(42);
        let result = Lock::read(&rwlock, |value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_std_rwlock_write() {
        let rwlock = RwLock::new(0);
        let result = Lock::write(&rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_std_rwlock_try_read_success() {
        let rwlock = RwLock::new(42);
        let result = Lock::try_read(&rwlock, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_std_rwlock_try_write_success() {
        let rwlock = RwLock::new(42);
        let result = Lock::try_write(&rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[test]
    fn test_std_rwlock_try_read_returns_would_block_when_write_locked() {
        let rwlock = Arc::new(RwLock::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let rwlock_clone = rwlock.clone();
        let barrier_clone = barrier.clone();

        // Hold the write lock in another thread
        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.write().unwrap();
            // Notify main thread that we have the write lock
            barrier_clone.wait();
            // Hold the write lock for some time
            thread::sleep(std::time::Duration::from_millis(100));
        });

        // Wait for child thread to acquire the write lock
        barrier.wait();

        // Try to acquire read lock, should return WouldBlock since write lock is held by another thread
        let result = Lock::try_read(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the read lock
        let result = Lock::try_read(&*rwlock, |value| *value);
        assert_eq!(result, Ok(0));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_would_block_when_read_locked_short_path() {
        let rwlock = Arc::new(RwLock::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let rwlock_clone = rwlock.clone();
        let barrier_clone = barrier.clone();

        // Hold the read lock in another thread
        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.read().unwrap();
            // Notify main thread that we have the read lock
            barrier_clone.wait();
            // Hold the read lock for some time
            thread::sleep(std::time::Duration::from_millis(100));
        });

        // Wait for child thread to acquire the read lock
        barrier.wait();

        // Try to acquire write lock, should return WouldBlock since read lock is held by another thread
        let result = Lock::try_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the write lock
        let result = Lock::try_write(&*rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_would_block_when_write_locked() {
        let rwlock = Arc::new(RwLock::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let rwlock_clone = rwlock.clone();
        let barrier_clone = barrier.clone();

        // Hold the write lock in another thread
        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.write().unwrap();
            // Notify main thread that we have the write lock
            barrier_clone.wait();
            // Hold the write lock for some time
            thread::sleep(std::time::Duration::from_millis(100));
        });

        // Wait for child thread to acquire the write lock
        barrier.wait();

        // Try to acquire write lock, should return WouldBlock since write lock is held by another thread
        let result = Lock::try_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the write lock
        let result = Lock::try_write(&*rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_would_block_when_read_locked() {
        let rwlock = Arc::new(RwLock::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let rwlock_clone = rwlock.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let _guard = rwlock_clone.read().unwrap();
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(100));
        });

        barrier.wait();
        let result = Lock::try_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        handle.join().unwrap();
    }

    #[test]
    fn test_std_rwlock_try_read_returns_poisoned_when_poisoned() {
        let rwlock = Arc::new(RwLock::new(0));

        let rwlock_clone = rwlock.clone();
        let handle = thread::spawn(move || {
            let mut guard = rwlock_clone.write().unwrap();
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });

        let _ = handle.join();

        let result = Lock::try_read(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_rwlock_try_write_returns_poisoned_when_poisoned() {
        let rwlock = Arc::new(RwLock::new(0));

        let rwlock_clone = rwlock.clone();
        let handle = thread::spawn(move || {
            let mut guard = rwlock_clone.write().unwrap();
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });

        let _ = handle.join();

        let result = Lock::try_write(&*rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_std_rwlock_try_methods_cover_shared_function_pointer_paths() {
        let rwlock = Arc::new(RwLock::new(0));

        assert_eq!(Lock::try_read(&*rwlock, read_i32), Ok(0));
        assert_eq!(Lock::try_write(&*rwlock, increment_i32), Ok(1));

        let read_barrier = Arc::new(Barrier::new(2));
        let read_lock = rwlock.clone();
        let read_barrier_clone = read_barrier.clone();
        let read_holder = thread::spawn(move || {
            let _guard = read_lock.write().unwrap();
            read_barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(50));
        });
        read_barrier.wait();
        assert_eq!(
            Lock::try_read(&*rwlock, read_i32),
            Err(TryLockError::WouldBlock)
        );
        read_holder.join().unwrap();

        let write_barrier = Arc::new(Barrier::new(2));
        let write_lock = rwlock.clone();
        let write_barrier_clone = write_barrier.clone();
        let write_holder = thread::spawn(move || {
            let _guard = write_lock.read().unwrap();
            write_barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(50));
        });
        write_barrier.wait();
        assert_eq!(
            Lock::try_write(&*rwlock, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        write_holder.join().unwrap();

        let poisoned = Arc::new(RwLock::new(0));
        let poisoned_clone = poisoned.clone();
        let handle = thread::spawn(move || {
            let mut guard = poisoned_clone.write().unwrap();
            *guard += 1;
            panic!("intentional panic to poison the lock");
        });
        let _ = handle.join();

        assert_eq!(
            Lock::try_read(&*poisoned, read_i32),
            Err(TryLockError::Poisoned)
        );
        assert_eq!(
            Lock::try_write(&*poisoned, increment_i32),
            Err(TryLockError::Poisoned),
        );
    }
}
