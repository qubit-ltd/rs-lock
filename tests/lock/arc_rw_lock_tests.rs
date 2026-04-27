/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # ArcRwLock Tests
//!
//! Tests for the ArcRwLock implementation

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use qubit_lock::{
    ArcRwLock,
    Lock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod arc_rw_lock_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    #[test]
    fn test_arc_rw_lock_new() {
        let rw_lock = ArcRwLock::new(42);
        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_arc_rw_lock_read() {
        let rw_lock = ArcRwLock::new(0);

        // Test read lock
        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_arc_rw_lock_write() {
        let rw_lock = ArcRwLock::new(0);

        // Test write lock
        let result = rw_lock.write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_arc_rw_lock_clone() {
        let rw_lock = ArcRwLock::new(0);
        let rw_lock_clone = rw_lock.clone();

        // Test cloned read-write lock
        let result = rw_lock_clone.write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);

        // Verify that original lock can see changes
        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_arc_rw_lock_concurrent_readers() {
        let rw_lock = Arc::new(ArcRwLock::new(vec![1, 2, 3, 4, 5]));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rw_lock_clone = Arc::clone(&rw_lock);
        let holder = thread::spawn(move || {
            let sum = rw_lock_clone.read(|data| {
                locked_tx
                    .send(())
                    .expect("test should observe held read lock");
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

        let concurrent_sum = rw_lock.try_read(|data| data.iter().sum::<i32>());
        assert_eq!(concurrent_sum, Ok(15));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        holder.join().expect("holder thread should not panic");
    }

    #[test]
    fn test_arc_rw_lock_write_lock_is_exclusive() {
        let rw_lock = ArcRwLock::new(0);
        let rw_lock = Arc::new(rw_lock);
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
    fn test_arc_rw_lock_read_after_write() {
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
    fn test_arc_rw_lock_with_complex_types() {
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
    fn test_arc_rw_lock_read_lock_returns_closure_result() {
        let rw_lock = ArcRwLock::new(vec![10, 20, 30]);

        let result = rw_lock.read(|v| v.iter().map(|&x| x * 2).collect::<Vec<_>>());

        assert_eq!(result, vec![20, 40, 60]);

        // Original should be unchanged
        let original = rw_lock.read(|v| v.clone());
        assert_eq!(original, vec![10, 20, 30]);
    }

    #[test]
    fn test_arc_rw_lock_write_lock_returns_closure_result() {
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
    fn test_arc_rw_lock_mixed_read_write_operations() {
        let rw_lock = ArcRwLock::new(0);
        let rw_lock = Arc::new(rw_lock);
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

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_arc_rw_lock_read_panics_on_poisoned() {
        let rw_lock = ArcRwLock::new(0);
        let rw_lock = Arc::new(rw_lock);

        let rw_lock_clone = rw_lock.clone();

        // Poison the lock by panicking while holding write lock
        let handle = thread::spawn(move || {
            rw_lock_clone.write(|value| {
                *value += 1;
                panic!("intentional panic to poison the lock");
            });
        });

        // Wait for thread to panic
        let _ = handle.join();

        // Try to acquire read lock on poisoned lock, should panic
        rw_lock.read(|_| {});
    }

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_arc_rw_lock_write_panics_on_poisoned() {
        let rw_lock = ArcRwLock::new(0);
        let rw_lock = Arc::new(rw_lock);

        let rw_lock_clone = rw_lock.clone();

        // Poison the lock by panicking while holding write lock
        let handle = thread::spawn(move || {
            rw_lock_clone.write(|value| {
                *value += 1;
                panic!("intentional panic to poison the lock");
            });
        });

        // Wait for thread to panic
        let _ = handle.join();

        // Try to acquire write lock on poisoned lock, should panic
        rw_lock.write(|_| {});
    }

    #[test]
    fn test_arc_rw_lock_try_read_returns_poisoned() {
        let rw_lock = ArcRwLock::new(0);
        let rw_lock = Arc::new(rw_lock);

        let rw_lock_clone = rw_lock.clone();
        let handle = thread::spawn(move || {
            rw_lock_clone.write(|value| {
                *value += 1;
                panic!("intentional panic to poison the lock");
            });
        });

        let _ = handle.join();
        let result = rw_lock.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_arc_rw_lock_try_methods_cover_shared_function_pointer_paths() {
        let rw_lock = Arc::new(ArcRwLock::new(0));

        assert_eq!(rw_lock.try_read(read_i32), Ok(0));
        assert_eq!(rw_lock.try_write(increment_i32), Ok(1));

        let (read_locked_tx, read_locked_rx) = mpsc::channel();
        let (read_release_tx, read_release_rx) = mpsc::channel();
        let read_lock = rw_lock.clone();
        let read_holder = thread::spawn(move || {
            read_lock.write(|_| {
                read_locked_tx
                    .send(())
                    .expect("test should observe held write lock");
                read_release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held write lock");
            });
        });
        read_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");
        assert_eq!(rw_lock.try_read(read_i32), Err(TryLockError::WouldBlock));
        read_release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        read_holder.join().unwrap();

        let (write_locked_tx, write_locked_rx) = mpsc::channel();
        let (write_release_tx, write_release_rx) = mpsc::channel();
        let write_lock = rw_lock.clone();
        let write_holder = thread::spawn(move || {
            write_lock.read(|_| {
                write_locked_tx
                    .send(())
                    .expect("test should observe held read lock");
                write_release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held read lock");
            });
        });
        write_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");
        assert_eq!(
            rw_lock.try_write(increment_i32),
            Err(TryLockError::WouldBlock),
        );
        write_release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        write_holder.join().unwrap();

        let poisoned = Arc::new(ArcRwLock::new(0));
        let poisoned_clone = poisoned.clone();
        let handle = thread::spawn(move || {
            poisoned_clone.write(|value| {
                *value += 1;
                panic!("intentional panic to poison the lock");
            });
        });
        let _ = handle.join();

        assert_eq!(poisoned.try_read(read_i32), Err(TryLockError::Poisoned));
        assert_eq!(
            poisoned.try_write(increment_i32),
            Err(TryLockError::Poisoned),
        );
    }

    #[test]
    fn test_arc_rw_lock_try_write_returns_would_block() {
        let rw_lock = Arc::new(ArcRwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rw_lock_clone = rw_lock.clone();
        let handle = thread::spawn(move || {
            rw_lock_clone.read(|_| {
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
        let result = rw_lock.try_write(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        handle.join().unwrap();
    }

    #[test]
    fn test_arc_rw_lock_try_read_returns_would_block() {
        let rw_lock = Arc::new(ArcRwLock::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let rw_lock_clone = rw_lock.clone();
        let handle = thread::spawn(move || {
            rw_lock_clone.write(|_| {
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
        let result = rw_lock.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        handle.join().unwrap();
    }

    #[test]
    fn test_arc_rw_lock_try_write_returns_poisoned() {
        let rw_lock = Arc::new(ArcRwLock::new(0));

        let rw_lock_clone = rw_lock.clone();
        let handle = thread::spawn(move || {
            rw_lock_clone.write(|value| {
                *value += 1;
                panic!("intentional panic to poison the lock");
            });
        });

        let _ = handle.join();
        let result = rw_lock.try_write(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_arc_rw_lock_sharing_across_threads() {
        let rw_lock = ArcRwLock::new(0);

        let rw_lock1 = rw_lock.clone();
        let handle1 = thread::spawn(move || {
            for _ in 0..50 {
                rw_lock1.write(|value| {
                    *value += 1;
                });
            }
        });

        let rw_lock2 = rw_lock.clone();
        let handle2 = thread::spawn(move || {
            for _ in 0..50 {
                rw_lock2.write(|value| {
                    *value += 1;
                });
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        let result = rw_lock.read(|value| *value);
        assert_eq!(result, 100);
    }

    #[test]
    fn test_arc_rw_lock_nested_data_structures() {
        use std::collections::HashMap;

        let rw_lock = ArcRwLock::new(HashMap::new());

        rw_lock.write(|map| {
            map.insert("key1", 10);
            map.insert("key2", 20);
        });

        let value1 = rw_lock.read(|map| map.get("key1").copied());
        assert_eq!(value1, Some(10));

        let value2 = rw_lock.read(|map| map.get("key2").copied());
        assert_eq!(value2, Some(20));
    }
}
