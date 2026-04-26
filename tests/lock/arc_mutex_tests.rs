/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # ArcMutex Tests
//!
//! Tests for the ArcMutex implementation

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use qubit_lock::{
    ArcMutex,
    Lock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod arc_mutex_tests {
    use super::*;

    #[test]
    fn test_arc_mutex_new() {
        let mutex = ArcMutex::new(42);
        let result = mutex.read(|value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_arc_mutex_read_write_basic_operations() {
        let mutex = ArcMutex::new(0);

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
    fn test_arc_mutex_try_read_write_success() {
        let mutex = ArcMutex::new(42);

        let result = mutex.try_read(|value| *value).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_arc_mutex_clone() {
        let mutex = ArcMutex::new(0);
        let mutex_clone = mutex.clone();

        // Test that cloned lock shares data
        let result = mutex_clone.write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, 1);

        // Verify that original lock can see changes
        let result = mutex.read(|value| *value);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_arc_mutex_try_read_returns_would_block() {
        let mutex = Arc::new(ArcMutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
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
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_arc_mutex_try_write_returns_would_block() {
        let mutex = Arc::new(ArcMutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let mutex_clone = mutex.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
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
        let result = mutex.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the write lock
        let result = mutex.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(2));
    }

    #[test]
    fn test_arc_mutex_concurrent_access() {
        let mutex = ArcMutex::new(0);
        let mutex = Arc::new(mutex);

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
    fn test_arc_mutex_with_complex_types() {
        let mutex = ArcMutex::new(String::from("Hello"));

        mutex.write(|s| {
            s.push_str(" World");
        });

        let result = mutex.read(|s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_arc_mutex_multiple_modifications() {
        let mutex = ArcMutex::new(vec![1, 2, 3]);

        mutex.write(|v| {
            v.push(4);
        });

        mutex.write(|v| {
            v.push(5);
        });

        let result = mutex.read(|v| v.clone());
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_arc_mutex_return_values() {
        let mutex = ArcMutex::new(vec![1, 2, 3, 4, 5]);

        let sum = mutex.read(|v| v.iter().sum::<i32>());
        assert_eq!(sum, 15);

        let len = mutex.read(|v| v.len());
        assert_eq!(len, 5);

        let first = mutex.read(|v| v[0]);
        assert_eq!(first, 1);
    }

    #[test]
    fn test_arc_mutex_sharing_across_threads() {
        let mutex = ArcMutex::new(0);

        let mutex1 = mutex.clone();
        let handle1 = thread::spawn(move || {
            for _ in 0..100 {
                mutex1.write(|value| {
                    *value += 1;
                });
            }
        });

        let mutex2 = mutex.clone();
        let handle2 = thread::spawn(move || {
            for _ in 0..100 {
                mutex2.write(|value| {
                    *value += 1;
                });
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        let result = mutex.read(|value| *value);
        assert_eq!(result, 200);
    }

    #[test]
    fn test_arc_mutex_nested_data_structures() {
        use std::collections::HashMap;

        let mutex = ArcMutex::new(HashMap::new());

        mutex.write(|map| {
            map.insert("key1", 10);
            map.insert("key2", 20);
        });

        let value1 = mutex.read(|map| map.get("key1").copied());
        assert_eq!(value1, Some(10));

        let value2 = mutex.read(|map| map.get("key2").copied());
        assert_eq!(value2, Some(20));
    }

    #[test]
    fn test_arc_mutex_read_write_interaction() {
        let mutex = ArcMutex::new(0);

        // Multiple reads should work
        let read1 = mutex.read(|v| *v);
        let read2 = mutex.read(|v| *v);
        assert_eq!(read1, 0);
        assert_eq!(read2, 0);

        // Write should change the value
        mutex.write(|v| *v = 42);

        // Subsequent reads should see the change
        let read3 = mutex.read(|v| *v);
        assert_eq!(read3, 42);
    }

    #[test]
    fn test_arc_mutex_try_read_try_write_interaction() {
        let mutex = ArcMutex::new(0);

        // Try read should work
        let result = mutex.try_read(|v| *v);
        assert_eq!(result, Ok(0));

        // Try write should work
        let result = mutex.try_write(|v| {
            *v = 42;
            *v
        });
        assert_eq!(result, Ok(42));

        // Verify the change
        let result = mutex.try_read(|v| *v);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_arc_mutex_zero_sized_types() {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        struct ZeroSized;

        let mutex = ArcMutex::new(ZeroSized);

        let result = mutex.read(|z| *z);
        assert_eq!(result, ZeroSized);

        mutex.write(|z| {
            // Zero-sized types can't be modified, but we can verify access
            let _ = z;
        });
    }

    #[test]
    fn test_arc_mutex_with_option() {
        let mutex = ArcMutex::new(Some(42));

        let result = mutex.read(|opt| opt.as_ref().copied());
        assert_eq!(result, Some(42));

        mutex.write(|opt| {
            *opt = None;
        });

        let result = mutex.read(|opt| opt.as_ref().copied());
        assert_eq!(result, None);
    }

    #[test]
    fn test_arc_mutex_with_result() {
        let mutex = ArcMutex::new(Ok::<i32, &str>(42));

        let result = mutex.read(|r| *r);
        assert_eq!(result, Ok(42));

        mutex.write(|r| {
            *r = Err("error");
        });

        let result = mutex.read(|r| *r);
        assert_eq!(result, Err("error"));
    }

    #[test]
    fn test_arc_mutex_performance_comparison() {
        // This test ensures ArcMutex performs basic operations without issues
        // In a real performance test, we'd use criterion or similar
        let mutex = ArcMutex::new(0);

        let start = std::time::Instant::now();

        for i in 0..1000 {
            mutex.write(|v| *v = i);
            let _ = mutex.read(|v| *v);
        }

        let elapsed = start.elapsed();
        // Just ensure it completes in reasonable time (less than 1 second)
        assert!(elapsed < std::time::Duration::from_secs(1));
    }
}
