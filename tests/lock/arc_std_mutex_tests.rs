/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # ArcStdMutex Tests
//!
//! Tests for the ArcStdMutex implementation

use std::{
    sync::{
        Arc,
        Barrier,
    },
    thread,
};

use qubit_lock::{
    Lock,
    TryLockError,
    lock::ArcStdMutex,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod arc_std_mutex_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    #[test]
    fn test_arc_mutex_new() {
        let mutex = ArcStdMutex::new(42);
        let result = mutex.read(|value| *value);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_arc_mutex_with_lock_basic_operations() {
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
    fn test_arc_mutex_try_with_lock_success() {
        let mutex = ArcStdMutex::new(42);

        let result = mutex.try_read(|value| *value).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_arc_mutex_clone() {
        let mutex = ArcStdMutex::new(0);
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
    fn test_arc_mutex_clone_with_different_types() {
        // Test Clone with String
        let string_mutex = ArcStdMutex::new(String::from("hello"));
        let string_clone = string_mutex.clone();
        string_clone.write(|s| s.push_str(" world"));
        let result = string_mutex.read(|s| s.clone());
        assert_eq!(result, "hello world");

        // Test Clone with Vec
        let vec_mutex = ArcStdMutex::new(vec![1, 2, 3]);
        let vec_clone = vec_mutex.clone();
        vec_clone.write(|v| v.push(4));
        let result = vec_mutex.read(|v| v.clone());
        assert_eq!(result, vec![1, 2, 3, 4]);

        // Test Clone with Option
        let option_mutex = ArcStdMutex::new(Some(42));
        let option_clone = option_mutex.clone();
        option_clone.write(|opt| *opt = Some(84));
        let result = option_mutex.read(|opt| *opt);
        assert_eq!(result, Some(84));
    }

    #[test]
    fn test_arc_mutex_clone_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let mutex = Arc::new(ArcStdMutex::new(0));
        let mut handles = vec![];

        // Create clones for concurrent access
        for _ in 0..5 {
            let mutex_clone = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                mutex_clone.write(|value| {
                    *value += 1;
                });
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final value through original mutex
        let result = mutex.read(|value| *value);
        assert_eq!(result, 5);
    }

    #[test]
    fn test_arc_mutex_try_with_lock_returns_would_block() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                // Notify main thread that it can try to acquire the lock
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
    fn test_arc_mutex_try_with_lock_poisoned() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock and panic in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                // Notify main thread that lock has been acquired
                barrier_clone.wait();
                // Panic while holding the lock, causing the lock to be poisoned
                panic!("intentional panic to poison the lock");
            });
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Wait for child thread to complete panicking (will poison the lock)
        let _ = handle.join();

        // Try to acquire poisoned lock, should return Poisoned
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_arc_mutex_try_methods_cover_shared_function_pointer_paths() {
        let mutex = Arc::new(ArcStdMutex::new(0));

        assert_eq!(mutex.try_read(read_i32), Ok(0));
        assert_eq!(mutex.try_write(increment_i32), Ok(1));

        let barrier = Arc::new(Barrier::new(2));
        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();
        let holder = thread::spawn(move || {
            mutex_clone.write(|_| {
                barrier_clone.wait();
                thread::sleep(std::time::Duration::from_millis(50));
            });
        });
        barrier.wait();
        assert_eq!(mutex.try_read(read_i32), Err(TryLockError::WouldBlock));
        assert_eq!(
            mutex.try_write(increment_i32),
            Err(TryLockError::WouldBlock),
        );
        holder.join().unwrap();

        let poisoned = Arc::new(ArcStdMutex::new(0));
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
    fn test_arc_mutex_try_read_returns_would_block() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                barrier_clone.wait();
                thread::sleep(std::time::Duration::from_millis(100));
            });
        });

        barrier.wait();
        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        handle.join().unwrap();
    }

    #[test]
    fn test_arc_mutex_try_read_returns_poisoned() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                barrier_clone.wait();
                panic!("intentional panic to poison the lock");
            });
        });

        barrier.wait();
        let _ = handle.join();

        let result = mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    fn test_arc_mutex_try_write_returns_poisoned() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                barrier_clone.wait();
                panic!("intentional panic to poison the lock");
            });
        });

        barrier.wait();
        let _ = handle.join();

        let result = mutex.try_write(|value| *value);
        assert_eq!(result, Err(TryLockError::Poisoned));
    }

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_arc_mutex_with_lock_poisoned() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock and panic in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                // Notify main thread that lock has been acquired
                barrier_clone.wait();
                // Panic while holding the lock, causing the lock to be poisoned
                panic!("intentional panic to poison the lock");
            });
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Wait for child thread to complete panicking (will poison the lock)
        let _ = handle.join();

        // Try to acquire poisoned lock with read (not try_read)
        // This should panic because read uses unwrap()
        mutex.read(|_| {});
    }

    #[test]
    fn test_arc_mutex_concurrent_access() {
        let mutex = ArcStdMutex::new(0);
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
        let mutex = ArcStdMutex::new(String::from("Hello"));

        mutex.write(|s| {
            s.push_str(" World");
        });

        let result = mutex.read(|s| s.clone());
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_arc_mutex_multiple_modifications() {
        let mutex = ArcStdMutex::new(vec![1, 2, 3]);

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
        let mutex = ArcStdMutex::new(vec![1, 2, 3, 4, 5]);

        let sum = mutex.read(|v| v.iter().sum::<i32>());
        assert_eq!(sum, 15);

        let len = mutex.read(|v| v.len());
        assert_eq!(len, 5);

        let first = mutex.read(|v| v[0]);
        assert_eq!(first, 1);
    }

    #[test]
    fn test_arc_mutex_sharing_across_threads() {
        let mutex = ArcStdMutex::new(0);

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

        let mutex = ArcStdMutex::new(HashMap::new());

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
    fn test_arc_mutex_try_write_returns_would_block() {
        let mutex = Arc::new(ArcStdMutex::new(0));
        let barrier = Arc::new(Barrier::new(2));

        let mutex_clone = mutex.clone();
        let barrier_clone = barrier.clone();

        // Hold the lock in another thread
        let handle = thread::spawn(move || {
            mutex_clone.write(|value| {
                *value += 1;
                // Notify main thread that it can try to acquire the lock
                barrier_clone.wait();
                // Hold the lock for some time
                thread::sleep(std::time::Duration::from_millis(100));
            });
        });

        // Wait for child thread to acquire the lock
        barrier.wait();

        // Try to acquire write lock, should return WouldBlock
        let result = mutex.try_write(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        // Wait for child thread to complete
        handle.join().unwrap();

        // Now should be able to successfully acquire the lock
        let result = mutex.try_write(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_arc_mutex_zero_sized_types() {
        let mutex = ArcStdMutex::new(());

        let result = mutex.read(|_| "read_result");
        assert_eq!(result, "read_result");

        let result = mutex.write(|_| "write_result");
        assert_eq!(result, "write_result");

        let result = mutex.try_read(|_| "try_read_result");
        assert_eq!(result, Ok("try_read_result"));

        let result = mutex.try_write(|_| "try_write_result");
        assert_eq!(result, Ok("try_write_result"));
    }

    #[test]
    fn test_arc_mutex_with_option() {
        let mutex = ArcStdMutex::new(Some(42));

        let result = mutex.read(|opt| opt.as_ref().map(|&x| x * 2));
        assert_eq!(result, Some(84));

        mutex.write(|opt| {
            *opt = None;
        });

        let result = mutex.read(|opt| opt.is_none());
        assert!(result);
    }

    #[test]
    fn test_arc_mutex_with_result() {
        let mutex = ArcStdMutex::new(Ok::<i32, &str>(42));

        let result = mutex.write(|res| match res {
            Ok(val) => {
                *val *= 2;
                Ok(*val)
            }
            Err(_) => Err("was error"),
        });
        assert_eq!(result, Ok(84));

        mutex.write(|res| {
            *res = Err("test error");
        });

        let result = mutex.read(|res| *res);
        assert_eq!(result, Err("test error"));
    }

    #[test]
    fn test_arc_mutex_performance_comparison() {
        let mutex1 = ArcStdMutex::new(0);
        let mutex2 = ArcStdMutex::new(0);

        // Test that multiple operations work correctly
        for i in 0..10 {
            mutex1.write(|val| *val += i);
            mutex2.write(|val| *val += i * 2);
        }

        let sum1 = mutex1.read(|val| *val);
        let sum2 = mutex2.read(|val| *val);

        // sum1 = 0+1+2+...+9 = 45
        // sum2 = 0+2+4+...+18 = 90
        assert_eq!(sum1, 45);
        assert_eq!(sum2, 90);
    }

    #[test]
    fn test_arc_mutex_try_read_try_write_interaction() {
        let mutex = ArcStdMutex::new(42);

        // Test successful try_read
        let result = mutex.try_read(|val| *val);
        assert_eq!(result, Ok(42));

        // Test successful try_write
        let result = mutex.try_write(|val| {
            *val += 1;
            *val
        });
        assert_eq!(result, Ok(43));

        // Verify the change
        let result = mutex.try_read(|val| *val);
        assert_eq!(result, Ok(43));
    }
}
