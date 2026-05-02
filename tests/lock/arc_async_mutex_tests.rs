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
//! # ArcAsyncMutex Tests
//!
//! Tests for the ArcAsyncMutex implementation

use std::sync::Arc;

use qubit_lock::{
    ArcAsyncMutex,
    AsyncLock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod arc_async_mutex_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    #[tokio::test]
    async fn test_arc_async_mutex_new() {
        let async_mutex = ArcAsyncMutex::new(42);
        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_read_write() {
        let async_mutex = ArcAsyncMutex::new(0);

        // Test async lock
        let result = async_mutex
            .write(|value| {
                *value += 1;
                *value
            })
            .await;
        assert_eq!(result, 1);

        // Test trying to acquire lock
        let result = async_mutex.try_read(|value| *value).unwrap();
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_clone() {
        let async_mutex = ArcAsyncMutex::new(0);
        let async_mutex_clone = async_mutex.clone();

        // Test cloned async lock
        let result = async_mutex_clone
            .write(|value| {
                *value += 1;
                *value
            })
            .await;
        assert_eq!(result, 1);

        // Verify that original lock can see changes
        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_try_read_returns_would_block() {
        use std::{
            sync::mpsc,
            time::Duration,
        };

        let async_mutex = Arc::new(ArcAsyncMutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let async_mutex_clone = async_mutex.clone();

        // Hold the lock in another thread (note: using thread instead of tokio task)
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("failed to create Tokio runtime");
            rt.block_on(async {
                async_mutex_clone
                    .write(move |value| {
                        *value += 1;
                        locked_tx.send(()).expect("test should observe held mutex");
                        release_rx
                            .recv_timeout(Duration::from_secs(1))
                            .expect("test should release held mutex");
                    })
                    .await;
            });
        });

        // Wait for the spawned thread to acquire the lock.
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        // Try to acquire lock, should report contention.
        let result = async_mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for child thread to complete
        handle.join().expect("holder thread should not panic");

        // Now should be able to successfully acquire the lock
        let result = async_mutex.try_read(|value| *value);
        assert_eq!(result, Ok(1));
    }

    #[tokio::test]
    async fn test_arc_async_mutex_concurrent_access() {
        let async_mutex = ArcAsyncMutex::new(0);
        let async_mutex = Arc::new(async_mutex);
        let mut handles = vec![];

        // Create multiple tasks accessing the lock concurrently
        for _ in 0..10 {
            let async_mutex = Arc::clone(&async_mutex);
            let handle = tokio::spawn(async move {
                async_mutex
                    .write(|value| {
                        *value += 1;
                    })
                    .await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify final value
        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 10);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_with_complex_types() {
        let async_mutex = ArcAsyncMutex::new(String::from("Hello"));

        async_mutex
            .write(|s| {
                s.push_str(" World");
            })
            .await;

        let result = async_mutex.read(|s| s.clone()).await;
        assert_eq!(result, "Hello World");
    }

    #[tokio::test]
    async fn test_arc_async_mutex_multiple_modifications() {
        let async_mutex = ArcAsyncMutex::new(vec![1, 2, 3]);

        async_mutex
            .write(|v| {
                v.push(4);
            })
            .await;

        async_mutex
            .write(|v| {
                v.push(5);
            })
            .await;

        let result = async_mutex.read(|v| v.clone()).await;
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_return_values() {
        let async_mutex = ArcAsyncMutex::new(vec![1, 2, 3, 4, 5]);

        let sum = async_mutex.read(|v| v.iter().sum::<i32>()).await;
        assert_eq!(sum, 15);

        let len = async_mutex.read(|v| v.len()).await;
        assert_eq!(len, 5);

        let first = async_mutex.read(|v| v[0]).await;
        assert_eq!(first, 1);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_sharing_across_tasks() {
        let async_mutex = ArcAsyncMutex::new(0);

        let async_mutex1 = async_mutex.clone();
        let handle1 = tokio::spawn(async move {
            for _ in 0..100 {
                async_mutex1
                    .write(|value| {
                        *value += 1;
                    })
                    .await;
            }
        });

        let async_mutex2 = async_mutex.clone();
        let handle2 = tokio::spawn(async move {
            for _ in 0..100 {
                async_mutex2
                    .write(|value| {
                        *value += 1;
                    })
                    .await;
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 200);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_nested_data_structures() {
        use std::collections::HashMap;

        let async_mutex = ArcAsyncMutex::new(HashMap::new());

        async_mutex
            .write(|map| {
                map.insert("key1", 10);
                map.insert("key2", 20);
            })
            .await;

        let value1 = async_mutex.read(|map| map.get("key1").copied()).await;
        assert_eq!(value1, Some(10));

        let value2 = async_mutex.read(|map| map.get("key2").copied()).await;
        assert_eq!(value2, Some(20));
    }

    #[tokio::test]
    async fn test_arc_async_mutex_fairness() {
        let async_mutex = ArcAsyncMutex::new(Vec::new());
        let async_mutex = Arc::new(async_mutex);
        let mut handles = vec![];

        // Spawn multiple tasks that append their ID
        for i in 0..5 {
            let async_mutex = Arc::clone(&async_mutex);
            let handle = tokio::spawn(async move {
                async_mutex
                    .write(|v| {
                        v.push(i);
                    })
                    .await;
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all tasks completed
        let result = async_mutex.read(|v| v.len()).await;
        assert_eq!(result, 5);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_serializes_contended_writes() {
        use std::{
            sync::mpsc,
            time::Duration,
        };

        let async_mutex = ArcAsyncMutex::new(0);
        let async_mutex = Arc::new(async_mutex);
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (attempted_tx, attempted_rx) = tokio::sync::oneshot::channel();

        let async_mutex_clone = async_mutex.clone();

        let holder = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("failed to create Tokio runtime");
            rt.block_on(async {
                async_mutex_clone
                    .write(move |value| {
                        *value += 1;
                        locked_tx.send(()).expect("test should observe held mutex");
                        release_rx
                            .recv_timeout(Duration::from_secs(1))
                            .expect("test should release held mutex");
                    })
                    .await;
            });
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        let async_mutex_clone2 = async_mutex.clone();
        let writer = tokio::spawn(async move {
            attempted_tx
                .send(())
                .expect("test should observe contended writer attempt");
            async_mutex_clone2
                .write(|value| {
                    *value += 1;
                })
                .await;
        });

        attempted_rx
            .await
            .expect("contended writer should attempt to acquire the mutex");
        assert_eq!(
            async_mutex.try_read(|value| *value),
            Err(TryLockError::WouldBlock),
        );

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        holder.join().expect("holder thread should not panic");
        writer.await.unwrap();

        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_with_result_types() {
        let async_mutex = ArcAsyncMutex::new(10);

        let result = async_mutex
            .read(|value| -> Result<i32, &str> {
                if *value > 0 {
                    Ok(*value * 2)
                } else {
                    Err("value must be positive")
                }
            })
            .await;

        assert_eq!(result, Ok(20));
    }

    #[tokio::test]
    async fn test_arc_async_mutex_try_write_returns_ok() {
        let async_mutex = ArcAsyncMutex::new(0);

        // For async mutex, try_write will succeed when the lock is not held
        // This test verifies that try_write works correctly in the normal case
        let result = async_mutex.try_write(|value| *value);
        assert_eq!(result, Ok(0));

        // Try again immediately, should still succeed since we released the lock
        let result = async_mutex.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(1));
    }

    #[tokio::test]
    async fn test_arc_async_mutex_try_methods_cover_shared_function_pointer_paths() {
        use std::{
            sync::mpsc,
            time::Duration,
        };

        let async_mutex = Arc::new(ArcAsyncMutex::new(0));

        assert_eq!(async_mutex.try_read(read_i32), Ok(0));
        assert_eq!(async_mutex.try_write(increment_i32), Ok(1));

        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let lock_clone = async_mutex.clone();
        let holder = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("failed to create Tokio runtime");
            rt.block_on(async {
                lock_clone
                    .write(move |_| {
                        locked_tx.send(()).expect("test should observe held mutex");
                        release_rx
                            .recv_timeout(Duration::from_secs(1))
                            .expect("test should release held mutex");
                    })
                    .await;
            });
        });

        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");
        assert_eq!(
            async_mutex.try_read(read_i32),
            Err(TryLockError::WouldBlock),
        );
        assert_eq!(
            async_mutex.try_write(increment_i32),
            Err(TryLockError::WouldBlock),
        );
        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        holder.join().expect("holder thread should not panic");
    }

    #[tokio::test]
    async fn test_arc_async_mutex_zero_sized_types() {
        let async_mutex = ArcAsyncMutex::new(());

        let result = async_mutex.read(|_| "read_result").await;
        assert_eq!(result, "read_result");

        let result = async_mutex.write(|_| "write_result").await;
        assert_eq!(result, "write_result");

        let result = async_mutex.try_read(|_| "try_read_result");
        assert_eq!(result, Ok("try_read_result"));

        let result = async_mutex.try_write(|_| "try_write_result");
        assert_eq!(result, Ok("try_write_result"));
    }

    #[tokio::test]
    async fn test_arc_async_mutex_with_option() {
        let async_mutex = ArcAsyncMutex::new(Some(42));

        let result = async_mutex.read(|opt| opt.as_ref().map(|&x| x * 2)).await;
        assert_eq!(result, Some(84));

        async_mutex
            .write(|opt| {
                *opt = None;
            })
            .await;

        let result = async_mutex.read(|opt| opt.is_none()).await;
        assert!(result);
    }

    #[tokio::test]
    async fn test_arc_async_mutex_performance_comparison() {
        let async_mutex1 = ArcAsyncMutex::new(0);
        let async_mutex2 = ArcAsyncMutex::new(0);

        // Test that multiple operations work correctly
        for i in 0..5 {
            async_mutex1.write(|val| *val += i).await;
            async_mutex2.write(|val| *val += i * 2).await;
        }

        let sum1 = async_mutex1.read(|val| *val).await;
        let sum2 = async_mutex2.read(|val| *val).await;

        // sum1 = 0+1+2+3+4 = 10
        // sum2 = 0+2+4+6+8 = 20
        assert_eq!(sum1, 10);
        assert_eq!(sum2, 20);
    }
}
