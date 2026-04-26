/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # ArcAsyncRwLock Tests
//!
//! Tests for the ArcAsyncRwLock implementation

use std::sync::Arc;

use qubit_lock::{
    ArcAsyncRwLock,
    AsyncLock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod arc_async_rw_lock_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_new() {
        let async_rw_lock = ArcAsyncRwLock::new(42);
        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_read() {
        let async_rw_lock = ArcAsyncRwLock::new(0);

        // Test read lock
        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_write() {
        let async_rw_lock = ArcAsyncRwLock::new(0);

        // Test write lock
        let result = async_rw_lock
            .write(|value| {
                *value += 1;
                *value
            })
            .await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_clone() {
        let async_rw_lock = ArcAsyncRwLock::new(0);
        let async_rw_lock_clone = async_rw_lock.clone();

        // Test cloned async read-write lock
        let result = async_rw_lock_clone
            .write(|value| {
                *value += 1;
                *value
            })
            .await;
        assert_eq!(result, 1);

        // Verify that original lock can see changes
        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_concurrent_readers() {
        let async_rw_lock = ArcAsyncRwLock::new(vec![1, 2, 3, 4, 5]);
        let async_rw_lock = Arc::new(async_rw_lock);
        let mut handles = vec![];

        // Create multiple reader tasks
        for _ in 0..10 {
            let async_rw_lock = Arc::clone(&async_rw_lock);
            let handle = tokio::spawn(async move {
                async_rw_lock
                    .read(|data| {
                        // Simulate some read operation
                        data.iter().sum::<i32>()
                    })
                    .await
            });
            handles.push(handle);
        }

        // All readers should get the same result
        for handle in handles {
            let sum = handle.await.unwrap();
            assert_eq!(sum, 15);
        }
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_write_lock_is_exclusive() {
        let async_rw_lock = ArcAsyncRwLock::new(0);
        let async_rw_lock = Arc::new(async_rw_lock);
        let mut handles = vec![];

        // Create multiple writer tasks
        for _ in 0..10 {
            let async_rw_lock = Arc::clone(&async_rw_lock);
            let handle = tokio::spawn(async move {
                async_rw_lock
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

        // Verify final value (should be 10 if writes are exclusive)
        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 10);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_read_after_write() {
        let async_rw_lock = ArcAsyncRwLock::new(String::from("Hello"));

        // Write operation
        async_rw_lock
            .write(|s| {
                s.push_str(" World");
            })
            .await;

        // Read operation should see the change
        let result = async_rw_lock.read(|s| s.clone()).await;
        assert_eq!(result, "Hello World");
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_with_complex_types() {
        let async_rw_lock = ArcAsyncRwLock::new(vec![1, 2, 3]);

        // Multiple readers can access concurrently
        let len = async_rw_lock.read(|v| v.len()).await;
        assert_eq!(len, 3);

        // Writer modifies the data
        async_rw_lock
            .write(|v| {
                v.push(4);
                v.push(5);
            })
            .await;

        // Reader sees the updated data
        let sum = async_rw_lock.read(|v| v.iter().sum::<i32>()).await;
        assert_eq!(sum, 15);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_read_lock_returns_closure_result() {
        let async_rw_lock = ArcAsyncRwLock::new(vec![10, 20, 30]);

        let result = async_rw_lock
            .read(|v| v.iter().map(|&x| x * 2).collect::<Vec<_>>())
            .await;

        assert_eq!(result, vec![20, 40, 60]);

        // Original should be unchanged
        let original = async_rw_lock.read(|v| v.clone()).await;
        assert_eq!(original, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_write_lock_returns_closure_result() {
        let async_rw_lock = ArcAsyncRwLock::new(5);

        let result = async_rw_lock
            .write(|value| {
                *value *= 2;
                *value
            })
            .await;

        assert_eq!(result, 10);

        // Verify the value was actually modified
        let current = async_rw_lock.read(|value| *value).await;
        assert_eq!(current, 10);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_mixed_read_write_operations() {
        let async_rw_lock = ArcAsyncRwLock::new(0);
        let async_rw_lock = Arc::new(async_rw_lock);
        let mut handles = vec![];

        // Create some readers
        for _ in 0..5 {
            let async_rw_lock = Arc::clone(&async_rw_lock);
            let handle = tokio::spawn(async move {
                for _ in 0..10 {
                    async_rw_lock
                        .read(|value| {
                            let _ = *value;
                        })
                        .await;
                }
            });
            handles.push(handle);
        }

        // Create some writers
        for _ in 0..5 {
            let async_rw_lock = Arc::clone(&async_rw_lock);
            let handle = tokio::spawn(async move {
                for _ in 0..10 {
                    async_rw_lock
                        .write(|value| {
                            *value += 1;
                        })
                        .await;
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify final value
        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 50); // 5 writers × 10 increments each
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_readers_do_not_block_each_other() {
        let async_rw_lock = ArcAsyncRwLock::new(vec![1, 2, 3, 4, 5]);
        let async_rw_lock = Arc::new(async_rw_lock);
        let mut handles = vec![];

        // Create multiple readers that all access the lock simultaneously
        for i in 0..5 {
            let async_rw_lock = Arc::clone(&async_rw_lock);
            let handle = tokio::spawn(async move {
                // All readers should be able to access concurrently
                async_rw_lock
                    .read(|data| data.iter().sum::<i32>() + i)
                    .await
            });
            handles.push(handle);
        }

        // All readers should successfully complete and return results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // Verify all readers got the correct sum (15) plus their index
        assert_eq!(results.len(), 5);
        for (i, &result) in results.iter().enumerate() {
            assert_eq!(result, 15 + i as i32);
        }
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_writer_blocks_readers() {
        let async_rw_lock = ArcAsyncRwLock::new(0);
        let async_rw_lock = Arc::new(async_rw_lock);

        // Hold write lock in one task
        let async_rw_lock_clone = async_rw_lock.clone();
        let write_handle = tokio::spawn(async move {
            async_rw_lock_clone
                .write(|value| {
                    *value += 1;
                    // Hold the write lock for some time
                    std::thread::sleep(std::time::Duration::from_millis(50));
                })
                .await;
        });

        // Give the write task time to acquire the lock
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to read (should wait for write to complete)
        let read_result = async_rw_lock.read(|value| *value).await;

        // Wait for write task to complete
        write_handle.await.unwrap();

        // Should see the updated value
        assert_eq!(read_result, 1);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_sharing_across_tasks() {
        let async_rw_lock = ArcAsyncRwLock::new(0);

        let async_rw_lock1 = async_rw_lock.clone();
        let handle1 = tokio::spawn(async move {
            for _ in 0..50 {
                async_rw_lock1
                    .write(|value| {
                        *value += 1;
                    })
                    .await;
            }
        });

        let async_rw_lock2 = async_rw_lock.clone();
        let handle2 = tokio::spawn(async move {
            for _ in 0..50 {
                async_rw_lock2
                    .write(|value| {
                        *value += 1;
                    })
                    .await;
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 100);
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_nested_data_structures() {
        use std::collections::HashMap;

        let async_rw_lock = ArcAsyncRwLock::new(HashMap::new());

        async_rw_lock
            .write(|map| {
                map.insert("key1", 10);
                map.insert("key2", 20);
            })
            .await;

        let value1 = async_rw_lock.read(|map| map.get("key1").copied()).await;
        assert_eq!(value1, Some(10));

        let value2 = async_rw_lock.read(|map| map.get("key2").copied()).await;
        assert_eq!(value2, Some(20));
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_with_result_types() {
        let async_rw_lock = ArcAsyncRwLock::new(10);

        let result = async_rw_lock
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
    async fn test_arc_async_rw_lock_try_read_returns_would_block_when_write_locked() {
        let async_rw_lock = Arc::new(ArcAsyncRwLock::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let lock_clone = async_rw_lock.clone();
        let barrier_clone = barrier.clone();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                lock_clone
                    .write(|_| {
                        barrier_clone.wait();
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    })
                    .await;
            });
        });

        barrier.wait();
        let result = async_rw_lock.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        handle.join().unwrap();
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_try_methods_cover_shared_function_pointer_paths() {
        let async_rw_lock = Arc::new(ArcAsyncRwLock::new(0));

        assert_eq!(async_rw_lock.try_read(read_i32), Ok(0));
        assert_eq!(async_rw_lock.try_write(increment_i32), Ok(1));

        let read_barrier = Arc::new(std::sync::Barrier::new(2));
        let read_lock = async_rw_lock.clone();
        let read_barrier_clone = read_barrier.clone();
        let read_holder = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                read_lock
                    .write(|_| {
                        read_barrier_clone.wait();
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    })
                    .await;
            });
        });
        read_barrier.wait();
        assert_eq!(
            async_rw_lock.try_read(read_i32),
            Err(TryLockError::WouldBlock),
        );
        read_holder.join().unwrap();

        let write_barrier = Arc::new(std::sync::Barrier::new(2));
        let write_lock = async_rw_lock.clone();
        let write_barrier_clone = write_barrier.clone();
        let write_holder = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                write_lock
                    .read(|_| {
                        write_barrier_clone.wait();
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    })
                    .await;
            });
        });
        write_barrier.wait();
        assert_eq!(
            async_rw_lock.try_write(increment_i32),
            Err(TryLockError::WouldBlock),
        );
        write_holder.join().unwrap();
    }

    #[tokio::test]
    async fn test_arc_async_rw_lock_try_write_returns_would_block_when_read_locked() {
        let async_rw_lock = Arc::new(ArcAsyncRwLock::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let lock_clone = async_rw_lock.clone();
        let barrier_clone = barrier.clone();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                lock_clone
                    .read(|_| {
                        barrier_clone.wait();
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    })
                    .await;
            });
        });

        barrier.wait();
        let result = async_rw_lock.try_write(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        handle.join().unwrap();
    }
}
