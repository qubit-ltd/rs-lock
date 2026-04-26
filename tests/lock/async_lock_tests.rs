/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # AsyncLock Trait Tests
//!
//! Tests for the AsyncLock trait and its implementations for tokio::sync::Mutex and tokio::sync::RwLock

use tokio::sync::{
    Mutex as AsyncMutex,
    RwLock as AsyncRwLock,
};

use qubit_lock::lock::{
    ArcAsyncMutex,
    ArcAsyncRwLock,
    AsyncLock,
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
mod async_lock_trait_tests {
    use super::*;

    #[tokio::test]
    async fn test_async_mutex_read_write_basic_operations() {
        let async_mutex = ArcAsyncMutex::new(0);

        // Test basic lock and modify
        let result = async_mutex
            .write(|value| {
                *value += 1;
                *value
            })
            .await;
        assert_eq!(result, 1);

        // Verify the value was persisted
        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_async_mutex_read_returns_closure_result() {
        let async_mutex = ArcAsyncMutex::new(vec![1, 2, 3]);

        let length = async_mutex.read(|v| v.len()).await;
        assert_eq!(length, 3);

        let sum = async_mutex.read(|v| v.iter().sum::<i32>()).await;
        assert_eq!(sum, 6);
    }

    #[tokio::test]
    async fn test_async_mutex_try_read_write_success() {
        let async_mutex = ArcAsyncMutex::new(42);

        // Should successfully acquire the lock
        let result = async_mutex.try_read(|value| *value);
        assert_eq!(result, Ok(42));

        // Should be able to modify
        let result = async_mutex.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[tokio::test]
    async fn test_async_mutex_try_read_returns_would_block_when_locked() {
        use std::{
            sync::{
                Arc,
                mpsc,
            },
            time::Duration,
        };

        let async_mutex = Arc::new(ArcAsyncMutex::new(0));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        // Create a new reference to try acquiring in parallel
        let async_mutex_clone = async_mutex.clone();

        // Hold the lock in another thread (note: using thread instead of tokio task)
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("failed to create Tokio runtime");
            rt.block_on(async {
                async_mutex_clone
                    .write(move |_| {
                        locked_tx.send(()).expect("test should observe held mutex");
                        release_rx
                            .recv_timeout(Duration::from_secs(1))
                            .expect("test should release held mutex");
                    })
                    .await;
            });
        });

        // Wait for the lock to be held
        locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("mutex should be held within timeout");

        // Try to acquire lock while it's held, should report contention.
        let result = async_mutex.try_read(|value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));

        release_tx
            .send(())
            .expect("holder thread should still be waiting for release");

        // Wait for the spawned thread to complete
        handle.join().expect("holder thread should not panic");

        // Now should be able to successfully acquire the lock
        let result = async_mutex.try_read(|value| *value);
        assert_eq!(result, Ok(0));
    }

    #[tokio::test]
    async fn test_async_mutex_concurrent_access() {
        use std::sync::Arc;

        let async_mutex = Arc::new(ArcAsyncMutex::new(0));
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
    async fn test_async_mutex_read_write_complex_types() {
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
    async fn test_async_mutex_nested_operations() {
        let async_mutex = ArcAsyncMutex::new(vec![1, 2, 3]);

        let result = async_mutex
            .write(|v| {
                v.push(4);
                v.push(5);
                v.iter().map(|&x| x * 2).collect::<Vec<_>>()
            })
            .await;

        assert_eq!(result, vec![2, 4, 6, 8, 10]);

        // Verify original was modified
        let original = async_mutex.read(|v| v.clone()).await;
        assert_eq!(original, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_async_mutex_fairness() {
        use std::sync::Arc;

        let async_mutex = Arc::new(ArcAsyncMutex::new(Vec::new()));
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
    async fn test_async_mutex_serializes_contended_writes() {
        use std::sync::Arc;

        let async_mutex = Arc::new(ArcAsyncMutex::new(0));
        let async_mutex_clone = async_mutex.clone();

        // Hold the lock long enough for the second task to contend.
        let handle1 = tokio::spawn(async move {
            async_mutex_clone
                .write(|value| {
                    *value += 1;
                    // The closure itself is synchronous while the guard is held.
                    std::thread::sleep(std::time::Duration::from_millis(50));
                })
                .await;
        });

        // The second write should wait for the first guard to be released.
        let async_mutex_clone2 = async_mutex.clone();
        let handle2 = tokio::spawn(async move {
            async_mutex_clone2
                .write(|value| {
                    *value += 1;
                })
                .await;
        });

        // Both tasks should complete
        handle1.await.unwrap();
        handle2.await.unwrap();

        let result = async_mutex.read(|value| *value).await;
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn test_async_mutex_with_result_types() {
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

    // Tests for AsyncMutex trait implementation
    #[tokio::test]
    async fn test_tokio_async_mutex_read() {
        let mutex = AsyncMutex::new(42);
        let result = AsyncLock::read(&mutex, |value| *value).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_write() {
        let mutex = AsyncMutex::new(0);
        let result = AsyncLock::write(&mutex, |value| {
            *value += 1;
            *value
        })
        .await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_try_read_success() {
        let mutex = AsyncMutex::new(42);
        let result = AsyncLock::try_read(&mutex, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_try_write_success() {
        let mutex = AsyncMutex::new(42);
        let result = AsyncLock::try_write(&mutex, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_try_write_succeeds_after_guard_released() {
        let mutex = AsyncMutex::new(0);

        // Hold the lock in current task first to ensure it's locked
        let result = AsyncLock::try_write(&mutex, |value| *value);
        assert_eq!(result, Ok(0)); // Should succeed initially

        // Now try again while it's not locked (since we're in the same task)
        let result = AsyncLock::try_write(&mutex, |value| *value);
        assert_eq!(result, Ok(0)); // Should succeed again since lock was released
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_try_read_returns_would_block_when_locked() {
        let mutex = AsyncMutex::new(0);
        let _guard = mutex
            .try_lock()
            .expect("failed to acquire initial mutex guard");

        let result = AsyncLock::try_read(&mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_try_write_returns_would_block_when_guard_held() {
        let mutex = AsyncMutex::new(0);
        let _guard = mutex
            .try_lock()
            .expect("failed to acquire initial mutex guard");

        let result = AsyncLock::try_write(&mutex, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));
    }

    #[tokio::test]
    async fn test_tokio_async_mutex_try_methods_cover_shared_function_pointer_paths() {
        let mutex = AsyncMutex::new(0);

        assert_eq!(AsyncLock::try_read(&mutex, read_i32), Ok(0));
        assert_eq!(AsyncLock::try_write(&mutex, increment_i32), Ok(1));

        let guard = mutex
            .try_lock()
            .expect("failed to acquire initial mutex guard");
        assert_eq!(
            AsyncLock::try_read(&mutex, read_i32),
            Err(TryLockError::WouldBlock),
        );
        assert_eq!(
            AsyncLock::try_write(&mutex, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        drop(guard);
    }
}

#[cfg(test)]
mod async_rwlock_trait_tests {
    use super::*;

    #[tokio::test]
    async fn test_async_rwlock_read_basic() {
        let async_rw_lock = ArcAsyncRwLock::new(42);

        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_async_rwlock_write_basic() {
        let async_rw_lock = ArcAsyncRwLock::new(0);

        let result = async_rw_lock
            .write(|value| {
                *value += 1;
                *value
            })
            .await;
        assert_eq!(result, 1);

        // Verify the value was persisted
        let result = async_rw_lock.read(|value| *value).await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_async_rwlock_concurrent_readers() {
        use std::sync::Arc;

        let async_rw_lock = Arc::new(ArcAsyncRwLock::new(vec![1, 2, 3, 4, 5]));
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
    async fn test_async_rwlock_write_lock_is_exclusive() {
        use std::sync::Arc;

        let async_rw_lock = Arc::new(ArcAsyncRwLock::new(0));
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
    async fn test_async_rwlock_read_after_write() {
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
    async fn test_async_rwlock_with_complex_types() {
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
    async fn test_async_rwlock_read_lock_returns_closure_result() {
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
    async fn test_async_rwlock_write_lock_returns_closure_result() {
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
    async fn test_async_rwlock_try_read_success() {
        let async_rw_lock = ArcAsyncRwLock::new(42);

        // Should successfully acquire the read lock
        let result = async_rw_lock.try_read(|value| *value);
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_async_rwlock_try_write_success() {
        let async_rw_lock = ArcAsyncRwLock::new(42);

        // Should successfully acquire the write lock
        let result = async_rw_lock.try_write(|value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[tokio::test]
    async fn test_async_rwlock_mixed_read_write_operations() {
        use std::sync::Arc;

        let async_rw_lock = Arc::new(ArcAsyncRwLock::new(0));
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

    // Tests for AsyncRwLock trait implementation
    #[tokio::test]
    async fn test_tokio_async_rwlock_read() {
        let rwlock = AsyncRwLock::new(42);
        let result = AsyncLock::read(&rwlock, |value| *value).await;
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_write() {
        let rwlock = AsyncRwLock::new(0);
        let result = AsyncLock::write(&rwlock, |value| {
            *value += 1;
            *value
        })
        .await;
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_read_success() {
        let rwlock = AsyncRwLock::new(42);
        let result = AsyncLock::try_read(&rwlock, |value| *value);
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_write_success() {
        let rwlock = AsyncRwLock::new(42);
        let result = AsyncLock::try_write(&rwlock, |value| {
            *value += 1;
            *value
        });
        assert_eq!(result, Ok(43));
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_read_succeeds_after_write_guard_released() {
        let rwlock = AsyncRwLock::new(0);

        // First acquire write lock to ensure it's locked
        let result = AsyncLock::try_write(&rwlock, |value| *value);
        assert_eq!(result, Ok(0)); // Should succeed initially

        // Now try to acquire read lock while write lock was held (but now released)
        let result = AsyncLock::try_read(&rwlock, |value| *value);
        assert_eq!(result, Ok(0)); // Should succeed since lock was released
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_write_succeeds_after_read_guard_released() {
        let rwlock = AsyncRwLock::new(0);

        // First acquire read lock to ensure it's locked
        let result = AsyncLock::try_read(&rwlock, |value| *value);
        assert_eq!(result, Ok(0)); // Should succeed initially

        // Now try to acquire write lock while read lock was held (but now released)
        let result = AsyncLock::try_write(&rwlock, |value| *value);
        assert_eq!(result, Ok(0)); // Should succeed since lock was released
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_read_returns_would_block_when_write_guard_held() {
        let rwlock = AsyncRwLock::new(0);
        let _guard = rwlock
            .try_write()
            .expect("failed to acquire initial write guard");

        let result = AsyncLock::try_read(&rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_write_returns_would_block_when_read_guard_held() {
        let rwlock = AsyncRwLock::new(0);
        let _guard = rwlock
            .try_read()
            .expect("failed to acquire initial read guard");

        let result = AsyncLock::try_write(&rwlock, |value| *value);
        assert_eq!(result, Err(TryLockError::WouldBlock));
    }

    #[tokio::test]
    async fn test_tokio_async_rwlock_try_methods_cover_shared_function_pointer_paths() {
        let rwlock = AsyncRwLock::new(0);

        assert_eq!(AsyncLock::try_read(&rwlock, read_i32), Ok(0));
        assert_eq!(AsyncLock::try_write(&rwlock, increment_i32), Ok(1));

        let write_guard = rwlock
            .try_write()
            .expect("failed to acquire initial write guard");
        assert_eq!(
            AsyncLock::try_read(&rwlock, read_i32),
            Err(TryLockError::WouldBlock),
        );
        drop(write_guard);

        let read_guard = rwlock
            .try_read()
            .expect("failed to acquire initial read guard");
        assert_eq!(
            AsyncLock::try_write(&rwlock, increment_i32),
            Err(TryLockError::WouldBlock),
        );
        drop(read_guard);
    }
}
