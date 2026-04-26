/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Asynchronous Mutex Wrapper
//!
//! Provides an Arc-wrapped asynchronous mutex for protecting shared
//! data in async environments without blocking threads.
//!
//! # Author
//!
//! Haixing Hu
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use crate::lock::{
    AsyncLock,
    TryLockError,
};

/// Asynchronous Mutex Wrapper
///
/// Provides an encapsulation of asynchronous mutex for protecting
/// shared data in asynchronous environments. Supports safe access
/// and modification of shared data across multiple asynchronous
/// tasks.
///
/// # Features
///
/// - Asynchronously acquires locks, does not block threads
/// - Supports trying to acquire locks (non-blocking)
/// - Thread-safe, supports multi-threaded sharing
/// - Automatic lock management through RAII ensures proper lock
///   release
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::lock::{ArcAsyncMutex, AsyncLock};
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
/// rt.block_on(async {
///     let counter = ArcAsyncMutex::new(0);
///
///     // Asynchronously modify data
///     counter.write(|c| {
///         *c += 1;
///         println!("Counter: {}", *c);
///     }).await;
///
///     // Try to acquire lock
///     if let Ok(value) = counter.try_read(|c| *c) {
///         println!("Current value: {}", value);
///     }
/// });
/// ```
///
/// # Author
///
/// Haixing Hu
///
pub struct ArcAsyncMutex<T> {
    /// Shared Tokio mutex protecting the wrapped value.
    inner: Arc<AsyncMutex<T>>,
}

impl<T> ArcAsyncMutex<T> {
    /// Creates a new asynchronous mutex lock
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected
    ///
    /// # Returns
    ///
    /// Returns a new `ArcAsyncMutex` instance
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::ArcAsyncMutex;
    ///
    /// let lock = ArcAsyncMutex::new(42);
    /// ```
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(data)),
        }
    }
}

impl<T> AsyncLock<T> for ArcAsyncMutex<T>
where
    T: Send,
{
    /// Acquires the mutex and executes a read-only operation.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access to the protected value.
    ///
    /// # Returns
    ///
    /// A future resolving to the closure result.
    #[inline]
    async fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R + Send,
        R: Send,
    {
        let guard = self.inner.lock().await;
        f(&*guard)
    }

    /// Acquires the mutex and executes a mutable operation.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access to the protected value.
    ///
    /// # Returns
    ///
    /// A future resolving to the closure result.
    #[inline]
    async fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        let mut guard = self.inner.lock().await;
        f(&mut *guard)
    }

    /// Attempts to acquire the mutex for a read-only operation without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access when the mutex is available.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if the mutex was acquired, or
    /// [`TryLockError::WouldBlock`] if it was busy.
    #[inline]
    fn try_read<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&T) -> R,
    {
        self.inner
            .try_lock()
            .map(|guard| f(&*guard))
            .map_err(|_| TryLockError::WouldBlock)
    }

    /// Attempts to acquire the mutex for a mutable operation without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access when the mutex is available.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if the mutex was acquired, or
    /// [`TryLockError::WouldBlock`] if it was busy.
    #[inline]
    fn try_write<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner
            .try_lock()
            .map(|mut guard| f(&mut *guard))
            .map_err(|_| TryLockError::WouldBlock)
    }
}

impl<T> Clone for ArcAsyncMutex<T> {
    /// Clones the asynchronous mutex
    ///
    /// Creates a new `ArcAsyncMutex` instance that shares the same
    /// underlying lock with the original instance. This allows
    /// multiple tasks to hold references to the same lock
    /// simultaneously.
    ///
    /// # Returns
    ///
    /// A new handle sharing the same underlying async mutex and protected
    /// value.
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
