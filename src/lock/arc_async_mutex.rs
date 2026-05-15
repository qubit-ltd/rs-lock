/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Asynchronous Mutex Wrapper
//!
//! Provides an Arc-wrapped asynchronous mutex for protecting shared
//! data in async environments without blocking threads.
//!
use std::{
    ops::Deref,
    sync::Arc,
};

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
/// - Implements [`AsyncLock`] when the protected value is `Send`
/// - Implements [`Deref`] and [`AsRef`] to expose the underlying
///   [`tokio::sync::Mutex`] API when guard-based access is needed
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::{ArcAsyncMutex, AsyncLock};
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
    /// use qubit_lock::ArcAsyncMutex;
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

impl<T> AsRef<AsyncMutex<T>> for ArcAsyncMutex<T> {
    /// Returns a reference to the underlying Tokio mutex.
    ///
    /// This is useful when callers need guard-based APIs such as
    /// [`AsyncMutex::lock`] or [`AsyncMutex::try_lock`] instead of the
    /// closure-based [`AsyncLock`] methods.
    #[inline]
    fn as_ref(&self) -> &AsyncMutex<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcAsyncMutex<T> {
    type Target = AsyncMutex<T>;

    /// Dereferences this wrapper to the underlying Tokio mutex.
    ///
    /// Method-call dereferencing lets callers use native async mutex APIs
    /// directly, while the wrapper continues to provide the [`AsyncLock`] trait
    /// methods.
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
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

impl<T> From<T> for ArcAsyncMutex<T> {
    /// Creates an Arc-wrapped Tokio mutex from a value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to protect.
    ///
    /// # Returns
    ///
    /// A new [`ArcAsyncMutex`] protecting `value`.
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcAsyncMutex<T> {
    /// Creates an Arc-wrapped Tokio mutex containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A new [`ArcAsyncMutex`] protecting the default value for `T`.
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
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
