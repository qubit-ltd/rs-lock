/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Asynchronous Read-Write Lock Wrapper
//!
//! Provides an Arc-wrapped asynchronous read-write lock for
//! protecting shared data with multiple concurrent readers or a
//! single writer in async environments.
//!
use std::{
    ops::Deref,
    sync::Arc,
};

use tokio::sync::RwLock as AsyncRwLock;

use crate::lock::{
    AsyncLock,
    TryLockError,
};

/// Asynchronous Read-Write Lock Wrapper
///
/// Provides an encapsulation of asynchronous read-write lock,
/// supporting multiple read operations or a single write operation.
/// Read operations can execute concurrently, while write operations
/// have exclusive access.
///
/// # Features
///
/// - Supports multiple concurrent read operations
/// - Write operations have exclusive access, mutually exclusive with
///   read operations
/// - Asynchronously acquires locks, does not block threads
/// - Thread-safe, supports multi-threaded sharing
/// - Automatic lock management through RAII ensures proper lock
///   release
/// - Implements [`Deref`] and [`AsRef`] to expose the underlying
///   [`tokio::sync::RwLock`] API when guard-based access is needed
///
/// # Use Cases
///
/// Suitable for read-heavy scenarios such as caching, configuration
/// management, etc.
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::lock::{ArcAsyncRwLock, AsyncLock};
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
/// rt.block_on(async {
///     let data = ArcAsyncRwLock::new(String::from("Hello"));
///
///     // Multiple read operations can execute concurrently
///     data.read(|s| {
///         println!("Read: {}", s);
///     }).await;
///
///     // Write operations have exclusive access
///     data.write(|s| {
///         s.push_str(" World!");
///         println!("Write: {}", s);
///     }).await;
/// });
/// ```
///
///
pub struct ArcAsyncRwLock<T> {
    /// Shared Tokio read-write lock protecting the wrapped value.
    inner: Arc<AsyncRwLock<T>>,
}

impl<T> ArcAsyncRwLock<T> {
    /// Creates a new asynchronous read-write lock
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected
    ///
    /// # Returns
    ///
    /// Returns a new `ArcAsyncRwLock` instance
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::ArcAsyncRwLock;
    ///
    /// let rw_lock = ArcAsyncRwLock::new(vec![1, 2, 3]);
    /// ```
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(AsyncRwLock::new(data)),
        }
    }
}

impl<T> AsRef<AsyncRwLock<T>> for ArcAsyncRwLock<T> {
    /// Returns a reference to the underlying Tokio read-write lock.
    ///
    /// This is useful when callers need guard-based APIs such as
    /// [`AsyncRwLock::read`] or [`AsyncRwLock::write`] instead of the
    /// closure-based [`AsyncLock`] methods.
    #[inline]
    fn as_ref(&self) -> &AsyncRwLock<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcAsyncRwLock<T> {
    type Target = AsyncRwLock<T>;

    /// Dereferences this wrapper to the underlying Tokio read-write lock.
    ///
    /// When [`AsyncLock`] is in scope, `read` and `write` with closure
    /// arguments still call the trait methods on this wrapper. Use explicit
    /// dereferencing or [`AsRef::as_ref`] when you want the native guard-based
    /// [`AsyncRwLock`] methods.
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> AsyncLock<T> for ArcAsyncRwLock<T>
where
    T: Send + Sync,
{
    /// Acquires the read lock and executes an operation
    ///
    /// Asynchronously acquires the read lock, executes the provided
    /// closure, and then automatically releases the lock. Multiple
    /// read operations can execute concurrently.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the read
    ///   lock, can only read data
    ///
    /// # Returns
    ///
    /// Returns a future that resolves to the result of executing
    /// the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcAsyncRwLock, AsyncLock};
    ///
    /// let rt = tokio::runtime::Builder::new_current_thread()
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    /// rt.block_on(async {
    ///     let data = ArcAsyncRwLock::new(vec![1, 2, 3]);
    ///
    ///     let length = data.read(|v| v.len()).await;
    ///     println!("Vector length: {}", length);
    /// });
    /// ```
    #[inline]
    async fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R + Send,
        R: Send,
    {
        let guard = self.inner.read().await;
        f(&*guard)
    }

    /// Acquires the write lock and executes an operation
    ///
    /// Asynchronously acquires the write lock, executes the provided
    /// closure, and then automatically releases the lock. Write
    /// operations have exclusive access, mutually exclusive with
    /// read operations.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the write
    ///   lock, can modify data
    ///
    /// # Returns
    ///
    /// Returns a future that resolves to the result of executing
    /// the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcAsyncRwLock, AsyncLock};
    ///
    /// let rt = tokio::runtime::Builder::new_current_thread()
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    /// rt.block_on(async {
    ///     let data = ArcAsyncRwLock::new(vec![1, 2, 3]);
    ///
    ///     data.write(|v| {
    ///         v.push(4);
    ///         println!("Added element, new length: {}", v.len());
    ///     }).await;
    /// });
    /// ```
    #[inline]
    async fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        let mut guard = self.inner.write().await;
        f(&mut *guard)
    }

    /// Attempts to acquire the read lock without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access when a read lock is
    ///   available.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if a read lock was acquired, or
    /// [`TryLockError::WouldBlock`] if the lock was busy.
    #[inline]
    fn try_read<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&T) -> R,
    {
        self.inner
            .try_read()
            .map(|guard| f(&*guard))
            .map_err(|_| TryLockError::WouldBlock)
    }

    /// Attempts to acquire the write lock without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access when a write lock is available.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if a write lock was acquired, or
    /// [`TryLockError::WouldBlock`] if the lock was busy.
    #[inline]
    fn try_write<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner
            .try_write()
            .map(|mut guard| f(&mut *guard))
            .map_err(|_| TryLockError::WouldBlock)
    }
}

impl<T> From<T> for ArcAsyncRwLock<T> {
    /// Creates an Arc-wrapped Tokio read-write lock from a value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to protect.
    ///
    /// # Returns
    ///
    /// A new [`ArcAsyncRwLock`] protecting `value`.
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcAsyncRwLock<T> {
    /// Creates an Arc-wrapped Tokio read-write lock containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A new [`ArcAsyncRwLock`] protecting the default value for `T`.
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Clone for ArcAsyncRwLock<T> {
    /// Clones the asynchronous read-write lock
    ///
    /// Creates a new `ArcAsyncRwLock` instance that shares the same
    /// underlying lock with the original instance. This allows
    /// multiple tasks to hold references to the same lock
    /// simultaneously.
    ///
    /// # Returns
    ///
    /// A new handle sharing the same underlying async read-write lock and
    /// protected value.
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
