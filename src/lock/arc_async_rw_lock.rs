/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Asynchronous Read-Write Lock Wrapper
//!
//! Provides an Arc-wrapped asynchronous read-write lock for
//! protecting shared data with multiple concurrent readers or a
//! single writer in async environments.
//!
//! # Author
//!
//! Haixing Hu
use std::sync::Arc;

use tokio::sync::RwLock as AsyncRwLock;

use crate::lock::AsyncLock;

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
/// # Author
///
/// Haixing Hu
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
    /// `Some(result)` if a read lock was acquired, or `None` if the lock was
    /// busy.
    #[inline]
    fn try_read<R, F>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        if let Ok(guard) = self.inner.try_read() {
            Some(f(&*guard))
        } else {
            None
        }
    }

    /// Attempts to acquire the write lock without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access when a write lock is available.
    ///
    /// # Returns
    ///
    /// `Some(result)` if a write lock was acquired, or `None` if the lock was
    /// busy.
    #[inline]
    fn try_write<R, F>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        if let Ok(mut guard) = self.inner.try_write() {
            Some(f(&mut *guard))
        } else {
            None
        }
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
