/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Synchronous Read-Write Lock Wrapper
//!
//! Provides an Arc-wrapped parking_lot read-write lock for protecting
//! shared data with multiple concurrent readers or a single writer.
//!

use std::ops::Deref;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::lock::{Lock, TryLockError};

/// Parking-lot read-write lock wrapper.
///
/// Provides an Arc-wrapped [`parking_lot::RwLock`] for synchronous shared state.
/// Read operations can execute concurrently, while write operations have
/// exclusive access.
///
/// # Features
///
/// - Supports multiple concurrent read operations
/// - Write operations have exclusive access, mutually exclusive with
///   read operations
/// - Synchronously acquires locks, may block threads
/// - Thread-safe, supports multi-threaded sharing
/// - Automatic lock management through RAII ensures proper lock
///   release
/// - Does not use lock poisoning; panic while holding the lock does not make
///   future acquisitions fail
/// - Implements [`Deref`] and [`AsRef`] to expose the underlying
///   [`parking_lot::RwLock`] API when guard-based access is needed
///
/// # Use Cases
///
/// Suitable for read-heavy scenarios such as caching, configuration
/// management, etc.
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::{ArcRwLock, Lock};
///
/// let data = ArcRwLock::new(String::from("Hello"));
///
/// // Multiple read operations can execute concurrently
/// data.read(|s| {
///     println!("Read: {}", s);
/// });
///
/// // Write operations have exclusive access
/// data.write(|s| {
///     s.push_str(" World!");
///     println!("Write: {}", s);
/// });
/// ```
///
///
pub struct ArcRwLock<T> {
    /// Shared parking_lot read-write lock protecting the wrapped value.
    inner: Arc<RwLock<T>>,
}

impl<T> ArcRwLock<T> {
    /// Creates a new synchronous read-write lock
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected
    ///
    /// # Returns
    ///
    /// Returns a new `ArcRwLock` instance
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::ArcRwLock;
    ///
    /// let rw_lock = ArcRwLock::new(vec![1, 2, 3]);
    /// ```
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(data)),
        }
    }
}

impl<T> AsRef<RwLock<T>> for ArcRwLock<T> {
    /// Returns a reference to the underlying parking_lot read-write lock.
    ///
    /// This is useful when callers need guard-based APIs such as
    /// [`RwLock::read`] or [`RwLock::write`] instead of the closure-based
    /// [`Lock`] methods.
    #[inline]
    fn as_ref(&self) -> &RwLock<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcRwLock<T> {
    type Target = RwLock<T>;

    /// Dereferences this wrapper to the underlying parking_lot read-write lock.
    ///
    /// When [`Lock`] is in scope, `read` and `write` with closure arguments
    /// still call the trait methods on this wrapper. Use explicit
    /// dereferencing or [`AsRef::as_ref`] when you want the native guard-based
    /// [`RwLock`] methods.
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> Lock<T> for ArcRwLock<T> {
    /// Acquires a read lock and executes an operation
    ///
    /// Synchronously acquires the read lock, executes the provided
    /// closure, and then automatically releases the lock. Multiple
    /// read operations can execute concurrently, providing better
    /// performance for read-heavy workloads.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the read
    ///   lock, can only read data
    ///
    /// # Returns
    ///
    /// Returns the result of executing the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::{ArcRwLock, Lock};
    ///
    /// let data = ArcRwLock::new(vec![1, 2, 3]);
    ///
    /// let length = data.read(|v| v.len());
    /// println!("Vector length: {}", length);
    /// ```
    #[inline]
    fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.inner.read();
        f(&*guard)
    }

    /// Acquires a write lock and executes an operation
    ///
    /// Synchronously acquires the write lock, executes the provided
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
    /// Returns the result of executing the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::{ArcRwLock, Lock};
    ///
    /// let data = ArcRwLock::new(vec![1, 2, 3]);
    ///
    /// data.write(|v| {
    ///     v.push(4);
    ///     println!("Added element, new length: {}", v.len());
    /// });
    /// ```
    #[inline]
    fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.inner.write();
        f(&mut *guard)
    }

    /// Attempts to acquire a read lock without blocking
    ///
    /// Attempts to immediately acquire the read lock. If the lock is
    /// unavailable, returns a detailed error. This is a non-blocking operation.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the read lock
    ///
    /// # Returns
    ///
    /// * `Ok(R)` - If the lock was successfully acquired and the closure executed
    /// * `Err(TryLockError::WouldBlock)` - If the lock is currently held in write mode
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::{ArcRwLock, Lock};
    ///
    /// let data = ArcRwLock::new(vec![1, 2, 3]);
    ///
    /// if let Ok(length) = data.try_read(|v| v.len()) {
    ///     println!("Vector length: {}", length);
    /// } else {
    ///     println!("Lock is unavailable");
    /// }
    /// ```
    #[inline]
    fn try_read<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&T) -> R,
    {
        self.inner
            .try_read()
            .map(|guard| f(&*guard))
            .ok_or(TryLockError::WouldBlock)
    }

    /// Attempts to acquire a write lock without blocking
    ///
    /// Attempts to immediately acquire the write lock. If the lock is
    /// unavailable, returns a detailed error. This is a non-blocking operation.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the write lock
    ///
    /// # Returns
    ///
    /// * `Ok(R)` - If the lock was successfully acquired and the closure executed
    /// * `Err(TryLockError::WouldBlock)` - If the lock is unavailable
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::{ArcRwLock, Lock};
    ///
    /// let data = ArcRwLock::new(vec![1, 2, 3]);
    ///
    /// if let Ok(new_length) = data.try_write(|v| {
    ///     v.push(4);
    ///     v.len()
    /// }) {
    ///     println!("New length: {}", new_length);
    /// } else {
    ///     println!("Lock is unavailable");
    /// }
    /// ```
    #[inline]
    fn try_write<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner
            .try_write()
            .map(|mut guard| f(&mut *guard))
            .ok_or(TryLockError::WouldBlock)
    }
}

impl<T> From<T> for ArcRwLock<T> {
    /// Creates an Arc-wrapped parking_lot read-write lock from a value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to protect.
    ///
    /// # Returns
    ///
    /// A new [`ArcRwLock`] protecting `value`.
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcRwLock<T> {
    /// Creates an Arc-wrapped parking_lot read-write lock containing
    /// `T::default()`.
    ///
    /// # Returns
    ///
    /// A new [`ArcRwLock`] protecting the default value for `T`.
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Clone for ArcRwLock<T> {
    /// Clones the synchronous read-write lock
    ///
    /// Creates a new `ArcRwLock` instance that shares the same
    /// underlying lock with the original instance. This allows
    /// multiple threads to hold references to the same lock
    /// simultaneously.
    ///
    /// # Returns
    ///
    /// A new handle sharing the same underlying read-write lock and protected
    /// value.
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
