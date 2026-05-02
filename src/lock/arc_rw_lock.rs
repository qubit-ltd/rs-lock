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
//! Provides an Arc-wrapped synchronous read-write lock for protecting
//! shared data with multiple concurrent readers or a single writer.
//!

use std::sync::{
    Arc,
    RwLock,
};

use crate::lock::{
    Lock,
    TryLockError,
};

/// Synchronous Read-Write Lock Wrapper
///
/// Provides an encapsulation of synchronous read-write lock,
/// supporting multiple read operations or a single write operation.
/// Read operations can execute concurrently, while write operations
/// have exclusive access.
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
///
/// # Use Cases
///
/// Suitable for read-heavy scenarios such as caching, configuration
/// management, etc.
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::lock::{ArcRwLock, Lock};
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
    /// Shared standard read-write lock protecting the wrapped value.
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
    /// use qubit_lock::lock::ArcRwLock;
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
    /// # Panics
    ///
    /// Panics if the underlying standard read-write lock is poisoned.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcRwLock, Lock};
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
        let guard = self.inner.read().unwrap();
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
    /// # Panics
    ///
    /// Panics if the underlying standard read-write lock is poisoned.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcRwLock, Lock};
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
        let mut guard = self.inner.write().unwrap();
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
    /// * `Err(TryLockError::Poisoned)` - If the lock is poisoned
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcRwLock, Lock};
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
        match self.inner.try_read() {
            Ok(guard) => Ok(f(&*guard)),
            Err(std::sync::TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(TryLockError::Poisoned),
        }
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
    /// * `Err(TryLockError::WouldBlock)` - If the lock is currently held by another thread
    /// * `Err(TryLockError::Poisoned)` - If the lock is poisoned
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcRwLock, Lock};
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
        match self.inner.try_write() {
            Ok(mut guard) => Ok(f(&mut *guard)),
            Err(std::sync::TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(TryLockError::Poisoned),
        }
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
