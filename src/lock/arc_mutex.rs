/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

//! # Synchronous Mutex Wrapper (Parking Lot)
//!
//! Provides an Arc-wrapped synchronous mutex using parking_lot::Mutex
//! for protecting shared data in multi-threaded environments.
//!

use std::{
    ops::Deref,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::lock::{
    Lock,
    TryLockError,
};

/// Synchronous Mutex Wrapper (Parking Lot)
///
/// Provides an encapsulation of synchronous mutex using parking_lot::Mutex
/// for protecting shared data in synchronous environments. Supports safe
/// access and modification of shared data across multiple threads.
/// Compared to std::sync::Mutex, parking_lot::Mutex provides better
/// performance and more ergonomic API.
///
/// # Features
///
/// - Synchronously acquires locks, may block threads
/// - Supports trying to acquire locks (non-blocking)
/// - Thread-safe, supports multi-threaded sharing
/// - Automatic lock management through RAII ensures proper lock
///   release
/// - Better performance compared to std::sync::Mutex
/// - More ergonomic API with no unwrap() calls
/// - Implements [`Deref`] and [`AsRef`] to expose the underlying
///   [`parking_lot::Mutex`] API when guard-based access is needed
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::lock::{ArcMutex, Lock};
///
/// let counter = ArcMutex::new(0);
///
/// // Synchronously modify data
/// counter.write(|c| {
///     *c += 1;
///     println!("Counter: {}", *c);
/// });
///
/// // Try to acquire lock
/// if let Ok(value) = counter.try_read(|c| *c) {
///     println!("Current value: {}", value);
/// }
/// ```
///
///
pub struct ArcMutex<T> {
    /// Shared parking_lot mutex protecting the wrapped value.
    inner: Arc<Mutex<T>>,
}

impl<T> ArcMutex<T> {
    /// Creates a new synchronous mutex lock
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected
    ///
    /// # Returns
    ///
    /// Returns a new `ArcMutex` instance
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::ArcMutex;
    ///
    /// let lock = ArcMutex::new(42);
    /// ```
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(data)),
        }
    }
}

impl<T> AsRef<Mutex<T>> for ArcMutex<T> {
    /// Returns a reference to the underlying parking_lot mutex.
    ///
    /// This is useful when callers need guard-based APIs such as
    /// [`Mutex::lock`] or [`Mutex::try_lock`] instead of the closure-based
    /// [`Lock`] methods.
    #[inline]
    fn as_ref(&self) -> &Mutex<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcMutex<T> {
    type Target = Mutex<T>;

    /// Dereferences this wrapper to the underlying parking_lot mutex.
    ///
    /// Method-call dereferencing lets callers use native mutex APIs directly,
    /// while the wrapper continues to provide the [`Lock`] trait methods.
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> Lock<T> for ArcMutex<T> {
    /// Acquires a read lock and executes an operation
    ///
    /// For ArcMutex, this acquires the same exclusive lock as write
    /// operations, but provides immutable access to the data. This
    /// ensures thread safety while allowing read-only operations.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the read lock
    ///
    /// # Returns
    ///
    /// Returns the result of executing the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcMutex, Lock};
    ///
    /// let counter = ArcMutex::new(42);
    ///
    /// let value = counter.read(|c| *c);
    /// println!("Current value: {}", value);
    /// ```
    #[inline]
    fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.inner.lock();
        f(&*guard)
    }

    /// Acquires a write lock and executes an operation
    ///
    /// Synchronously acquires the exclusive lock, executes the provided
    /// closure with mutable access, and then automatically releases
    /// the lock. This is the recommended usage pattern for modifications.
    ///
    /// # Arguments
    ///
    /// * `f` - The closure to be executed while holding the write lock
    ///
    /// # Returns
    ///
    /// Returns the result of executing the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcMutex, Lock};
    ///
    /// let counter = ArcMutex::new(0);
    ///
    /// let result = counter.write(|c| {
    ///     *c += 1;
    ///     *c
    /// });
    ///
    /// println!("Counter value: {}", result);
    /// ```
    #[inline]
    fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.inner.lock();
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
    /// * `Err(TryLockError::WouldBlock)` - If the lock is already held by another thread
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcMutex, Lock};
    ///
    /// let counter = ArcMutex::new(42);
    ///
    /// if let Ok(value) = counter.try_read(|c| *c) {
    ///     println!("Current value: {}", value);
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
            .try_lock()
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
    /// * `Err(TryLockError::WouldBlock)` - If the lock is already held by another thread
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcMutex, Lock};
    ///
    /// let counter = ArcMutex::new(0);
    ///
    /// if let Ok(result) = counter.try_write(|c| {
    ///     *c += 1;
    ///     *c
    /// }) {
    ///     println!("New value: {}", result);
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
            .try_lock()
            .map(|mut guard| f(&mut *guard))
            .ok_or(TryLockError::WouldBlock)
    }
}

impl<T> Clone for ArcMutex<T> {
    /// Clones the synchronous mutex
    ///
    /// Creates a new `ArcMutex` instance that shares the same
    /// underlying lock with the original instance. This allows
    /// multiple threads to hold references to the same lock
    /// simultaneously.
    ///
    /// # Returns
    ///
    /// A new handle sharing the same underlying mutex and protected value.
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
