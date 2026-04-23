/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Synchronous Standard Mutex Wrapper
//!
//! Provides an Arc-wrapped synchronous mutex using std::sync::Mutex
//! for protecting shared data in multi-threaded environments.
//!
//! # Author
//!
//! Haixing Hu

use std::sync::{
    Arc,
    Mutex,
};

use crate::lock::{
    Lock,
    TryLockError,
};

/// Synchronous Standard Mutex Wrapper
///
/// Provides an encapsulation of synchronous mutex using std::sync::Mutex
/// for protecting shared data in synchronous environments. Supports safe
/// access and modification of shared data across multiple threads.
///
/// # Features
///
/// - Synchronously acquires locks, may block threads
/// - Supports trying to acquire locks (non-blocking)
/// - Thread-safe, supports multi-threaded sharing
/// - Automatic lock management through RAII ensures proper lock
///   release
///
/// # Usage Example
///
/// ```rust
/// use qubit_lock::lock::{ArcStdMutex, Lock};
/// use std::sync::Arc;
///
/// let counter = ArcStdMutex::new(0);
/// let counter = Arc::new(counter);
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
/// # Author
///
/// Haixing Hu
///
pub struct ArcStdMutex<T> {
    /// Shared standard mutex protecting the wrapped value.
    inner: Arc<Mutex<T>>,
}

impl<T> ArcStdMutex<T> {
    /// Creates a new synchronous mutex lock
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected
    ///
    /// # Returns
    ///
    /// Returns a new `ArcStdMutex` instance
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::ArcStdMutex;
    ///
    /// let lock = ArcStdMutex::new(42);
    /// ```
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(data)),
        }
    }
}

impl<T> Lock<T> for ArcStdMutex<T> {
    /// Acquires a read lock and executes an operation
    ///
    /// For ArcStdMutex, this acquires the same exclusive lock as write
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
    /// # Panics
    ///
    /// Panics if the underlying standard mutex is poisoned.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcStdMutex, Lock};
    ///
    /// let counter = ArcStdMutex::new(42);
    ///
    /// let value = counter.read(|c| *c);
    /// println!("Current value: {}", value);
    /// ```
    #[inline]
    fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.inner.lock().unwrap();
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
    /// # Panics
    ///
    /// Panics if the underlying standard mutex is poisoned.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcStdMutex, Lock};
    ///
    /// let counter = ArcStdMutex::new(0);
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
        let mut guard = self.inner.lock().unwrap();
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
    /// * `Err(TryLockError::Poisoned)` - If the lock is poisoned
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcStdMutex, Lock};
    ///
    /// let counter = ArcStdMutex::new(42);
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
        match self.inner.try_lock() {
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
    /// * `Err(TryLockError::WouldBlock)` - If the lock is already held by another thread
    /// * `Err(TryLockError::Poisoned)` - If the lock is poisoned
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{ArcStdMutex, Lock};
    ///
    /// let counter = ArcStdMutex::new(0);
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
        match self.inner.try_lock() {
            Ok(mut guard) => Ok(f(&mut *guard)),
            Err(std::sync::TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(_)) => Err(TryLockError::Poisoned),
        }
    }
}

impl<T> Clone for ArcStdMutex<T> {
    /// Clones the synchronous mutex
    ///
    /// Creates a new `ArcStdMutex` instance that shares the same
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
