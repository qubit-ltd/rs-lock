/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Standard Read-Write Lock Wrapper
//!
//! Provides an Arc-wrapped standard-library read-write lock for callers that
//! need `std::sync::RwLock` poisoning semantics.
//!

use std::ops::Deref;
use std::sync::{
    Arc,
    RwLock,
};

use crate::lock::{
    Lock,
    TryLockError,
};

/// Standard-library read-write lock wrapper.
///
/// Provides an Arc-wrapped [`std::sync::RwLock`] for synchronous shared state.
/// Read operations can execute concurrently, while write operations have
/// exclusive access. Unlike [`ArcRwLock`](crate::ArcRwLock), this type
/// preserves standard-library poison behavior.
///
/// # Type Parameters
///
/// * `T` - The type protected by this lock.
pub struct ArcStdRwLock<T> {
    /// Shared standard read-write lock protecting the wrapped value.
    inner: Arc<RwLock<T>>,
}

impl<T> ArcStdRwLock<T> {
    /// Creates a new standard read-write lock.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected.
    ///
    /// # Returns
    ///
    /// A new [`ArcStdRwLock`] protecting `data`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::ArcStdRwLock;
    ///
    /// let lock = ArcStdRwLock::new(vec![1, 2, 3]);
    /// ```
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(data)),
        }
    }
}

impl<T> AsRef<RwLock<T>> for ArcStdRwLock<T> {
    /// Returns a reference to the underlying standard read-write lock.
    ///
    /// This is useful when callers need guard-based APIs such as
    /// [`RwLock::read`] or [`RwLock::write`] instead of the closure-based
    /// [`Lock`] methods.
    #[inline]
    fn as_ref(&self) -> &RwLock<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcStdRwLock<T> {
    type Target = RwLock<T>;

    /// Dereferences this wrapper to the underlying standard read-write lock.
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

impl<T> Lock<T> for ArcStdRwLock<T> {
    /// Acquires a shared read lock and executes a closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access to the protected value.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Panics if the underlying standard read-write lock is poisoned.
    #[inline]
    fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.inner.read().unwrap();
        f(&*guard)
    }

    /// Acquires an exclusive write lock and executes a closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access to the protected value.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Panics if the underlying standard read-write lock is poisoned.
    #[inline]
    fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.inner.write().unwrap();
        f(&mut *guard)
    }

    /// Attempts to acquire a shared read lock without blocking.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access when a read lock is acquired.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if a read lock is acquired.
    ///
    /// # Errors
    ///
    /// Returns [`TryLockError::WouldBlock`] when the lock is unavailable, or
    /// [`TryLockError::Poisoned`] when the lock is poisoned.
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

    /// Attempts to acquire an exclusive write lock without blocking.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access when a write lock is acquired.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if a write lock is acquired.
    ///
    /// # Errors
    ///
    /// Returns [`TryLockError::WouldBlock`] when the lock is unavailable, or
    /// [`TryLockError::Poisoned`] when the lock is poisoned.
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

impl<T> From<T> for ArcStdRwLock<T> {
    /// Creates an Arc-wrapped standard read-write lock from a value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to protect.
    ///
    /// # Returns
    ///
    /// A new [`ArcStdRwLock`] protecting `value`.
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcStdRwLock<T> {
    /// Creates an Arc-wrapped standard read-write lock containing
    /// `T::default()`.
    ///
    /// # Returns
    ///
    /// A new [`ArcStdRwLock`] protecting the default value for `T`.
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Clone for ArcStdRwLock<T> {
    /// Clones this standard read-write lock handle.
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
