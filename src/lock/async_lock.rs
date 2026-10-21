// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Data-independent asynchronous RAII lock capability.
//!
//! Tokio mutexes intentionally implement only the asynchronous capability.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

use crate::lock::TryLockError;

/// Represents one asynchronous lock-acquisition mode.
///
/// Futures returned by this crate's implementations are lazy: constructing
/// one does not acquire the lock. Dropping a pending acquisition future
/// cancels that attempt and does not produce a guard. Once acquisition
/// resolves, the returned guard owns the lock until the guard is dropped.
///
/// # Examples
///
/// ```rust
/// use qubit_lock::AsyncLock;
/// use tokio::sync::Mutex;
///
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
/// runtime.block_on(async {
///     let lock = Mutex::new(1);
///     {
///         let mut guard = lock.lock().await;
///         *guard += 1;
///     }
///     assert_eq!(*lock.try_lock().unwrap(), 2);
/// });
/// ```
pub trait AsyncLock: Send + Sync {
    /// RAII guard returned by this asynchronous lock.
    type Guard<'a>: 'a
    where
        Self: 'a;

    /// Asynchronously acquires the lock.
    ///
    /// # Returns
    ///
    /// A future resolving to a guard that releases the lock when dropped.
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send;

    /// Attempts to acquire the lock without waiting.
    ///
    /// # Returns
    ///
    /// A guard when acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns TryLockError::WouldBlock when the lock is unavailable.
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError>;
}

impl<L> AsyncLock for &L
where
    L: AsyncLock + ?Sized,
{
    type Guard<'a>
        = L::Guard<'a>
    where
        Self: 'a;

    /// Delegates asynchronous acquisition to the borrowed lock.
    #[inline]
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send {
        L::lock(*self)
    }

    /// Delegates immediate acquisition to the borrowed lock.
    #[inline]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        L::try_lock(*self)
    }
}

impl<L> AsyncLock for Arc<L>
where
    L: AsyncLock + ?Sized,
{
    type Guard<'a>
        = L::Guard<'a>
    where
        Self: 'a;

    /// Delegates asynchronous acquisition to the shared lock.
    #[inline]
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send {
        L::lock(self.as_ref())
    }

    /// Delegates immediate acquisition to the shared lock.
    #[inline]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        L::try_lock(self.as_ref())
    }
}

impl<T> AsyncLock for Mutex<T>
where
    T: Send + ?Sized,
{
    type Guard<'a>
        = MutexGuard<'a, T>
    where
        Self: 'a;

    /// Acquires the Tokio mutex without blocking the thread.
    #[inline]
    async fn lock(&self) -> Self::Guard<'_> {
        Mutex::lock(self).await
    }

    /// Attempts to acquire the Tokio mutex without waiting.
    #[inline]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        Mutex::try_lock(self).map_err(|_| TryLockError::WouldBlock)
    }
}
