// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous RAII read-write lock capability.

use std::{
    future::Future,
    sync::Arc,
};

use tokio::sync::{
    RwLock,
    RwLockReadGuard,
    RwLockWriteGuard,
};

use crate::lock::{
    AsyncReadLock,
    AsyncWriteLock,
    TryLockError,
};

/// Represents an asynchronous lock with shared and exclusive modes.
pub trait AsyncReadWriteLock: Send + Sync {
    /// Shared asynchronous read guard.
    type ReadGuard<'a>: 'a
    where
        Self: 'a;

    /// Exclusive asynchronous write guard.
    type WriteGuard<'a>: 'a
    where
        Self: 'a;

    /// Asynchronously acquires a shared read guard.
    ///
    /// # Returns
    ///
    /// A future resolving to a read guard.
    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + Send;

    /// Asynchronously acquires an exclusive write guard.
    ///
    /// # Returns
    ///
    /// A future resolving to a write guard.
    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send;

    /// Attempts to acquire a shared read guard without waiting.
    ///
    /// # Returns
    ///
    /// A read guard when acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns TryLockError::WouldBlock when unavailable.
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError>;

    /// Attempts to acquire an exclusive write guard without waiting.
    ///
    /// # Returns
    ///
    /// A write guard when acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns TryLockError::WouldBlock when unavailable.
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError>;

    /// Returns a borrowed asynchronous read-mode adapter.
    ///
    /// # Returns
    ///
    /// A zero-allocation adapter borrowing this lock.
    #[inline(always)]
    fn read_lock(&self) -> AsyncReadLock<'_, Self> {
        AsyncReadLock::new(self)
    }

    /// Returns a borrowed asynchronous write-mode adapter.
    ///
    /// # Returns
    ///
    /// A zero-allocation adapter borrowing this lock.
    #[inline(always)]
    fn write_lock(&self) -> AsyncWriteLock<'_, Self> {
        AsyncWriteLock::new(self)
    }
}

impl<L> AsyncReadWriteLock for &L
where
    L: AsyncReadWriteLock + ?Sized,
{
    type ReadGuard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;
    type WriteGuard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    /// Delegates asynchronous shared acquisition.
    #[inline(always)]
    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + Send {
        L::read(*self)
    }

    /// Delegates asynchronous exclusive acquisition.
    #[inline(always)]
    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send {
        L::write(*self)
    }

    /// Delegates immediate shared acquisition.
    #[inline(always)]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        L::try_read(*self)
    }

    /// Delegates immediate exclusive acquisition.
    #[inline(always)]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        L::try_write(*self)
    }
}

impl<L> AsyncReadWriteLock for Arc<L>
where
    L: AsyncReadWriteLock + ?Sized,
{
    type ReadGuard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;
    type WriteGuard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    /// Delegates asynchronous shared acquisition.
    #[inline(always)]
    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + Send {
        L::read(self.as_ref())
    }

    /// Delegates asynchronous exclusive acquisition.
    #[inline(always)]
    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send {
        L::write(self.as_ref())
    }

    /// Delegates immediate shared acquisition.
    #[inline(always)]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        L::try_read(self.as_ref())
    }

    /// Delegates immediate exclusive acquisition.
    #[inline(always)]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        L::try_write(self.as_ref())
    }
}

impl<T> AsyncReadWriteLock for RwLock<T>
where
    T: Send + Sync + ?Sized,
{
    type ReadGuard<'a>
        = RwLockReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a>
        = RwLockWriteGuard<'a, T>
    where
        Self: 'a;

    /// Acquires a Tokio read guard without blocking the thread.
    #[inline]
    async fn read(&self) -> Self::ReadGuard<'_> {
        RwLock::read(self).await
    }

    /// Acquires a Tokio write guard without blocking the thread.
    #[inline]
    async fn write(&self) -> Self::WriteGuard<'_> {
        RwLock::write(self).await
    }

    /// Attempts to acquire a Tokio read guard without waiting.
    #[inline]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        RwLock::try_read(self).map_err(|_| TryLockError::WouldBlock)
    }

    /// Attempts to acquire a Tokio write guard without waiting.
    #[inline]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        RwLock::try_write(self).map_err(|_| TryLockError::WouldBlock)
    }
}
