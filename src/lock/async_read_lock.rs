// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Borrowed asynchronous shared-mode adapter.

use std::future::Future;

use crate::lock::{
    AsyncLock,
    AsyncReadWriteLock,
    TryLockError,
};

/// Adapts the read mode of an AsyncReadWriteLock to AsyncLock.
///
/// # Type Parameters
///
/// * `L` - The underlying asynchronous read-write lock type.
#[must_use = "use the adapter to acquire an asynchronous read guard"]
pub struct AsyncReadLock<'a, L: ?Sized> {
    /// Underlying asynchronous read-write lock.
    lock: &'a L,
}

impl<'a, L> AsyncReadLock<'a, L>
where
    L: AsyncReadWriteLock + ?Sized,
{
    /// Creates a borrowed asynchronous read-mode adapter.
    ///
    /// # Parameters
    ///
    /// * `lock` - Underlying asynchronous read-write lock.
    ///
    /// # Returns
    ///
    /// An adapter borrowing `lock`.
    #[inline(always)]
    pub(crate) const fn new(lock: &'a L) -> Self {
        Self { lock }
    }
}

impl<L> AsyncLock for AsyncReadLock<'_, L>
where
    L: AsyncReadWriteLock + ?Sized,
{
    type Guard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;

    /// Acquires a shared asynchronous read guard.
    #[inline(always)]
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send {
        self.lock.read()
    }

    /// Attempts to acquire a shared read guard without waiting.
    #[inline(always)]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        self.lock.try_read()
    }
}
