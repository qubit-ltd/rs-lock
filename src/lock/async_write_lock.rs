// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Borrowed asynchronous exclusive-mode adapter.

use std::future::Future;

use crate::lock::{AsyncLock, AsyncReadWriteLock, TryLockError};

/// Adapts the write mode of an AsyncReadWriteLock to AsyncLock.
///
/// ```compile_fail
/// #![deny(unused_must_use)]
///
/// use qubit_lock::AsyncReadWriteLock;
///
/// let lock = tokio::sync::RwLock::new(());
/// lock.write_lock();
/// ```
#[must_use = "use the adapter to acquire an asynchronous write guard"]
pub struct AsyncWriteLock<'a, L: ?Sized> {
    /// Underlying asynchronous read-write lock.
    lock: &'a L,
}

impl<'a, L> AsyncWriteLock<'a, L>
where
    L: AsyncReadWriteLock + ?Sized,
{
    /// Creates a borrowed asynchronous write-mode adapter.
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

impl<L> AsyncLock for AsyncWriteLock<'_, L>
where
    L: AsyncReadWriteLock + ?Sized,
{
    type Guard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    /// Acquires an exclusive asynchronous write guard.
    #[inline(always)]
    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send {
        self.lock.write()
    }

    /// Attempts to acquire an exclusive write guard without waiting.
    #[inline(always)]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        self.lock.try_write()
    }
}
