// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Borrowed shared-mode lock adapter.

use crate::lock::{
    Lock,
    ReadWriteLock,
    TryLockError,
};

/// Adapts the read mode of a ReadWriteLock to Lock.
///
/// Multiple guards from this adapter may coexist. It must not be used where
/// the consumer requires exclusive entry.
#[must_use = "use the adapter to acquire a read guard"]
pub struct ReadLock<'a, L: ?Sized> {
    /// Underlying read-write lock.
    lock: &'a L,
}

impl<'a, L> ReadLock<'a, L>
where
    L: ReadWriteLock + ?Sized,
{
    /// Creates a borrowed read-mode adapter.
    ///
    /// # Parameters
    ///
    /// * `lock` - Underlying read-write lock.
    ///
    /// # Returns
    ///
    /// An adapter borrowing `lock`.
    #[inline(always)]
    pub(crate) const fn new(lock: &'a L) -> Self {
        Self { lock }
    }
}

impl<L> Lock for ReadLock<'_, L>
where
    L: ReadWriteLock + ?Sized,
{
    type Guard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;

    /// Acquires a shared read guard.
    #[inline(always)]
    fn lock(&self) -> Self::Guard<'_> {
        self.lock.read()
    }

    /// Attempts to acquire a shared read guard without blocking.
    #[inline(always)]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        self.lock.try_read()
    }
}
