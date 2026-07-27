// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Borrowed exclusive-mode lock adapter.

use crate::lock::{Lock, ReadWriteLock, TryLockError};

/// Adapts the write mode of a ReadWriteLock to Lock.
#[must_use = "use the adapter to acquire a write guard"]
pub struct WriteLock<'a, L: ?Sized> {
    /// Underlying read-write lock.
    lock: &'a L,
}

impl<'a, L> WriteLock<'a, L>
where
    L: ReadWriteLock + ?Sized,
{
    /// Creates a borrowed write-mode adapter.
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

impl<L> Lock for WriteLock<'_, L>
where
    L: ReadWriteLock + ?Sized,
{
    type Guard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    /// Acquires an exclusive write guard.
    #[inline(always)]
    fn lock(&self) -> Self::Guard<'_> {
        self.lock.write()
    }

    /// Attempts to acquire an exclusive write guard without blocking.
    #[inline(always)]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        self.lock.try_write()
    }
}
