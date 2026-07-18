// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous RAII read-write lock capability.

use std::sync::{
    Arc,
    RwLock,
    RwLockReadGuard,
    RwLockWriteGuard,
};

use parking_lot::{
    RwLock as ParkingLotRwLock,
    RwLockReadGuard as ParkingLotRwLockReadGuard,
    RwLockWriteGuard as ParkingLotRwLockWriteGuard,
};

use crate::lock::{
    ReadLock,
    TryLockError,
    WriteLock,
};

/// Represents a synchronous lock with explicit shared and exclusive modes.
pub trait ReadWriteLock: Send + Sync {
    /// Shared read guard returned by this lock.
    type ReadGuard<'a>: 'a
    where
        Self: 'a;

    /// Exclusive write guard returned by this lock.
    type WriteGuard<'a>: 'a
    where
        Self: 'a;

    /// Acquires a shared read guard.
    ///
    /// # Returns
    ///
    /// A guard that releases the read mode when dropped.
    ///
    /// # Panics
    ///
    /// Standard-library implementations panic when poisoned.
    #[must_use = "dropping the guard immediately releases the read lock"]
    fn read(&self) -> Self::ReadGuard<'_>;

    /// Acquires an exclusive write guard.
    ///
    /// # Returns
    ///
    /// A guard that releases the write mode when dropped.
    ///
    /// # Panics
    ///
    /// Standard-library implementations panic when poisoned.
    #[must_use = "dropping the guard immediately releases the write lock"]
    fn write(&self) -> Self::WriteGuard<'_>;

    /// Attempts to acquire a shared read guard without blocking.
    ///
    /// # Returns
    ///
    /// A read guard when acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns the backend-independent contention or poisoning error.
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError>;

    /// Attempts to acquire an exclusive write guard without blocking.
    ///
    /// # Returns
    ///
    /// A write guard when acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns the backend-independent contention or poisoning error.
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError>;

    /// Returns a borrowed shared-mode ReadLock adapter.
    ///
    /// # Returns
    ///
    /// A zero-allocation adapter borrowing this lock.
    #[inline(always)]
    fn read_lock(&self) -> ReadLock<'_, Self> {
        ReadLock::new(self)
    }

    /// Returns a borrowed exclusive-mode WriteLock adapter.
    ///
    /// # Returns
    ///
    /// A zero-allocation adapter borrowing this lock.
    #[inline(always)]
    fn write_lock(&self) -> WriteLock<'_, Self> {
        WriteLock::new(self)
    }
}

impl<L> ReadWriteLock for &L
where
    L: ReadWriteLock + ?Sized,
{
    type ReadGuard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;
    type WriteGuard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    /// Delegates shared acquisition.
    #[inline(always)]
    fn read(&self) -> Self::ReadGuard<'_> {
        L::read(*self)
    }

    /// Delegates exclusive acquisition.
    #[inline(always)]
    fn write(&self) -> Self::WriteGuard<'_> {
        L::write(*self)
    }

    /// Delegates non-blocking shared acquisition.
    #[inline(always)]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        L::try_read(*self)
    }

    /// Delegates non-blocking exclusive acquisition.
    #[inline(always)]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        L::try_write(*self)
    }
}

impl<L> ReadWriteLock for Arc<L>
where
    L: ReadWriteLock + ?Sized,
{
    type ReadGuard<'a>
        = L::ReadGuard<'a>
    where
        Self: 'a;
    type WriteGuard<'a>
        = L::WriteGuard<'a>
    where
        Self: 'a;

    /// Delegates shared acquisition.
    #[inline(always)]
    fn read(&self) -> Self::ReadGuard<'_> {
        L::read(self.as_ref())
    }

    /// Delegates exclusive acquisition.
    #[inline(always)]
    fn write(&self) -> Self::WriteGuard<'_> {
        L::write(self.as_ref())
    }

    /// Delegates non-blocking shared acquisition.
    #[inline(always)]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        L::try_read(self.as_ref())
    }

    /// Delegates non-blocking exclusive acquisition.
    #[inline(always)]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        L::try_write(self.as_ref())
    }
}

impl<T> ReadWriteLock for RwLock<T>
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

    /// Acquires a standard-library read guard.
    ///
    /// # Panics
    ///
    /// Panics when the lock is poisoned.
    #[inline]
    fn read(&self) -> Self::ReadGuard<'_> {
        RwLock::read(self).unwrap()
    }

    /// Acquires a standard-library write guard.
    ///
    /// # Panics
    ///
    /// Panics when the lock is poisoned.
    #[inline]
    fn write(&self) -> Self::WriteGuard<'_> {
        RwLock::write(self).unwrap()
    }

    /// Attempts to acquire a standard-library read guard.
    #[inline]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        match RwLock::try_read(self) {
            Ok(guard) => Ok(guard),
            Err(std::sync::TryLockError::WouldBlock) => {
                Err(TryLockError::WouldBlock)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(TryLockError::Poisoned)
            }
        }
    }

    /// Attempts to acquire a standard-library write guard.
    #[inline]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        match RwLock::try_write(self) {
            Ok(guard) => Ok(guard),
            Err(std::sync::TryLockError::WouldBlock) => {
                Err(TryLockError::WouldBlock)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(TryLockError::Poisoned)
            }
        }
    }
}

impl<T> ReadWriteLock for ParkingLotRwLock<T>
where
    T: Send + Sync + ?Sized,
{
    type ReadGuard<'a>
        = ParkingLotRwLockReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a>
        = ParkingLotRwLockWriteGuard<'a, T>
    where
        Self: 'a;

    /// Acquires a non-poisoning parking_lot read guard.
    #[inline]
    fn read(&self) -> Self::ReadGuard<'_> {
        ParkingLotRwLock::read(self)
    }

    /// Acquires a non-poisoning parking_lot write guard.
    #[inline]
    fn write(&self) -> Self::WriteGuard<'_> {
        ParkingLotRwLock::write(self)
    }

    /// Attempts to acquire a parking_lot read guard.
    #[inline]
    fn try_read(&self) -> Result<Self::ReadGuard<'_>, TryLockError> {
        ParkingLotRwLock::try_read(self).ok_or(TryLockError::WouldBlock)
    }

    /// Attempts to acquire a parking_lot write guard.
    #[inline]
    fn try_write(&self) -> Result<Self::WriteGuard<'_>, TryLockError> {
        ParkingLotRwLock::try_write(self).ok_or(TryLockError::WouldBlock)
    }
}
