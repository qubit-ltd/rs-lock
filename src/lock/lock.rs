// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Data-independent synchronous RAII lock capability.

use std::sync::{
    Arc,
    Mutex,
    MutexGuard,
};

#[cfg(feature = "parking-lot")]
use parking_lot::{
    Mutex as ParkingLotMutex,
    MutexGuard as ParkingLotMutexGuard,
};

use crate::lock::TryLockError;

/// Represents one synchronous lock-acquisition mode.
///
/// The trait is independent of protected data. Its associated guard may carry
/// backend-specific state, but generic users receive no data-access contract
/// and can only retain or drop that guard.
pub trait Lock: Send + Sync {
    /// RAII guard returned by this lock implementation.
    type Guard<'a>: 'a
    where
        Self: 'a;

    /// Acquires the lock and returns its RAII guard.
    ///
    /// # Returns
    ///
    /// A guard that releases the lock when dropped.
    ///
    /// # Panics
    ///
    /// Implementations backed by poisoned standard-library locks panic when
    /// acquisition reports poisoning.
    #[must_use = "dropping the guard immediately releases the lock"]
    fn lock(&self) -> Self::Guard<'_>;

    /// Attempts to acquire the lock without blocking.
    ///
    /// # Returns
    ///
    /// A guard when acquisition succeeds.
    ///
    /// # Errors
    ///
    /// Returns TryLockError::WouldBlock when another guard prevents immediate
    /// acquisition, or TryLockError::Poisoned when supported by the backend.
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError>;
}

impl<L> Lock for &L
where
    L: Lock + ?Sized,
{
    type Guard<'a>
        = L::Guard<'a>
    where
        Self: 'a;

    /// Delegates blocking acquisition to the borrowed lock.
    #[inline(always)]
    fn lock(&self) -> Self::Guard<'_> {
        L::lock(*self)
    }

    /// Delegates non-blocking acquisition to the borrowed lock.
    #[inline(always)]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        L::try_lock(*self)
    }
}

impl<L> Lock for Arc<L>
where
    L: Lock + ?Sized,
{
    type Guard<'a>
        = L::Guard<'a>
    where
        Self: 'a;

    /// Delegates blocking acquisition to the shared lock.
    #[inline(always)]
    fn lock(&self) -> Self::Guard<'_> {
        L::lock(self.as_ref())
    }

    /// Delegates non-blocking acquisition to the shared lock.
    #[inline(always)]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        L::try_lock(self.as_ref())
    }
}

impl<T> Lock for Mutex<T>
where
    T: Send + ?Sized,
{
    type Guard<'a>
        = MutexGuard<'a, T>
    where
        Self: 'a;

    /// Acquires the mutex.
    ///
    /// # Panics
    ///
    /// Panics when the mutex is poisoned.
    #[inline]
    fn lock(&self) -> Self::Guard<'_> {
        Mutex::lock(self).unwrap()
    }

    /// Attempts to acquire the mutex without blocking.
    #[inline]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        match Mutex::try_lock(self) {
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

#[cfg(feature = "parking-lot")]
impl<T> Lock for ParkingLotMutex<T>
where
    T: Send + ?Sized,
{
    type Guard<'a>
        = ParkingLotMutexGuard<'a, T>
    where
        Self: 'a;

    /// Acquires the non-poisoning parking_lot mutex.
    #[inline]
    fn lock(&self) -> Self::Guard<'_> {
        ParkingLotMutex::lock(self)
    }

    /// Attempts to acquire the parking_lot mutex without blocking.
    #[inline]
    fn try_lock(&self) -> Result<Self::Guard<'_>, TryLockError> {
        ParkingLotMutex::try_lock(self).ok_or(TryLockError::WouldBlock)
    }
}
