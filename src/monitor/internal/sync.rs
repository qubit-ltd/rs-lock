// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Selects the synchronization primitives used by the blocking monitor.
//!
//! Normal builds use standard-library primitives. Loom model builds replace
//! them with Loom's equivalents so the production monitor handshake remains
//! visible to the scheduler.

use std::sync::{LockResult, PoisonError};

#[cfg(all(loom, feature = "loom-model"))]
pub(in crate::monitor) use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(all(loom, feature = "loom-model")))]
pub(in crate::monitor) use std::sync::{Condvar, Mutex, MutexGuard};

/// Recovers a synchronization primitive's protected value after poisoning.
///
/// # Parameters
///
/// * `result` - Result returned by locking or waiting on a primitive.
///
/// # Returns
///
/// The protected value, including the recovered value after a panic.
#[inline]
pub(in crate::monitor) fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(PoisonError::into_inner)
}

/// Reports whether the selected mutex backend records poisoning.
///
/// Loom's mutex intentionally does not model standard-library poisoning, so a
/// Loom model build reports `false`. Normal builds delegate to
/// [`std::sync::Mutex::is_poisoned`].
#[cfg(all(loom, feature = "loom-model"))]
#[inline(always)]
pub(in crate::monitor) const fn is_poisoned<T>(_mutex: &Mutex<T>) -> bool {
    false
}

/// Reports whether the selected mutex backend records poisoning.
#[cfg(not(all(loom, feature = "loom-model")))]
#[inline(always)]
pub(in crate::monitor) fn is_poisoned<T>(mutex: &Mutex<T>) -> bool {
    mutex.is_poisoned()
}

/// Clears poisoning when the selected mutex backend supports it.
///
/// Loom's mutex intentionally does not model standard-library poisoning, so
/// this is a no-op in Loom model builds.
#[cfg(all(loom, feature = "loom-model"))]
#[inline(always)]
pub(in crate::monitor) const fn clear_poison<T>(_mutex: &Mutex<T>) {}

/// Clears poisoning when the selected mutex backend supports it.
#[cfg(not(all(loom, feature = "loom-model")))]
#[inline(always)]
pub(in crate::monitor) fn clear_poison<T>(mutex: &Mutex<T>) {
    mutex.clear_poison();
}
