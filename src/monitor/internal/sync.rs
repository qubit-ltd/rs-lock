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

use std::sync::{
    LockResult,
    PoisonError,
};

#[cfg(all(loom, feature = "loom-model"))]
pub(in crate::monitor) use loom::sync::{
    Condvar,
    Mutex,
    MutexGuard,
};
#[cfg(not(all(loom, feature = "loom-model")))]
pub(in crate::monitor) use std::sync::{
    Condvar,
    Mutex,
    MutexGuard,
};

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
