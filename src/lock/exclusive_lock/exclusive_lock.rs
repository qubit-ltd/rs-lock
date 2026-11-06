// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Marker capability for exclusive synchronous lock-acquisition modes.

use std::sync::Arc;
use std::sync::Mutex;

#[cfg(feature = "parking-lot")]
use parking_lot::Mutex as ParkingLotMutex;

use crate::lock::Lock;
use crate::lock::ReadWriteLock;
use crate::lock::WriteLock;

/// Marks a [`Lock`] acquisition mode that excludes every competing guard.
///
/// While a guard acquired through an `ExclusiveLock` implementation remains
/// alive, no other guard for the same underlying lock may coexist. The marker
/// describes the acquisition mode rather than the protected data: mutexes and
/// write-mode adapters implement it, while read-mode adapters deliberately do
/// not.
///
/// This is an open, safe trait so third-party lock implementations can expose
/// the capability. Implementors must uphold the exclusivity contract, but
/// generic code must not rely on this marker to justify otherwise unsafe Rust
/// operations.
///
/// # Examples
///
/// ```
/// use qubit_lock::ExclusiveLock;
///
/// fn run_exclusively<L>(lock: &L)
/// where
///     L: ExclusiveLock + ?Sized,
/// {
///     let _guard = lock.lock();
/// }
///
/// run_exclusively(&std::sync::Mutex::new(()));
/// ```
///
/// A read-mode adapter cannot be used where exclusive acquisition is required:
///
/// ```compile_fail
/// use std::sync::RwLock;
///
/// use qubit_lock::ExclusiveLock;
/// use qubit_lock::ReadWriteLock;
///
/// fn run_exclusively<L: ExclusiveLock + ?Sized>(lock: &L) {
///     let _guard = lock.lock();
/// }
///
/// let lock = RwLock::new(());
/// let read_mode = lock.read_lock();
/// run_exclusively(&read_mode);
/// ```
pub trait ExclusiveLock: Lock {
    // empty
}

impl<L> ExclusiveLock for &L where L: ExclusiveLock + ?Sized {}

impl<L> ExclusiveLock for Arc<L> where L: ExclusiveLock + ?Sized {}

impl<T> ExclusiveLock for Mutex<T> where T: Send + ?Sized {}

#[cfg(feature = "parking-lot")]
impl<T> ExclusiveLock for ParkingLotMutex<T> where T: Send + ?Sized {}

impl<L> ExclusiveLock for WriteLock<'_, L> where L: ReadWriteLock + ?Sized {}
