// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate blocking monitor capability.

use std::sync::Arc;

use crate::monitor::{ConditionWaiter, Notifier};

/// Aggregate trait for blocking monitor-style synchronization.
///
/// A full blocking monitor combines protected-state access, memoryless
/// notification, and condition waits without requiring timeout support.
/// Combined write-and-notify methods update state while holding the monitor
/// lock, release that lock, and then notify registered waiters.
///
/// Use this trait as a static generic bound when code needs the complete
/// untimed blocking monitor contract. Its generic methods make it unsuitable
/// as a `dyn` trait-object interface. Prefer [`ConditionWaiter`] or
/// [`Notifier`] when an API needs only one narrower capability.
pub trait Monitor: Notifier + ConditionWaiter {
    /// Reads protected monitor state while holding its lock.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving immutable access to the protected state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f` after the monitor lock is released.
    fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&Self::State) -> R;

    /// Mutates protected monitor state while holding its lock.
    ///
    /// This method does not notify waiters automatically.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f` after the monitor lock is released.
    fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R;

    /// Mutates protected state and then notifies one registered waiter.
    ///
    /// The monitor lock is released before notification. If `f` panics, the
    /// panic propagates and no notification is sent.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`. In that case no notification is sent.
    #[inline(always)]
    fn with_write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R,
    {
        let result = self.with_write(f);
        self.notify_one();
        result
    }

    /// Mutates protected state and then notifies all registered waiters.
    ///
    /// The monitor lock is released before notification. If `f` panics, the
    /// panic propagates and no notification is sent.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`. In that case no notification is sent.
    #[inline(always)]
    fn with_write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R,
    {
        let result = self.with_write(f);
        self.notify_all();
        result
    }
}

impl<M: ?Sized> Monitor for Arc<M>
where
    M: Monitor,
{
    /// Delegates protected-state reading to the shared monitor.
    #[inline(always)]
    fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&Self::State) -> R,
    {
        self.as_ref().with_read(f)
    }

    /// Delegates protected-state mutation to the shared monitor.
    #[inline(always)]
    fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R,
    {
        self.as_ref().with_write(f)
    }
}
