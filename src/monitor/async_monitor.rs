// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate asynchronous monitor capability.

use std::{
    future::Future,
    sync::Arc,
};

use crate::monitor::{
    AsyncConditionWaiter,
    Notifier,
};

/// Aggregate trait for asynchronous monitor-style synchronization.
///
/// This capability combines protected-state access, notification, and untimed
/// asynchronous condition waits. Use [`crate::monitor::AsyncTimedMonitor`] when
/// callers also require timeout-based waits.
///
/// Its return-position `impl Future` methods make this trait unsuitable as a
/// `dyn` trait-object interface. Prefer narrower capability traits when the
/// complete contract is unnecessary.
pub trait AsyncMonitor: Notifier + AsyncConditionWaiter + Sync {
    /// Acquires the monitor and reads the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving immutable access to the protected state.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `f` after the state lock is
    /// released.
    fn with_read_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&Self::State) -> R + Send + 'a;

    /// Acquires the monitor and mutates the protected state.
    ///
    /// This method does not notify waiters automatically.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `f` after the state lock is
    /// released.
    fn with_write_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;

    /// Mutates the protected state and wakes one waiter.
    ///
    /// The state lock is released before notification. If `f` panics, no
    /// notification is sent.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `f`. In that case no
    /// notification is sent.
    #[inline]
    fn with_write_notify_one_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        async move {
            let result = self.with_write_async(f).await;
            self.notify_one();
            result
        }
    }

    /// Mutates the protected state and wakes all waiters.
    ///
    /// The state lock is released before notification. If `f` panics, no
    /// notification is sent.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `f`. In that case no
    /// notification is sent.
    #[inline]
    fn with_write_notify_all_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        async move {
            let result = self.with_write_async(f).await;
            self.notify_all();
            result
        }
    }
}

impl<M> AsyncMonitor for Arc<M>
where
    M: AsyncMonitor + Send + ?Sized,
{
    /// Forwards protected-state reads to the wrapped monitor.
    #[inline(always)]
    fn with_read_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&Self::State) -> R + Send + 'a,
    {
        self.as_ref().with_read_async(f)
    }

    /// Forwards protected-state writes to the wrapped monitor.
    #[inline(always)]
    fn with_write_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.as_ref().with_write_async(f)
    }
}
