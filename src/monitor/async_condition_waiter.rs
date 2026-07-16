// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous condition-wait capability.

use std::future::Future;

/// Waits asynchronously for predicates over protected monitor state.
///
/// Returned futures are lazy: constructing one neither acquires the state lock
/// nor registers a waiter. Once polling reaches a blocking predicate check,
/// the waiter is registered before the state lock is released. Notifications
/// have memoryless condition-variable semantics, so every wakeup rechecks the
/// predicate and no fairness is guaranteed.
///
/// Dropping a pending future cancels and unregisters its active wait. It does
/// not run the action or roll back protected-state changes made while the wait
/// existed. If a notification already selected that waiter, cancellation
/// discards the selection rather than transferring it to another or future
/// waiter.
pub trait AsyncConditionWaiter {
    /// State protected by the monitor.
    type State;

    /// Returns a future that waits until the predicate becomes true.
    ///
    /// The predicate and action run while the monitor state is locked. The
    /// trait-level laziness and cancellation contract applies to the returned
    /// future.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to the value returned by `action`.
    #[inline(always)]
    fn wait_until_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_async(move |state| !predicate(state), action)
    }

    /// Returns a future that waits while the predicate remains true.
    ///
    /// The predicate and action run while the monitor state is locked. The
    /// trait-level laziness and cancellation contract applies to the returned
    /// future.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to the value returned by `action`.
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}
