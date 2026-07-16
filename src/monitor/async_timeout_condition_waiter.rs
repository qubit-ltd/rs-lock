// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous timeout condition-wait capability.

use std::{
    future::Future,
    time::Duration,
};

use crate::monitor::{
    AsyncConditionWaiter,
    WaitTimeoutResult,
};

/// Waits asynchronously for predicates over protected state with timeouts.
///
/// The timeout is a condition-wait budget. The returned future is lazy, so
/// construction and time before its first poll consume no budget. Initial
/// state-lock contention and the initial locked predicate check are also
/// excluded. If waiting is required, one fixed deadline is established
/// immediately before the first condition-wait suspension and reused across
/// wakeups. A zero timeout still checks the predicate, and a final locked
/// predicate check wins over timeout.
///
/// Dropping a pending future cancels and unregisters its active wait. It does
/// not run the action or roll back protected-state changes made while the wait
/// existed. If a notification already selected that waiter, cancellation
/// discards the selection rather than transferring it to another or future
/// waiter.
pub trait AsyncTimeoutConditionWaiter: AsyncConditionWaiter {
    /// Returns a future that waits until the predicate becomes true or times
    /// out.
    ///
    /// The trait-level timeout, laziness, and cancellation contract applies to
    /// the returned future.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Relative condition-wait budget.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to [`WaitTimeoutResult::Ready`] with the
    /// action result, or [`WaitTimeoutResult::TimedOut`] when the budget
    /// expires while the predicate still requires waiting.
    #[inline(always)]
    fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_for_async(
            timeout,
            move |state| !predicate(state),
            action,
        )
    }

    /// Returns a future that waits while the predicate remains true or times
    /// out.
    ///
    /// The trait-level timeout, laziness, and cancellation contract applies to
    /// the returned future.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Relative condition-wait budget.
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to [`WaitTimeoutResult::Ready`] with the
    /// action result, or [`WaitTimeoutResult::TimedOut`] when the budget
    /// expires while the predicate still requires waiting.
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}
