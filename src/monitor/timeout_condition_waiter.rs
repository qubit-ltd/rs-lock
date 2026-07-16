// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Blocking timeout condition-wait capability.

use std::{
    sync::Arc,
    time::Duration,
};

use crate::monitor::{
    ConditionWaiter,
    WaitTimeoutResult,
};

/// Waits for predicates over protected state with relative timeouts.
///
/// A timeout is a condition-wait budget. Initial state-lock contention and the
/// initial locked predicate check are excluded. If waiting is required, one
/// fixed deadline is established immediately before the first condition-wait
/// suspension and reused across wakeups. A zero timeout still checks the
/// predicate, and a final locked predicate check wins over timeout.
pub trait TimeoutConditionWaiter: ConditionWaiter {
    /// Blocks until the predicate becomes true or the timeout expires.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum relative duration to wait.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the action result, or
    /// [`WaitTimeoutResult::TimedOut`] when the condition-wait budget expires.
    /// The trait-level timeout contract determines when that budget starts and
    /// how the deadline boundary is resolved.
    #[inline(always)]
    fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while_for(timeout, move |state| !predicate(state), action)
    }

    /// Blocks while the predicate remains true or until the timeout expires.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum relative duration to wait.
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the action result, or
    /// [`WaitTimeoutResult::TimedOut`] when the condition-wait budget expires.
    /// The trait-level timeout contract determines when that budget starts and
    /// how the deadline boundary is resolved.
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R;
}

impl<M: ?Sized> TimeoutConditionWaiter for Arc<M>
where
    M: TimeoutConditionWaiter,
{
    /// Delegates a timed blocking condition wait to the shared monitor.
    #[inline(always)]
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <M as TimeoutConditionWaiter>::wait_while_for(
            self.as_ref(),
            timeout,
            predicate,
            action,
        )
    }
}
