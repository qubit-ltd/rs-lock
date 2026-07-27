// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Blocking timeout condition-wait capability.

use qubit_clock::TimeError;
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
/// predicate. After waiting begins, Timer registration or completion errors
/// take precedence over every post-wait predicate result, and the action is not
/// run. When the Timer completes successfully, a final locked predicate check
/// still wins over timeout.
///
/// The external predicate state handshake documented by
/// [`ConditionWaiter`] also applies to timed waits.
///
/// Use this trait as a static generic bound when blocking code needs timed
/// predicate waits. Its generic methods make it unsuitable as a `dyn`
/// trait-object interface.
pub trait TimeoutConditionWaiter: ConditionWaiter {
    /// Blocks until the predicate becomes true or the timeout expires.
    ///
    /// # Parameters
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
    ///
    /// # Errors
    ///
    /// Returns Timer registration or completion errors rather than reporting
    /// them as timeouts. After waiting begins, such an error takes precedence
    /// over a post-wait ready predicate and prevents `action` from running.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate` or `action`.
    #[inline(always)]
    fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while_for(timeout, move |state| !predicate(state), action)
    }

    /// Blocks until the predicate becomes true or the timeout expires.
    ///
    /// This convenience method does not run an action after the predicate
    /// becomes true. Its timeout budget and deadline-boundary semantics are
    /// identical to [`Self::wait_until_for`].
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum relative duration to wait.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with `()` when the predicate becomes true,
    /// or [`WaitTimeoutResult::TimedOut`] when the condition-wait budget
    /// expires.
    ///
    /// # Errors
    ///
    /// Returns Timer registration or completion errors rather than reporting
    /// them as timeouts. After waiting begins, such an error takes precedence
    /// over a post-wait ready predicate.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate`.
    #[inline(always)]
    fn wait_until_ready_for<P>(
        &self,
        timeout: Duration,
        predicate: P,
    ) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
    {
        self.wait_until_for(timeout, predicate, |_| ())
    }

    /// Blocks while the predicate remains true or until the timeout expires.
    ///
    /// # Parameters
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
    ///
    /// # Errors
    ///
    /// Returns Timer registration or completion errors rather than reporting
    /// them as timeouts. After waiting begins, such an error takes precedence
    /// over a post-wait ready predicate and prevents `action` from running.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate` or `action`.
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R;
}

impl<M: ?Sized> TimeoutConditionWaiter for Arc<M>
where
    M: TimeoutConditionWaiter,
{
    /// Delegates a timed blocking condition wait to the shared monitor.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget.
    /// * `predicate` - Predicate that remains true while waiting continues.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The timed-wait result returned by the wrapped monitor.
    ///
    /// # Errors
    ///
    /// Returns an error when the wrapped monitor's Timer fails.
    ///
    /// # Panics
    ///
    /// Propagates a panic from the wrapped monitor, including from `predicate`
    /// or `action`.
    #[inline(always)]
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
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
