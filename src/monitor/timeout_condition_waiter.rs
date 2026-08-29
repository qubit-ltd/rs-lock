// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Blocking timeout condition-wait capability.

use std::sync::Arc;
use std::time::Duration;

use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;

use crate::monitor::ConditionWaiter;
use crate::monitor::WaitTimeoutResult;

/// Waits for predicates over protected state with bounded time budgets.
///
/// The `*_for` methods use a condition-wait budget aligned with
/// [`std::sync::Condvar::wait_timeout_while`]. Initial state-lock contention
/// is excluded. The budget starts after acquiring the state lock and before
/// the first predicate check, so predicate work consumes it. If waiting is
/// required, one fixed deadline is reused across wakeups. A timed wait may
/// return after the timeout while reacquiring the state lock. A zero timeout
/// still checks the predicate. After waiting begins, Timer registration or
/// completion errors take precedence over every post-wait predicate result,
/// and the action is not run. When the Timer completes successfully, a final
/// locked predicate check still wins over timeout.
///
/// The `*_with_total_timeout` methods instead fix one absolute deadline before
/// attempting to acquire the state lock. Initial lock contention therefore
/// consumes that operation-wide budget. These methods do not provide a hard
/// return-time guarantee: reaching the deadline cannot interrupt mutex
/// acquisition or reacquisition, and a ready action runs after the deciding
/// predicate check without a time limit.
///
/// The external predicate state handshake documented by
/// [`ConditionWaiter`] also applies to timed waits.
///
/// Use this trait as a static generic bound when blocking code needs timed
/// predicate waits. Its generic methods make it unsuitable as a `dyn`
/// trait-object interface.
pub trait TimeoutConditionWaiter: ConditionWaiter {
    /// Blocks until the predicate becomes true or an absolute deadline passes.
    ///
    /// The supplied deadline keeps advancing while acquiring the state lock,
    /// evaluating the predicate, waiting, and reacquiring the lock. A ready
    /// predicate wins even when the deadline has already passed.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes true.
    /// * `P` - Readiness predicate evaluated against the protected state.
    /// * `F` - Action closure run with mutable protected-state access.
    ///
    /// # Parameters
    ///
    /// * deadline - Absolute monotonic deadline for the condition wait.
    /// * predicate - Predicate that returns true when the state is ready.
    /// * action - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// Returns Ready with the action result, or TimedOut when the deadline
    /// passes while the predicate remains false.
    ///
    /// # Errors
    ///
    /// Returns Timer domain, registration, or completion errors when waiting
    /// is required. After waiting begins, such an error prevents action.
    ///
    /// # Panics
    ///
    /// Propagates a panic from predicate or action.
    #[inline(always)]
    fn wait_until_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        mut predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while_with_deadline(deadline, move |state| !predicate(state), action)
    }

    /// Blocks until the predicate becomes true or an absolute deadline passes.
    ///
    /// This convenience method does not run an action and otherwise has the
    /// same deadline and error semantics as wait_until_with_deadline.
    ///
    /// # Type Parameters
    ///
    /// * `P` - Readiness predicate evaluated against the protected state.
    ///
    /// # Parameters
    ///
    /// * deadline - Absolute monotonic deadline for the condition wait.
    /// * predicate - Predicate that returns true when the state is ready.
    ///
    /// # Returns
    ///
    /// Returns Ready with unit, or TimedOut when the deadline passes.
    ///
    /// # Errors
    ///
    /// Returns Timer errors when waiting is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from predicate.
    #[inline(always)]
    fn wait_until_ready_with_deadline<P>(
        &self,
        deadline: MonotonicInstant,
        predicate: P,
    ) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
    {
        self.wait_until_with_deadline(deadline, predicate, |_| ())
    }

    /// Blocks while the predicate remains true or until an absolute deadline.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate evaluated against the protected state.
    /// * `F` - Action closure run with mutable protected-state access.
    ///
    /// # Parameters
    ///
    /// * deadline - Absolute monotonic deadline for the condition wait.
    /// * predicate - Predicate that returns true while waiting continues.
    /// * action - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// Returns Ready with the action result, or TimedOut when the deadline
    /// passes while the predicate remains true.
    ///
    /// # Errors
    ///
    /// Returns Timer domain, registration, or completion errors when waiting
    /// is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from predicate or action.
    fn wait_while_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R;

    /// Blocks until the predicate becomes true or the total timeout expires.
    ///
    /// The implementation fixes an absolute deadline before attempting to
    /// acquire the protected state lock. Initial lock contention, predicate
    /// evaluation, condition waiting, and lock reacquisition therefore consume
    /// the same operation-wide budget. A ready predicate wins on the deciding
    /// locked check even when the deadline has already passed. The action runs
    /// without a time limit after that ready decision.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes true.
    /// * `P` - Readiness predicate evaluated against the protected state.
    /// * `F` - Action closure run with mutable protected-state access.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the action result, or
    /// [`WaitTimeoutResult::TimedOut`] when the fixed deadline has passed while
    /// the predicate remains false on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns a deadline overflow before acquiring the state lock or running
    /// the predicate. Returns Timer domain, registration, or completion errors
    /// when waiting is required. After waiting begins, such an error prevents
    /// `action` from running.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate` or `action`.
    #[inline(always)]
    fn wait_until_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while_with_total_timeout(timeout, move |state| !predicate(state), action)
    }

    /// Blocks until the predicate becomes true or the total timeout expires.
    ///
    /// This convenience method does not run an action after readiness. Its
    /// initial-lock, deadline-boundary, and Timer error semantics are identical
    /// to [`Self::wait_until_with_total_timeout`].
    ///
    /// # Type Parameters
    ///
    /// * `P` - Readiness predicate evaluated against the protected state.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with `()` when the predicate becomes true,
    /// or [`WaitTimeoutResult::TimedOut`] when the total budget expires.
    ///
    /// # Errors
    ///
    /// Returns deadline construction or Timer errors from
    /// [`Self::wait_until_with_total_timeout`].
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate`.
    #[inline(always)]
    fn wait_until_ready_with_total_timeout<P>(
        &self,
        timeout: Duration,
        predicate: P,
    ) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
    {
        self.wait_until_with_total_timeout(timeout, predicate, |_| ())
    }

    /// Blocks while the predicate remains true or the total timeout expires.
    ///
    /// The implementation must fix an absolute deadline before attempting to
    /// acquire the protected state lock. Initial lock contention, predicate
    /// evaluation, condition waiting, and lock reacquisition consume that
    /// operation-wide budget. When the predicate becomes false on the deciding
    /// locked check, `action` runs without a time limit while the state remains
    /// locked.
    ///
    /// Reaching the deadline does not interrupt mutex acquisition or
    /// reacquisition, so this method may return after `timeout`.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate evaluated against the protected state.
    /// * `F` - Action closure run with mutable protected-state access.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `predicate` - Predicate that returns `true` while waiting continues.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the action result, or
    /// [`WaitTimeoutResult::TimedOut`] when the fixed deadline has passed while
    /// the predicate remains true on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns a deadline overflow before acquiring the state lock or running
    /// the predicate. Returns Timer domain, registration, or completion errors
    /// when waiting is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate` or `action`.
    fn wait_while_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R;

    /// Blocks until the predicate becomes true or the timeout expires.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes true.
    /// * `P` - Readiness predicate evaluated against the protected state.
    /// * `F` - Action closure run with mutable protected-state access.
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
    /// # Type Parameters
    ///
    /// * `P` - Readiness predicate evaluated against the protected state.
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
    fn wait_until_ready_for<P>(&self, timeout: Duration, predicate: P) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
    {
        self.wait_until_for(timeout, predicate, |_| ())
    }

    /// Blocks while the predicate remains true or until the timeout expires.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate evaluated against the protected state.
    /// * `F` - Action closure run with mutable protected-state access.
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
    /// Delegates an absolute-deadline condition wait to the shared monitor.
    ///
    /// The deadline, result, errors, and panic behavior are forwarded
    /// unchanged to the wrapped monitor.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate forwarded to the wrapped monitor.
    /// * `F` - Action closure forwarded to the wrapped monitor.
    #[inline(always)]
    fn wait_while_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <M as TimeoutConditionWaiter>::wait_while_with_deadline(self.as_ref(), deadline, predicate, action)
    }

    /// Delegates an operation-wide timed condition wait to the shared monitor.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate forwarded to the wrapped monitor.
    /// * `F` - Action closure forwarded to the wrapped monitor.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `predicate` - Predicate that remains true while waiting continues.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The total-timeout result returned by the wrapped monitor.
    ///
    /// # Errors
    ///
    /// Returns deadline construction or Timer errors from the wrapped monitor.
    ///
    /// # Panics
    ///
    /// Propagates a panic from the wrapped monitor, including from `predicate`
    /// or `action`.
    #[inline(always)]
    fn wait_while_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <M as TimeoutConditionWaiter>::wait_while_with_total_timeout(self.as_ref(), timeout, predicate, action)
    }

    /// Delegates a timed blocking condition wait to the shared monitor.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate forwarded to the wrapped monitor.
    /// * `F` - Action closure forwarded to the wrapped monitor.
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
        <M as TimeoutConditionWaiter>::wait_while_for(self.as_ref(), timeout, predicate, action)
    }
}
