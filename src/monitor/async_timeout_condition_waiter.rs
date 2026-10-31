// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous timeout condition-wait capability.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;

use crate::monitor::AsyncConditionWaiter;
use crate::monitor::WaitTimeoutResult;

/// Waits asynchronously for predicates over protected state with timeouts.
///
/// The relative `*_for_async` methods use a condition-wait budget, aligned
/// with [`std::sync::Condvar::wait_timeout_while`]. Their futures are lazy:
/// time before the first poll and initial state-lock contention do not consume
/// that budget. It starts after acquiring the state lock and before the first
/// predicate check, and one fixed deadline is reused across wakeups.
///
/// The absolute `*_with_deadline_async` methods also return lazy futures, but
/// their supplied deadline is never reset. Time before the first poll and
/// time spent acquiring the initial state lock both advance toward that
/// deadline. Neither form can interrupt mutex acquisition or reacquisition.
/// A timed wait may return after its timeout while reacquiring the state lock.
/// A zero timeout still checks the predicate. After waiting begins, Timer
/// registration or completion errors take precedence over every post-wait
/// predicate result, and the action is not run. When the Timer completes
/// successfully, a final locked predicate check still wins over timeout.
///
/// The external predicate state handshake documented by
/// [`AsyncConditionWaiter`] also applies to timed asynchronous waits.
///
/// Dropping a pending future cancels and unregisters its active wait. It does
/// not run the action or roll back protected-state changes made while the wait
/// existed. If a notification already selected that waiter, cancellation
/// discards the selection rather than transferring it to another or future
/// waiter.
///
/// Use this trait as a static generic bound when asynchronous code needs timed
/// predicate waits. Its return-position `impl Future` methods make it
/// unsuitable as a `dyn` trait-object interface. Absolute deadlines must come
/// from the clock domain of the monitor Timer used for the wait. Operations
/// sharing a deadline must use timers in that same domain; equal instant
/// values do not imply a shared domain.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use qubit_lock::{
///     AsyncTimeoutConditionWaiter,
///     TokioMonitor,
///     WaitTimeoutResult,
/// };
///
/// # #[tokio::main]
/// # async fn main() {
/// let monitor = TokioMonitor::current(true);
/// let deadline = monitor.timer().clock().now()
///     .checked_add(Duration::from_secs(1))
///     .expect("short deadline should be representable");
/// let result = monitor.wait_until_ready_with_deadline_async(
///     deadline,
///     |ready| *ready,
/// ).await.expect("deadline belongs to this monitor timer");
/// assert_eq!(result, WaitTimeoutResult::Ready(()));
/// # }
/// ```
pub trait AsyncTimeoutConditionWaiter: AsyncConditionWaiter {
    /// Returns a future that waits until the predicate becomes true or an
    /// absolute deadline passes.
    ///
    /// The future is lazy, but the supplied deadline is not reset when it is
    /// first polled. The deadline must belong to this waiter's Timer clock
    /// domain. A ready predicate wins even when the deadline has passed.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes true.
    /// * `P` - Readiness predicate. It must be sendable and remain valid for
    ///   `'a`.
    /// * `F` - Action closure run with mutable protected-state access. It must
    ///   be sendable and remain valid for `'a`.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute monotonic deadline from this monitor Timer's
    ///   clock domain. Equal timestamp values from different domains are not
    ///   interchangeable.
    /// * predicate - Predicate that returns true when the state is ready.
    /// * action - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// A future resolving to Ready with the action result or TimedOut.
    ///
    /// # Errors
    ///
    /// The future resolves to a Timer domain error if waiting is required and
    /// the deadline belongs to another clock domain, or to a Timer
    /// registration/completion error.
    ///
    /// # Panics
    ///
    /// The future propagates a panic from predicate or action when polled.
    #[inline(always)]
    fn wait_until_with_deadline_async<'a, R, P, F>(
        &'a self,
        deadline: MonotonicInstant,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_with_deadline_async(deadline, move |state| !predicate(state), action)
    }

    /// Returns a future that waits until the predicate becomes true or an
    /// absolute deadline passes without running an action.
    ///
    /// # Type Parameters
    ///
    /// * `P` - Readiness predicate. It must be sendable and remain valid for
    ///   `'a`.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute monotonic deadline from this monitor Timer's
    ///   clock domain. Equal timestamp values from different domains are not
    ///   interchangeable.
    /// * predicate - Predicate that returns true when the state is ready.
    ///
    /// # Returns
    ///
    /// A future resolving to Ready with unit or TimedOut.
    ///
    /// # Errors
    ///
    /// The future resolves to a Timer domain error for a deadline from another
    /// clock domain, or to a Timer registration/completion error, when waiting
    /// is required.
    ///
    /// # Panics
    ///
    /// The future propagates a panic from predicate when polled.
    #[inline(always)]
    fn wait_until_ready_with_deadline_async<'a, P>(
        &'a self,
        deadline: MonotonicInstant,
        predicate: P,
    ) -> impl Future<Output = Result<WaitTimeoutResult<()>, TimeError>> + Send + 'a
    where
        P: FnMut(&Self::State) -> bool + Send + 'a,
    {
        self.wait_until_with_deadline_async(deadline, predicate, |_| ())
    }

    /// Returns a future that waits while the predicate remains true or until
    /// an absolute deadline passes.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate. It must be sendable and remain valid for
    ///   `'a`.
    /// * `F` - Action closure run with mutable protected-state access. It must
    ///   be sendable and remain valid for `'a`.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute monotonic deadline from this monitor Timer's
    ///   clock domain. Equal timestamp values from different domains are not
    ///   interchangeable.
    /// * predicate - Predicate that returns true while waiting continues.
    /// * action - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A future resolving to Ready with the action result or TimedOut.
    ///
    /// # Errors
    ///
    /// The future resolves to a Timer domain error if waiting is required and
    /// the deadline belongs to another clock domain, or to a Timer
    /// registration/completion error.
    ///
    /// # Panics
    ///
    /// The future propagates a panic from predicate or action when polled.
    fn wait_while_with_deadline_async<'a, R, P, F>(
        &'a self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;

    /// Returns a future that waits until the predicate becomes true or times
    /// out.
    ///
    /// The trait-level timeout, laziness, and cancellation contract applies to
    /// the returned future.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes true.
    /// * `P` - Readiness predicate. It must be sendable and remain valid for
    ///   `'a`.
    /// * `F` - Action closure run with mutable protected-state access. It must
    ///   be sendable and remain valid for `'a`.
    ///
    /// # Parameters
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
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::InstantOverflow`] if waiting is required and adding
    /// `timeout` to the sampled start time cannot represent a deadline in the
    /// Timer's clock domain. This error is not reported as a normal timeout.
    ///
    /// The future resolves to Timer registration or completion errors rather
    /// than reporting them as timeouts. After waiting begins, such an error
    /// takes precedence over a post-wait ready predicate and prevents `action`
    /// from running.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled.
    #[inline(always)]
    fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_for_async(timeout, move |state| !predicate(state), action)
    }

    /// Returns a future that waits until the predicate becomes true or times
    /// out.
    ///
    /// This convenience method does not run an action after the predicate
    /// becomes true. Its timeout budget, laziness, cancellation, and
    /// deadline-boundary semantics are identical to
    /// [`Self::wait_until_for_async`].
    ///
    /// # Type Parameters
    ///
    /// * `P` - Readiness predicate. It must be sendable and remain valid for
    ///   `'a`.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to [`WaitTimeoutResult::Ready`] with `()`
    /// when the predicate becomes true, or [`WaitTimeoutResult::TimedOut`] when
    /// the condition-wait budget expires.
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::InstantOverflow`] if waiting is required and adding
    /// `timeout` to the sampled start time cannot represent a deadline in the
    /// Timer's clock domain. This error is not reported as a normal timeout.
    ///
    /// The future resolves to Timer registration or completion errors rather
    /// than reporting them as timeouts. After waiting begins, such an error
    /// takes precedence over a post-wait ready predicate.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` when it is
    /// polled.
    #[inline(always)]
    fn wait_until_ready_for_async<'a, P>(
        &'a self,
        timeout: Duration,
        predicate: P,
    ) -> impl Future<Output = Result<WaitTimeoutResult<()>, TimeError>> + Send + 'a
    where
        P: FnMut(&Self::State) -> bool + Send + 'a,
    {
        self.wait_until_for_async(timeout, predicate, |_| ())
    }

    /// Returns a future that waits while the predicate remains true or times
    /// out.
    ///
    /// The trait-level timeout, laziness, and cancellation contract applies to
    /// the returned future.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate. It must be sendable and remain valid for
    ///   `'a`.
    /// * `F` - Action closure run with mutable protected-state access. It must
    ///   be sendable and remain valid for `'a`.
    ///
    /// # Parameters
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
    ///
    /// # Errors
    ///
    /// Returns [`TimeError::InstantOverflow`] if waiting is required and adding
    /// `timeout` to the sampled start time cannot represent a deadline in the
    /// Timer's clock domain. This error is not reported as a normal timeout.
    ///
    /// The future resolves to Timer registration or completion errors rather
    /// than reporting them as timeouts. After waiting begins, such an error
    /// takes precedence over a post-wait ready predicate and prevents `action`
    /// from running.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled.
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}

impl<M> AsyncTimeoutConditionWaiter for Arc<M>
where
    M: AsyncTimeoutConditionWaiter + ?Sized,
{
    /// See [`AsyncTimeoutConditionWaiter::wait_while_with_deadline_async`] for
    /// the complete lazy-wait, cancellation, and error-precedence contract.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate forwarded to the wrapped monitor. It must be
    ///   sendable and remain valid for `'a`.
    /// * `F` - Action closure forwarded to the wrapped monitor. It must be
    ///   sendable and remain valid for `'a`.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the wrapped monitor Timer's clock
    ///   domain.
    /// * `predicate` - Returns `true` while waiting must continue.
    /// * `action` - Runs with mutable state after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A lazy future yielding the action result as
    /// [`WaitTimeoutResult::Ready`], or [`WaitTimeoutResult::TimedOut`] if
    /// the final predicate still blocks.
    ///
    /// # Errors
    ///
    /// Forwards errors from the wrapped waiter unchanged, including Timer
    /// clock-domain, registration, and completion errors when waiting is
    /// needed.
    ///
    /// # Panics
    ///
    /// Propagates panics from the wrapped waiter, predicate, or action.
    #[inline(always)]
    fn wait_while_with_deadline_async<'a, R, P, F>(
        &'a self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.as_ref()
            .wait_while_with_deadline_async(deadline, predicate, action)
    }

    /// Forwards the timed asynchronous wait to the wrapped monitor.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value produced by `action` when the predicate becomes false.
    /// * `P` - Waiting predicate forwarded to the wrapped monitor. It must be
    ///   sendable and remain valid for `'a`.
    /// * `F` - Action closure forwarded to the wrapped monitor. It must be
    ///   sendable and remain valid for `'a`.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget.
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The future returned by the wrapped monitor.
    ///
    /// # Errors
    ///
    /// The future resolves to Timer registration or completion errors from the
    /// wrapped monitor rather than reporting them as timeouts.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from the wrapped monitor,
    /// including from `predicate` or `action`, when it is polled.
    #[inline(always)]
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.as_ref().wait_while_for_async(timeout, predicate, action)
    }
}
