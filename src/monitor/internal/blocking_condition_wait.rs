// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Defines backend-neutral blocking predicate-wait algorithms.

use std::ops::DerefMut;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;

use super::BlockingConditionWaiter;
use crate::monitor::WaitTimeoutResult;
use crate::monitor::WaitTimeoutStatus;

/// Waits while a predicate remains true using a backend-specific guard wait.
///
/// # Type Parameters
///
/// * `G` - Guard providing mutable access to the protected state.
/// * `T` - Protected state type.
/// * `R` - Value returned by the ready action.
/// * `P` - Predicate deciding whether waiting must continue.
/// * `F` - Action run once the predicate stops blocking.
/// * `W` - Backend-specific suspension operation.
///
/// # Parameters
///
/// * `guard` - Acquired state guard retained across predicate checks.
/// * `waiting` - Predicate returning `true` while suspension must continue.
/// * `f` - Action receiving mutable state when waiting finishes.
/// * `wait` - Operation that releases and reacquires `guard` once.
///
/// # Returns
///
/// The value returned by `f` after `waiting` becomes false.
///
/// # Panics
///
/// Propagates a panic from `waiting`, `f`, or `wait`.
pub(in crate::monitor) fn wait_while_locked<G, T, R, P, F, W>(mut guard: G, mut waiting: P, f: F, mut wait: W) -> R
where
    G: DerefMut<Target = T>,
    P: FnMut(&T) -> bool,
    F: FnOnce(&mut T) -> R,
    W: FnMut(&mut G),
{
    while waiting(&*guard) {
        wait(&mut guard);
    }
    f(&mut *guard)
}

/// Waits while a predicate remains true using one fixed Timer future.
///
/// # Type Parameters
///
/// * `G` - Guard providing mutable access to the protected state.
/// * `T` - Protected state type.
/// * `R` - Value returned by the ready action.
/// * `P` - Predicate deciding whether waiting must continue.
/// * `F` - Action run once the predicate stops blocking.
/// * `W` - Backend-specific suspension operation.
///
/// # Parameters
///
/// * `guard` - Acquired state guard retained across predicate checks.
/// * `future` - Fixed Timer registration reused across wakeups.
/// * `waiting` - Predicate returning `true` while suspension must continue.
/// * `f` - Action receiving mutable state when waiting finishes.
/// * `wait` - Operation that releases and reacquires `guard` while polling the
///   fixed Timer.
///
/// # Returns
///
/// [`WaitTimeoutResult::Ready`] with the value returned by `f` when `waiting`
/// becomes false, or [`WaitTimeoutResult::TimedOut`] when the Timer completes
/// while the predicate still blocks.
///
/// # Errors
///
/// Returns Timer completion errors reported by `wait`. Such an error prevents
/// `f` from running.
///
/// # Panics
///
/// Propagates a panic from `waiting`, `f`, or `wait`.
pub(in crate::monitor) fn wait_while_with_timer_locked<G, T, R, P, F, W>(
    mut guard: G,
    mut future: TimerFuture,
    mut waiting: P,
    f: F,
    mut wait: W,
) -> Result<WaitTimeoutResult<R>, TimeError>
where
    G: DerefMut<Target = T>,
    P: FnMut(&T) -> bool,
    F: FnOnce(&mut T) -> R,
    W: FnMut(&mut G, &mut TimerFuture) -> Result<WaitTimeoutStatus, TimeError>,
{
    loop {
        let status = wait(&mut guard, &mut future)?;
        if !waiting(&*guard) {
            return Ok(WaitTimeoutResult::Ready(f(&mut *guard)));
        }
        if status.is_timed_out() {
            return Ok(WaitTimeoutResult::TimedOut);
        }
    }
}

/// Waits while a predicate remains true with a relative condition-wait budget.
///
/// The budget starts after `lock` acquires the state guard and before the
/// initial predicate check. One deadline and Timer future are reused across all
/// wakeups.
///
/// # Type Parameters
///
/// * `L` - Backend-specific state-guard acquisition operation.
/// * `G` - Guard providing mutable access to the protected state.
/// * `T` - Protected state type.
/// * `R` - Value returned by the ready action.
/// * `P` - Predicate deciding whether waiting must continue.
/// * `F` - Action run once the predicate stops blocking.
/// * `W` - Backend-specific timed suspension operation.
///
/// # Parameters
///
/// * `timer` - Timer supplying the condition-wait deadline.
/// * `timeout` - Relative condition-wait budget.
/// * `lock` - Operation acquiring the backend state guard.
/// * `waiting` - Predicate returning `true` while suspension must continue.
/// * `f` - Action receiving mutable state when waiting finishes.
/// * `wait` - Operation that releases and reacquires `guard` around the Timer.
///
/// # Returns
///
/// [`WaitTimeoutResult::Ready`] with the value returned by `f`, or
/// [`WaitTimeoutResult::TimedOut`] when the Timer completes while the
/// predicate still blocks.
///
/// # Errors
///
/// Returns deadline construction, Timer registration, or Timer completion
/// errors when waiting is required.
///
/// # Panics
///
/// Propagates a panic from `lock`, `waiting`, `f`, or `wait`.
pub(in crate::monitor) fn wait_while_for<L, G, T, R, P, F, W>(
    timer: &dyn Timer,
    timeout: Duration,
    lock: L,
    mut waiting: P,
    f: F,
    wait: W,
) -> Result<WaitTimeoutResult<R>, TimeError>
where
    L: FnOnce() -> G,
    G: DerefMut<Target = T>,
    P: FnMut(&T) -> bool,
    F: FnOnce(&mut T) -> R,
    W: FnMut(&mut G, &mut TimerFuture) -> Result<WaitTimeoutStatus, TimeError>,
{
    let mut guard = lock();
    let started_at = timer.clock().now();
    if !waiting(&*guard) {
        return Ok(WaitTimeoutResult::Ready(f(&mut *guard)));
    }
    if timeout.is_zero() {
        return Ok(WaitTimeoutResult::TimedOut);
    }
    let deadline = started_at.checked_add(timeout)?;
    let future = timer.at(deadline)?;
    wait_while_with_timer_locked(guard, future, waiting, f, wait)
}

/// Waits while a predicate remains true until an absolute deadline.
///
/// The supplied deadline includes initial lock acquisition and predicate
/// evaluation. A ready predicate wins the deciding locked check even if the
/// deadline has passed.
///
/// # Type Parameters
///
/// * `L` - Backend-specific state-guard acquisition operation.
/// * `G` - Guard providing mutable access to the protected state.
/// * `T` - Protected state type.
/// * `R` - Value returned by the ready action.
/// * `P` - Predicate deciding whether waiting must continue.
/// * `F` - Action run once the predicate stops blocking.
/// * `W` - Backend-specific timed suspension operation.
///
/// # Parameters
///
/// * `timer` - Timer that validates and registers `deadline`.
/// * `deadline` - Absolute deadline in the Timer clock domain.
/// * `lock` - Operation acquiring the backend state guard.
/// * `waiting` - Predicate returning `true` while suspension must continue.
/// * `f` - Action receiving mutable state when waiting finishes.
/// * `wait` - Operation that releases and reacquires `guard` around the Timer.
///
/// # Returns
///
/// [`WaitTimeoutResult::Ready`] with the value returned by `f`, or
/// [`WaitTimeoutResult::TimedOut`] when the Timer completes while the
/// predicate still blocks.
///
/// # Errors
///
/// Returns Timer domain, registration, or completion errors when waiting is
/// required.
///
/// # Panics
///
/// Propagates a panic from `lock`, `waiting`, `f`, or `wait`.
pub(in crate::monitor) fn wait_while_with_deadline<L, G, T, R, P, F, W>(
    timer: &dyn Timer,
    deadline: MonotonicInstant,
    lock: L,
    mut waiting: P,
    f: F,
    wait: W,
) -> Result<WaitTimeoutResult<R>, TimeError>
where
    L: FnOnce() -> G,
    G: DerefMut<Target = T>,
    P: FnMut(&T) -> bool,
    F: FnOnce(&mut T) -> R,
    W: FnMut(&mut G, &mut TimerFuture) -> Result<WaitTimeoutStatus, TimeError>,
{
    let mut guard = lock();
    if !waiting(&*guard) {
        return Ok(WaitTimeoutResult::Ready(f(&mut *guard)));
    }
    let mut future = timer.at(deadline)?;
    if let Poll::Ready(result) = BlockingConditionWaiter::poll_timer_without_waiter(&mut future) {
        result?;
        return Ok(WaitTimeoutResult::TimedOut);
    }
    wait_while_with_timer_locked(guard, future, waiting, f, wait)
}
