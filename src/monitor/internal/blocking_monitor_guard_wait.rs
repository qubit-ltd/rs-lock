// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Defines backend-neutral blocking monitor-guard wait operations.

use std::{
    sync::Arc,
    task::Poll,
};

use qubit_clock::{
    TimeError,
    TimerFuture,
};

use super::{
    BlockingConditionWaiter,
    BlockingWaiterRegistry,
};
use crate::monitor::WaitTimeoutStatus;

/// Releases the occupied state-guard slot.
///
/// # Type Parameters
///
/// * `G` - Backend-specific state-guard type.
///
/// # Parameters
///
/// * `guard` - Slot containing the currently held state guard.
/// * `missing_guard_message` - Invariant message used when the slot is empty.
///
/// # Panics
///
/// Panics with `missing_guard_message` when the state guard was already
/// released.
#[inline(always)]
pub(in crate::monitor) fn release_guard<G>(
    guard: &mut Option<G>,
    missing_guard_message: &'static str,
) {
    drop(guard.take().expect(missing_guard_message));
}

/// Waits for monitor notification while releasing and then reacquiring a
/// backend-specific state guard.
///
/// Registration completes before the state guard is released, and the
/// registration is removed before the guard is reacquired. This ordering keeps
/// notification memoryless without admitting a lost wakeup or leaving a
/// recontending waiter eligible for another notification.
///
/// # Type Parameters
///
/// * `G` - Backend-specific state-guard type.
/// * `L` - Backend-specific state-guard reacquisition operation.
///
/// # Parameters
///
/// * `guard` - Slot containing the currently held state guard.
/// * `waiters` - Registry receiving the temporary blocking waiter.
/// * `missing_guard_message` - Invariant message used when the slot is empty.
/// * `reacquire` - Operation that reacquires the backend state guard.
///
/// # Panics
///
/// Panics with `missing_guard_message` when the state guard was already
/// released. Propagates a panic from `reacquire` or waiter registration.
#[inline]
pub(in crate::monitor) fn wait_for_notification<G, L>(
    guard: &mut Option<G>,
    waiters: &BlockingWaiterRegistry,
    missing_guard_message: &'static str,
    reacquire: L,
) where
    L: FnOnce() -> G,
{
    let registration = waiters.register();
    release_guard(guard, missing_guard_message);
    registration.waiter().wait();
    drop(registration);
    *guard = Some(reacquire());
}

/// Waits for monitor notification or Timer completion while releasing and then
/// reacquiring a backend-specific state guard.
///
/// The Timer is first polled without a waiter so an already-complete deadline
/// avoids registration. A pending Timer is then polled with the registered
/// waiter's Waker before the state guard is released. After wakeup, this
/// function removes the registration, reacquires the state guard, and performs
/// one final Timer poll before reporting the outcome.
///
/// # Type Parameters
///
/// * `G` - Backend-specific state-guard type.
/// * `L` - Backend-specific state-guard reacquisition operation.
///
/// # Parameters
///
/// * `guard` - Slot containing the currently held state guard.
/// * `waiters` - Registry receiving the temporary blocking waiter.
/// * `future` - Fixed Timer registration raced against notification.
/// * `missing_guard_message` - Invariant message used when the slot is empty.
/// * `reacquire` - Operation that reacquires the backend state guard.
///
/// # Returns
///
/// [`WaitTimeoutStatus::Woken`] when notification wins the final Timer poll,
/// or [`WaitTimeoutStatus::TimedOut`] when the Timer completes.
///
/// # Errors
///
/// Returns an error reported by `future`. After the state guard has been
/// released, the guard is reacquired before a Timer completion error returns.
///
/// # Panics
///
/// Panics with `missing_guard_message` when the state guard was already
/// released. Propagates a panic from `reacquire` or waiter registration.
pub(in crate::monitor) fn wait_with_timer<G, L>(
    guard: &mut Option<G>,
    waiters: &BlockingWaiterRegistry,
    future: &mut TimerFuture,
    missing_guard_message: &'static str,
    reacquire: L,
) -> Result<WaitTimeoutStatus, TimeError>
where
    L: FnOnce() -> G,
{
    if let Poll::Ready(result) =
        BlockingConditionWaiter::poll_timer_without_waiter(future)
    {
        return result.map(|()| WaitTimeoutStatus::TimedOut);
    }
    let registration = waiters.register();
    let waiter = Arc::clone(registration.waiter());
    if let Poll::Ready(result) =
        BlockingConditionWaiter::poll_timer(&waiter, future)
    {
        return result.map(|()| WaitTimeoutStatus::TimedOut);
    }
    release_guard(guard, missing_guard_message);
    waiter.wait();
    drop(registration);
    *guard = Some(reacquire());
    match BlockingConditionWaiter::poll_timer(&waiter, future) {
        Poll::Ready(result) => result.map(|()| WaitTimeoutStatus::TimedOut),
        Poll::Pending => Ok(WaitTimeoutStatus::Woken),
    }
}
