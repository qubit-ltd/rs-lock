// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared loop for blocking predicate waits with one fixed timer future.

use std::ops::DerefMut;

use qubit_clock::{
    TimeError,
    TimerFuture,
};

use crate::monitor::{
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

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
