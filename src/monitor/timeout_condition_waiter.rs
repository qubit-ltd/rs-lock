/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Blocking timeout condition-wait capability.

use std::time::Duration;

use crate::monitor::{
    ConditionWaiter,
    WaitTimeoutResult,
};

/// Waits for predicates over protected state with relative timeouts.
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
    /// [`WaitTimeoutResult::TimedOut`] when the timeout expires first.
    fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R;

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
    /// [`WaitTimeoutResult::TimedOut`] when the timeout expires first.
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
