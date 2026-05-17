/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Asynchronous timeout condition-wait capability.

use std::time::Duration;

use crate::monitor::{
    AsyncConditionWaiter,
    AsyncMonitorFuture,
    WaitTimeoutResult,
};

/// Waits asynchronously for predicates over protected state with timeouts.
pub trait AsyncTimeoutConditionWaiter: AsyncConditionWaiter {
    /// Returns a future that waits until the predicate becomes true or times out.
    ///
    /// The timeout budget is measured from this method call.
    fn async_wait_until_for<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;

    /// Returns a future that waits while the predicate remains true or times out.
    ///
    /// The timeout budget is measured from this method call.
    fn async_wait_while_for<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}
