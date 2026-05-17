/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Asynchronous condition-wait capability.

use crate::monitor::AsyncMonitorFuture;

/// Waits asynchronously for predicates over protected monitor state.
pub trait AsyncConditionWaiter {
    /// State protected by the monitor.
    type State;

    /// Returns a future that waits until the predicate becomes true.
    ///
    /// The predicate and action run while the monitor state is locked.
    fn async_wait_until<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;

    /// Returns a future that waits while the predicate remains true.
    ///
    /// The predicate and action run while the monitor state is locked.
    fn async_wait_while<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}
