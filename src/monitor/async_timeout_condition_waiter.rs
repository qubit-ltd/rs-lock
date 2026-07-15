// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous timeout condition-wait capability.

use std::{
    future::Future,
    time::Duration,
};

use crate::monitor::{
    AsyncConditionWaiter,
    WaitTimeoutResult,
};

/// Waits asynchronously for predicates over protected state with timeouts.
pub trait AsyncTimeoutConditionWaiter: AsyncConditionWaiter {
    /// Returns a future that waits until the predicate becomes true or times
    /// out.
    ///
    /// The returned future is lazy. After it is first polled, the monitor
    /// acquires the state lock and checks the predicate once before starting
    /// the timeout budget immediately before the first suspension. Initial
    /// lock contention and time before the first poll do not consume the
    /// budget. One fixed deadline is reused across wakeups. At the deadline,
    /// the predicate is checked once more under the state lock; readiness wins
    /// over timeout. A zero timeout still performs the initial locked check.
    fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_for_async(
            timeout,
            move |state| !predicate(state),
            action,
        )
    }

    /// Returns a future that waits while the predicate remains true or times
    /// out.
    ///
    /// The returned future is lazy. After it is first polled, the monitor
    /// acquires the state lock and checks the predicate once before starting
    /// the timeout budget immediately before the first suspension. Initial
    /// lock contention and time before the first poll do not consume the
    /// budget. One fixed deadline is reused across wakeups. At the deadline,
    /// the predicate is checked once more under the state lock; readiness wins
    /// over timeout. A zero timeout still performs the initial locked check.
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}
