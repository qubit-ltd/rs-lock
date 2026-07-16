// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Blocking condition-wait capability.

use std::sync::Arc;

/// Waits for predicates over protected monitor state.
///
/// Predicates and actions run while the state is locked. A wakeup only causes
/// the predicate to be checked again: notifications have memoryless
/// condition-variable semantics, may be spurious, and do not guarantee
/// fairness or make the predicate true. The action runs only after the
/// predicate reaches its completion condition.
pub trait ConditionWaiter {
    /// State protected by the monitor.
    type State;

    /// Blocks until the predicate becomes true, then runs an action.
    ///
    /// The predicate and action run while the monitor state is locked.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    #[inline(always)]
    fn wait_until<R, P, F>(&self, mut predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while(move |state| !predicate(state), action)
    }

    /// Blocks while the predicate remains true, then runs an action.
    ///
    /// The predicate and action run while the monitor state is locked.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R;
}

impl<M: ?Sized> ConditionWaiter for Arc<M>
where
    M: ConditionWaiter,
{
    type State = M::State;

    /// Delegates a blocking condition wait to the shared monitor.
    #[inline(always)]
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <M as ConditionWaiter>::wait_while(self.as_ref(), predicate, action)
    }
}
