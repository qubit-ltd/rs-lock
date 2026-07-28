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
///
/// # External predicate state
///
/// If a predicate reads state stored outside [`Self::State`], every update
/// that may change the predicate must participate in the same monitor lock
/// handshake. The updater acquires the monitor lock, changes the external
/// state, releases the lock, and notifies the monitor. Atomic ordering alone
/// cannot prevent a notification from falling between the waiter's predicate
/// check and waiter registration.
///
/// The [`crate::monitor::Monitor`] trait provides combined helpers such as
/// [`crate::monitor::Monitor::with_write_notify_all`] for this protocol:
///
/// ```
/// use std::{
///     sync::{
///         Arc,
///         atomic::{
///             AtomicBool,
///             Ordering,
///         },
///     },
///     thread,
/// };
///
/// use qubit_lock::StdMonitor;
///
/// let ready = Arc::new(AtomicBool::new(false));
/// let monitor = Arc::new(StdMonitor::new(()));
/// let waiter_ready = Arc::clone(&ready);
/// let waiter_monitor = monitor.clone();
///
/// let waiter = thread::spawn(move || {
///     waiter_monitor.wait_until(
///         |_| waiter_ready.load(Ordering::Acquire),
///         |_| (),
///     );
/// });
///
/// monitor.with_write_notify_all(|_| {
///     ready.store(true, Ordering::Release);
/// });
/// waiter.join().expect("waiter should finish");
/// ```
///
/// Use this trait as a static generic bound when blocking code needs predicate
/// waits but no timeout. Its generic methods make it unsuitable as a `dyn`
/// trait-object interface.
pub trait ConditionWaiter {
    /// State protected by the monitor.
    type State;

    /// Blocks until the predicate becomes true, then runs an action.
    ///
    /// The predicate and action run while the monitor state is locked.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate` or `action`.
    #[inline(always)]
    fn wait_until<R, P, F>(&self, mut predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while(move |state| !predicate(state), action)
    }

    /// Blocks until the predicate becomes true.
    ///
    /// The predicate runs while the monitor state is locked. This convenience
    /// method does not run an action after the predicate becomes true.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate`.
    #[inline(always)]
    fn wait_until_ready<P>(&self, predicate: P)
    where
        P: FnMut(&Self::State) -> bool,
    {
        self.wait_until(predicate, |_| ());
    }

    /// Blocks while the predicate remains true, then runs an action.
    ///
    /// The predicate and action run while the monitor state is locked.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate` or `action`.
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
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that remains true while waiting continues.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from the wrapped monitor, including from `predicate`
    /// or `action`.
    #[inline(always)]
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <M as ConditionWaiter>::wait_while(self.as_ref(), predicate, action)
    }
}
