// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous condition-wait capability.

use std::{
    future::Future,
    sync::Arc,
};

/// Waits asynchronously for predicates over protected monitor state.
///
/// Returned futures are lazy: constructing one neither acquires the state lock
/// nor registers a waiter. Once polling reaches a blocking predicate check,
/// the waiter is registered before the state lock is released. Notifications
/// have memoryless condition-variable semantics, so every wakeup rechecks the
/// predicate and no fairness is guaranteed.
///
/// # External predicate state
///
/// If a predicate reads state outside [`Self::State`], every predicate-changing
/// update must participate in the same monitor lock handshake before notifying
/// the monitor. Atomic ordering alone cannot close the scheduling window
/// between the waiter's predicate check and waiter registration. Use the
/// [`crate::monitor::AsyncMonitor::with_write_notify_all_async`] to update that
/// external state while holding the monitor lock and then notify waiters.
///
/// ```
/// use std::sync::{
///     Arc,
///     atomic::{
///         AtomicBool,
///         Ordering,
///     },
/// };
///
/// use qubit_lock::{
///     AsyncConditionWaiter,
///     TokioMonitor,
/// };
///
/// # #[tokio::main]
/// # async fn main() {
/// let ready = Arc::new(AtomicBool::new(false));
/// let monitor = Arc::new(TokioMonitor::current(()));
/// let waiter_ready = Arc::clone(&ready);
/// let waiter_monitor = monitor.clone();
///
/// let waiter = tokio::spawn(async move {
///     waiter_monitor
///         .wait_until_async(
///             |_| waiter_ready.load(Ordering::Acquire),
///             |_| (),
///         )
///         .await;
/// });
///
/// monitor
///     .with_write_notify_all_async(|_| {
///         ready.store(true, Ordering::Release);
///     })
///     .await;
/// waiter.await.expect("waiter should finish");
/// # }
/// ```
///
/// Dropping a pending future cancels and unregisters its active wait. It does
/// not run the action or roll back protected-state changes made while the wait
/// existed. If a notification already selected that waiter, cancellation
/// discards the selection rather than transferring it to another or future
/// waiter.
///
/// Use this trait as a static generic bound when asynchronous code needs
/// predicate waits but no timeout. Its return-position `impl Future` methods
/// make it unsuitable as a `dyn` trait-object interface.
pub trait AsyncConditionWaiter {
    /// State protected by the monitor.
    type State;

    /// Returns a future that waits until the predicate becomes true.
    ///
    /// The predicate and action run while the monitor state is locked. The
    /// trait-level laziness and cancellation contract applies to the returned
    /// future.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to the value returned by `action`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled.
    #[inline(always)]
    fn wait_until_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_async(move |state| !predicate(state), action)
    }

    /// Returns a future that waits until the predicate becomes true.
    ///
    /// The predicate runs while the monitor state is locked. This convenience
    /// method does not run an action after the predicate becomes true. The
    /// trait-level laziness and cancellation contract applies to the returned
    /// future.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to `()` when the predicate becomes true.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` when it is
    /// polled.
    #[inline(always)]
    fn wait_until_ready_async<'a, P>(
        &'a self,
        predicate: P,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        P: FnMut(&Self::State) -> bool + Send + 'a,
    {
        self.wait_until_async(predicate, |_| ())
    }

    /// Returns a future that waits while the predicate remains true.
    ///
    /// The predicate and action run while the monitor state is locked. The
    /// trait-level laziness and cancellation contract applies to the returned
    /// future.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A lazy future that resolves to the value returned by `action`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled.
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a;
}

impl<M> AsyncConditionWaiter for Arc<M>
where
    M: AsyncConditionWaiter + ?Sized,
{
    type State = M::State;

    /// Forwards the asynchronous wait to the wrapped monitor.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The future returned by the wrapped monitor.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from the wrapped monitor,
    /// including from `predicate` or `action`, when it is polled.
    #[inline(always)]
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.as_ref().wait_while_async(predicate, action)
    }
}
