// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tokio-based asynchronous monitor.

use std::{
    collections::BTreeMap,
    future::{
        Future,
        poll_fn,
    },
    sync::{
        Arc,
        Mutex as StdMutex,
    },
    task::Poll,
    time::Duration,
};

use qubit_clock::{
    TimeError,
    Timer,
    TimerFuture,
    TokioRuntimeError,
    TokioTimer,
};
use tokio::sync::Mutex;

use super::{
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
    Notifier,
    WaitTimeoutResult,
    internal::{
        TokioConditionWaiter,
        TokioConditionWaiterRegistration,
    },
};

/// Asynchronous monitor built on Tokio synchronization primitives.
///
/// `TokioMonitor` protects one state value with a Tokio mutex and coordinates
/// waiters with a Tokio notification primitive. Notifications have memoryless
/// condition-variable semantics: they select already registered waiters but
/// carry no protected state, so every wake is followed by a predicate recheck.
/// Waiter selection has no fairness or FIFO guarantee.
///
/// Dropping a pending condition-wait future cancels the wait, releases any held
/// state guard, and unregisters its Tokio notification waiter without running
/// the action or rolling back protected-state changes. If `notify_one` has
/// already selected that waiter, cancellation discards that selection instead
/// of transferring it to another or future waiter. After an initial predicate
/// check requires waiting with a nonzero budget, a timed wait creates one timer
/// before registering its first waiter and reuses that fixed deadline across
/// wakeups. Registration and state-reacquisition time consume the
/// condition-wait budget; a signal cannot restart or extend it. When a signal
/// and the deadline are both ready, the deadline is selected first, followed
/// by one final locked predicate check. The default Tokio timer captures a
/// runtime handle during monitor construction. Its target runtime must remain
/// alive with time enabled and be driven while a timed wait is pending, though
/// the wait future may be polled from another runtime context. Injected timers
/// retain their own progress requirements. Initial mutex contention, an
/// immediately ready predicate, and a zero budget do not create a timer future.
pub struct TokioMonitor<T> {
    /// Protected monitor state.
    state: Mutex<T>,
    /// Active condition waiters eligible for memoryless notification.
    waiters: StdMutex<BTreeMap<usize, Arc<TokioConditionWaiter>>>,
    /// Timer driving every asynchronous deadline wait.
    timer: Arc<dyn Timer>,
}

impl<T> TokioMonitor<T> {
    /// Creates a monitor by capturing the currently entered Tokio runtime.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A Tokio monitor retaining the current runtime's timer capability.
    ///
    /// # Panics
    ///
    /// Panics when no Tokio runtime is entered or all process-wide clock-domain
    /// identifiers are exhausted.
    #[must_use]
    #[track_caller]
    #[inline]
    pub fn current(state: T) -> Self {
        Self::try_current(state).unwrap_or_else(|error| {
            panic!("cannot create Tokio monitor: {error}")
        })
    }

    /// Tries to create a monitor by capturing the current Tokio runtime.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A Tokio monitor retaining the current runtime's timer capability.
    ///
    /// # Errors
    ///
    /// Returns [`TokioRuntimeError::NotEntered`] when no Tokio runtime is
    /// entered.
    ///
    /// # Panics
    ///
    /// Panics if all process-wide clock-domain identifiers are exhausted.
    #[inline]
    pub fn try_current(state: T) -> Result<Self, TokioRuntimeError> {
        TokioTimer::try_current()
            .map(|timer| Self::with_timer(state, Arc::new(timer)))
    }

    /// Creates a Tokio monitor using an injected Timer.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial protected state.
    /// * `timer` - Timer driving asynchronous deadlines.
    ///
    /// # Returns
    ///
    /// A Tokio monitor using `timer`.
    ///
    /// The monitor does not drive the injected backend. Its owner must keep the
    /// timer's clock and deadline driver alive and progressing while waits are
    /// pending.
    #[inline]
    pub fn with_timer(state: T, timer: Arc<dyn Timer>) -> Self {
        Self {
            state: Mutex::new(state),
            waiters: StdMutex::new(BTreeMap::new()),
            timer,
        }
    }

    /// Returns the Timer driving this monitor's deadline waits.
    ///
    /// # Returns
    ///
    /// The injected Timer and its monotonic clock domain.
    #[must_use]
    #[inline(always)]
    pub fn timer(&self) -> &dyn Timer {
        self.timer.as_ref()
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives an immutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    #[inline]
    pub async fn with_read_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.state.lock().await;
        f(&*guard)
    }

    /// Acquires the monitor and mutates the protected state.
    ///
    /// This does not notify waiters automatically.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    #[inline]
    pub async fn with_write_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.state.lock().await;
        f(&mut *guard)
    }

    /// Mutates the protected state and wakes one waiter.
    ///
    /// The state lock is released before notification is sent. If `f` panics,
    /// the panic propagates and no notification is sent.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    #[inline]
    pub async fn with_write_notify_one_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write_async(f).await;
        self.notify_one();
        result
    }

    /// Mutates the protected state and wakes all waiters.
    ///
    /// The state lock is released before notification is sent. If `f` panics,
    /// the panic propagates and no notification is sent.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    #[inline]
    pub async fn with_write_notify_all_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write_async(f).await;
        self.notify_all();
        result
    }

    /// Selects at most one registered async waiter without a fairness
    /// guarantee.
    #[inline]
    pub fn notify_one(&self) {
        let waiter = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_last()
            .map(|(_, waiter)| waiter);
        if let Some(waiter) = waiter {
            waiter.signal().notify_one();
        }
    }

    /// Selects all registered async waiters without retaining protected state.
    pub fn notify_all(&self) {
        let waiters = {
            let mut registry = self
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *registry)
        };
        for waiter in waiters.into_values() {
            waiter.signal().notify_one();
        }
    }

    /// Registers one waiter while the protected state lock is still held.
    ///
    /// # Returns
    ///
    /// A registration that removes the waiter if it is cancelled or leaves the
    /// wait before notification selects it.
    #[inline]
    fn register_waiter(&self) -> TokioConditionWaiterRegistration<'_> {
        let waiter = Arc::new(TokioConditionWaiter::new());
        let waiter_key = Arc::as_ptr(&waiter) as usize;
        let previous = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(waiter_key, Arc::clone(&waiter));
        assert!(previous.is_none(), "Tokio monitor waiter pointer reused");
        TokioConditionWaiterRegistration::new(&self.waiters, waiter)
    }

    /// Polls a fixed Timer registration once after state reacquisition.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Timer registration shared across condition wakeups.
    ///
    /// # Returns
    ///
    /// `true` when the deadline has completed.
    #[inline]
    async fn deadline_reached(
        deadline: &mut TimerFuture,
    ) -> Result<bool, TimeError> {
        poll_fn(|context| match deadline.as_mut().poll(context) {
            Poll::Pending => Poll::Ready(Ok(false)),
            Poll::Ready(result) => Poll::Ready(result.map(|()| true)),
        })
        .await
    }
}

impl<T> Notifier for TokioMonitor<T> {
    /// Selects at most one registered async waiter without a fairness
    /// guarantee.
    #[inline(always)]
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Selects all registered async waiters without retaining protected state.
    #[inline(always)]
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send> AsyncConditionWaiter for TokioMonitor<T> {
    type State = T;

    /// Returns a future that rechecks the protected predicate until it becomes
    /// true.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
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

    /// Returns a future that rechecks the protected predicate while it remains
    /// true.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
    #[allow(
        clippy::manual_async_fn,
        reason = "the explicit Send bound is part of the trait contract"
    )]
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        async move {
            let mut guard = self.state.lock().await;
            while predicate(&*guard) {
                let registration = self.register_waiter();
                drop(guard);
                registration.waiter().signal().notified().await;
                drop(registration);
                guard = self.state.lock().await;
            }
            action(&mut *guard)
        }
    }
}

impl<T: Send> AsyncTimeoutConditionWaiter for TokioMonitor<T> {
    /// Returns a future that rechecks the predicate until it becomes true or
    /// the timeout expires.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
    /// After an initial blocking predicate with a nonzero budget, the method
    /// creates one Timer future before waiter registration. The default Tokio
    /// timer uses its retained runtime handle; that runtime must stay alive,
    /// have time enabled, and be driven while the wait is pending. Injected
    /// timers retain their own progress requirements. Registration time
    /// consumes the budget. Initial mutex contention, an immediately ready
    /// predicate, and a zero budget do not create a Timer future. The fixed
    /// deadline is reused across wakeups and followed by one
    /// final locked predicate check. Predicate readiness wins over timeout. If
    /// a signal wins before the timer is ready but reacquiring the state
    /// exhausts the fixed deadline, a still-blocking predicate times out
    /// without another waiter registration. When the signal and deadline are
    /// both ready, the deadline is selected first. A zero timeout still checks
    /// the predicate once.
    #[inline(always)]
    fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
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

    /// Returns a future that rechecks the predicate while it remains true or
    /// until the timeout expires.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
    /// After an initial blocking predicate with a nonzero budget, the method
    /// creates one Timer future before waiter registration. The default Tokio
    /// timer uses its retained runtime handle; that runtime must stay alive,
    /// have time enabled, and be driven while the wait is pending. Injected
    /// timers retain their own progress requirements. Registration time
    /// consumes the budget. Initial mutex contention, an immediately ready
    /// predicate, and a zero budget do not create a Timer future. The fixed
    /// deadline is reused across wakeups and followed by one
    /// final locked predicate check. Predicate readiness wins over timeout. If
    /// a signal wins before the timer is ready but reacquiring the state
    /// exhausts the fixed deadline, a still-blocking predicate times out
    /// without another waiter registration. When the signal and deadline are
    /// both ready, the deadline is selected first. A zero timeout still checks
    /// the predicate once.
    #[allow(
        clippy::manual_async_fn,
        reason = "the explicit Send bound is part of the trait contract"
    )]
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        async move {
            let mut guard = self.state.lock().await;
            if !predicate(&*guard) {
                return Ok(WaitTimeoutResult::Ready(action(&mut *guard)));
            }
            if timeout.is_zero() {
                return Ok(WaitTimeoutResult::TimedOut);
            }

            let mut deadline = self.timer.after(timeout)?;
            loop {
                let registration = self.register_waiter();
                drop(guard);
                let status = registration
                    .wait_until_signalled_or_deadline(&mut deadline)
                    .await;
                drop(registration);
                guard = self.state.lock().await;
                if !predicate(&*guard) {
                    return Ok(WaitTimeoutResult::Ready(action(&mut *guard)));
                }
                if status?.is_timed_out()
                    || Self::deadline_reached(&mut deadline).await?
                {
                    return Ok(WaitTimeoutResult::TimedOut);
                }
            }
        }
    }
}
