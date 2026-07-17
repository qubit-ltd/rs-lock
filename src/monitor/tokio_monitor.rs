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
    MonotonicClock,
    TimeError,
    Timer,
    TokioMonotonicClock,
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
/// by one final locked predicate check. The default Tokio Timer requires a
/// runtime with the time driver enabled; injected Timers may have different
/// runtime requirements. Initial mutex contention, an immediately ready
/// predicate, and a zero budget do not create a Timer future.
pub struct TokioMonitor<T> {
    /// Protected monitor state.
    state: Mutex<T>,
    /// Active condition waiters eligible for memoryless notification.
    waiters: StdMutex<BTreeMap<usize, Arc<TokioConditionWaiter>>>,
    /// Timer driving every asynchronous deadline wait.
    timer: Arc<dyn Timer>,
}

impl<T> TokioMonitor<T> {
    /// Creates an asynchronous monitor protecting the supplied state.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A Tokio-based monitor.
    #[inline]
    pub fn new(state: T) -> Self {
        Self::with_timer(state, TokioMonotonicClock::new().new_timer())
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
    /// A Tokio monitor bound to `timer`.
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
    pub fn timer(&self) -> &dyn Timer {
        self.timer.as_ref()
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// # Arguments
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
    /// # Arguments
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
    /// # Arguments
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
    /// # Arguments
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
    /// Timer requires a runtime with its time driver enabled; injected Timers
    /// may have different runtime requirements. Registration time consumes the
    /// budget. Initial mutex contention, an immediately ready predicate, and a
    /// zero budget do not create a Timer future. The fixed deadline is reused
    /// across wakeups and followed by one
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
    /// Timer requires a runtime with its time driver enabled; injected Timers
    /// may have different runtime requirements. Registration time consumes the
    /// budget. Initial mutex contention, an immediately ready predicate, and a
    /// zero budget do not create a Timer future. The fixed deadline is reused
    /// across wakeups and followed by one
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
                let timed_out = {
                    let notified = registration.waiter().signal().notified();
                    tokio::pin!(notified);
                    poll_fn(|context| {
                        if deadline.as_mut().poll(context).is_ready() {
                            Poll::Ready(true)
                        } else if notified.as_mut().poll(context).is_ready() {
                            Poll::Ready(false)
                        } else {
                            Poll::Pending
                        }
                    })
                    .await
                };
                drop(registration);
                if timed_out {
                    guard = self.state.lock().await;
                    if !predicate(&*guard) {
                        return Ok(WaitTimeoutResult::Ready(action(
                            &mut *guard,
                        )));
                    }
                    return Ok(WaitTimeoutResult::TimedOut);
                }
                guard = self.state.lock().await;
                if !predicate(&*guard) {
                    return Ok(WaitTimeoutResult::Ready(action(&mut *guard)));
                }
                let deadline_reached = poll_fn(|context| {
                    Poll::Ready(deadline.as_mut().poll(context).is_ready())
                })
                .await;
                if deadline_reached {
                    return Ok(WaitTimeoutResult::TimedOut);
                }
            }
        }
    }
}

impl<T> From<T> for TokioMonitor<T> {
    /// Creates a Tokio monitor from an initial state value.
    #[inline(always)]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for TokioMonitor<T> {
    /// Creates a Tokio monitor containing `T::default()`.
    #[inline(always)]
    fn default() -> Self {
        Self::new(T::default())
    }
}
