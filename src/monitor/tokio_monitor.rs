// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tokio-based asynchronous monitor.

use std::{
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
    MonotonicInstant,
    TimeError,
    Timer,
    TimerFuture,
    TokioRuntimeError,
    TokioTimer,
};
use tokio::sync::Mutex;

use super::{
    AsyncConditionWaiter,
    AsyncMonitor,
    AsyncTimeoutConditionWaiter,
    Notifier,
    WaitTimeoutResult,
    internal::{
        TokioConditionWaiter,
        TokioConditionWaiterRegistration,
        WaiterRegistry,
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
/// of transferring it to another or future waiter. Timed waits align with
/// [`std::sync::Condvar::wait_timeout_while`]: after acquiring the state lock
/// and before the first predicate check, they sample one fixed deadline. The
/// initial mutex contention is excluded, but predicate work, registration, and
/// waiting consume the condition-wait budget; a signal cannot restart or
/// extend it. A timed wait may return after the timeout while reacquiring the
/// state lock. When a signal and the deadline are both ready, the deadline is
/// selected first, followed by one final locked predicate check. The default
/// Tokio timer captures a
/// runtime handle during monitor construction. Its target runtime must remain
/// alive with time enabled and be driven while a timed wait is pending, though
/// the wait future may be polled from another runtime context. Injected timers
/// retain their own progress requirements. An immediately ready predicate and
/// a zero budget do not create a timer future. Closures and predicates execute
/// while the state mutex is held and must not re-enter the same monitor; doing
/// so can deadlock.
///
/// # Type Parameters
///
/// * `T` - State protected by the Tokio mutex.
#[must_use = "retain and use the monitor to coordinate protected state"]
pub struct TokioMonitor<T> {
    /// Protected monitor state.
    state: Mutex<T>,
    /// Active condition waiters selected in FIFO registration order.
    waiters: StdMutex<WaiterRegistry<Arc<TokioConditionWaiter>>>,
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
            waiters: StdMutex::new(WaiterRegistry::new()),
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
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to read the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives an immutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`.
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
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to mutate the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`.
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
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to mutate the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`. In that case no notification is sent.
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
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to mutate the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`. In that case no notification is sent.
    #[inline]
    pub async fn with_write_notify_all_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write_async(f).await;
        self.notify_all();
        result
    }

    /// Selects at most one registered async waiter without a fairness or FIFO
    /// guarantee.
    ///
    /// This does not guarantee scheduling or mutex reacquisition order.
    #[inline]
    pub fn notify_one(&self) {
        let waiter = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_one();
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
            registry.take_all()
        };
        for waiter in waiters {
            waiter.signal().notify_one();
        }
    }

    /// Registers one waiter while the protected state lock is still held.
    ///
    /// # Returns
    ///
    /// A registration that removes the waiter if it is cancelled or leaves the
    /// wait before notification selects it.
    ///
    /// # Panics
    ///
    /// Panics if the registry exhausts registration identifiers.
    #[inline]
    fn register_waiter(&self) -> TokioConditionWaiterRegistration<'_> {
        let waiter = Arc::new(TokioConditionWaiter::new());
        let waiter_id = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .register(Arc::clone(&waiter));
        TokioConditionWaiterRegistration::new(&self.waiters, waiter_id, waiter)
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
    ///
    /// # Errors
    ///
    /// Returns a Timer completion error if the fixed registration fails while
    /// being polled.
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
    /// Selects at most one registered async waiter without a fairness or FIFO
    /// guarantee.
    ///
    /// This does not guarantee scheduling or mutex reacquisition order.
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
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `action`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate returning `true` when the caller may continue.
    /// * `action` - Action receiving mutable state once `predicate` succeeds.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `action`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled. It also panics if the registry exhausts registration
    /// identifiers.
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
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `action`.
    /// * `P` - Predicate deciding whether waiting should continue.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `predicate` - Predicate returning `true` while the caller must wait.
    /// * `action` - Action receiving mutable state once `predicate` is false.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `action`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled. It also panics if the registry exhausts registration
    /// identifiers.
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

impl<T: Send> AsyncMonitor for TokioMonitor<T> {
    /// Acquires the monitor and reads the protected state.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closure, and returned future.
    /// * `R` - Value returned by `f`.
    /// * `F` - Read-only operation performed while holding the state lock.
    ///
    /// # Parameters
    ///
    /// * `f` - Operation receiving shared access to the protected state.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `f` when it is polled.
    #[inline(always)]
    fn with_read_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&Self::State) -> R + Send + 'a,
    {
        TokioMonitor::with_read_async(self, f)
    }

    /// Acquires the monitor and mutates the protected state.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closure, and returned future.
    /// * `R` - Value returned by `f`.
    /// * `F` - Mutating operation performed while holding the state lock.
    ///
    /// # Parameters
    ///
    /// * `f` - Operation receiving mutable access to the protected state.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `f` when it is polled.
    #[inline(always)]
    fn with_write_async<'a, R, F>(
        &'a self,
        f: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        TokioMonitor::with_write_async(self, f)
    }
}

impl<T: Send> TokioMonitor<T> {
    /// Returns a future that waits until a predicate becomes true or an
    /// absolute deadline passes.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the injected Timer's clock domain.
    /// * `ready` - Predicate returning `true` when the caller may continue.
    /// * `f` - Action receiving mutable state once `ready` succeeds.
    ///
    /// # Returns
    ///
    /// A future resolving to [`WaitTimeoutResult::Ready`] with the value
    /// returned by `f`, or [`WaitTimeoutResult::TimedOut`] if the deadline is
    /// reached first.
    ///
    /// # Errors
    ///
    /// The returned future reports Timer domain, registration, or completion
    /// errors when waiting is required.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `ready` or `f` when polled.
    /// It also panics if the waiter registry exhausts registration identifiers.
    #[inline(always)]
    pub fn wait_until_with_deadline_async<'a, R, P, F>(
        &'a self,
        deadline: MonotonicInstant,
        ready: P,
        f: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&T) -> bool + Send + 'a,
        F: FnOnce(&mut T) -> R + Send + 'a,
    {
        <Self as AsyncTimeoutConditionWaiter>::wait_until_with_deadline_async(
            self, deadline, ready, f,
        )
    }

    /// Returns a future that waits until a predicate becomes true or an
    /// absolute deadline passes without running an action.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, predicate, and returned future.
    /// * `P` - Predicate deciding when the state is ready.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the injected Timer's clock domain.
    /// * `ready` - Predicate returning `true` when the caller may continue.
    ///
    /// # Returns
    ///
    /// A future resolving to [`WaitTimeoutResult::Ready`] with `()`, or
    /// [`WaitTimeoutResult::TimedOut`] if the deadline is reached first.
    ///
    /// # Errors
    ///
    /// The returned future reports Timer domain, registration, or completion
    /// errors when waiting is required.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `ready` when polled. It also
    /// panics if the waiter registry exhausts registration identifiers.
    #[inline(always)]
    pub fn wait_until_ready_with_deadline_async<'a, P>(
        &'a self,
        deadline: MonotonicInstant,
        ready: P,
    ) -> impl Future<Output = Result<WaitTimeoutResult<()>, TimeError>> + Send + 'a
    where
        P: FnMut(&T) -> bool + Send + 'a,
    {
        <Self as AsyncTimeoutConditionWaiter>::wait_until_ready_with_deadline_async(
            self, deadline, ready,
        )
    }

    /// Returns a future that waits while a predicate remains true or until an
    /// absolute deadline passes.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `action`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the injected Timer's clock domain.
    /// * `predicate` - Predicate returning `true` while waiting must continue.
    /// * `action` - Action receiving mutable state when waiting finishes.
    ///
    /// # Returns
    ///
    /// A future resolving to [`WaitTimeoutResult::Ready`] with the value
    /// returned by `action`, or [`WaitTimeoutResult::TimedOut`] if the deadline
    /// is reached while `predicate` remains true.
    ///
    /// # Errors
    ///
    /// The returned future reports Timer domain, registration, or completion
    /// errors when waiting is required.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// polled. It also panics if the waiter registry exhausts registration
    /// identifiers.
    #[inline(always)]
    pub fn wait_while_with_deadline_async<'a, R, P, F>(
        &'a self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&T) -> bool + Send + 'a,
        F: FnOnce(&mut T) -> R + Send + 'a,
    {
        <Self as AsyncTimeoutConditionWaiter>::wait_while_with_deadline_async(
            self, deadline, predicate, action,
        )
    }
}

impl<T: Send> AsyncTimeoutConditionWaiter for TokioMonitor<T> {
    /// Returns a future that waits while a predicate remains true or until an
    /// absolute deadline passes.
    ///
    /// The deadline includes time before the first poll, async mutex
    /// contention, predicate evaluation, and all subsequent waits. A ready
    /// predicate wins even when the deadline has passed.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `action`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `deadline_at` - Absolute deadline in the injected Timer's clock
    ///   domain.
    /// * `predicate` - Predicate returning `true` while waiting must continue.
    /// * `action` - Action receiving mutable state when waiting finishes.
    ///
    /// # Returns
    ///
    /// A future resolving to [`WaitTimeoutResult::Ready`] with the value
    /// returned by `action`, or [`WaitTimeoutResult::TimedOut`] if the deadline
    /// is reached while `predicate` remains true.
    ///
    /// # Errors
    ///
    /// The returned future reports Timer domain, registration, or completion
    /// errors when waiting is required.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// polled. It also panics if the waiter registry exhausts registration
    /// identifiers.
    #[allow(
        clippy::manual_async_fn,
        reason = "the explicit Send bound is part of the trait contract"
    )]
    fn wait_while_with_deadline_async<'a, R, P, F>(
        &'a self,
        deadline_at: MonotonicInstant,
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

            let mut deadline = self.timer.at(deadline_at)?;
            if Self::deadline_reached(&mut deadline).await? {
                return Ok(WaitTimeoutResult::TimedOut);
            }
            loop {
                let registration = self.register_waiter();
                drop(guard);
                let status = registration
                    .wait_until_signalled_or_deadline(&mut deadline)
                    .await;
                drop(registration);
                guard = self.state.lock().await;
                let status = status?;
                let deadline_reached = if status.is_timed_out() {
                    true
                } else {
                    Self::deadline_reached(&mut deadline).await?
                };
                if !predicate(&*guard) {
                    return Ok(WaitTimeoutResult::Ready(action(&mut *guard)));
                }
                if deadline_reached {
                    return Ok(WaitTimeoutResult::TimedOut);
                }
            }
        }
    }

    /// Returns a future that rechecks the predicate until it becomes true or
    /// the timeout expires.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
    /// The timeout is aligned with [`std::sync::Condvar::wait_timeout_while`]:
    /// after acquiring the state lock and before the first predicate check, the
    /// method samples one fixed deadline. Initial mutex contention is excluded,
    /// but predicate work, registration, and waiting consume the budget. The
    /// method may return after the timeout while reacquiring the state lock.
    /// An immediately ready predicate and a zero budget do not create a Timer
    /// future. The default Tokio timer uses its retained runtime handle; that
    /// runtime must stay alive, have time enabled, and be driven while the wait
    /// is pending. Injected timers retain their own progress requirements.
    /// Predicate readiness wins over a successful timeout, while a Timer
    /// registration or completion error takes precedence over every post-wait
    /// predicate result and prevents `action` from running. If a signal wins
    /// before the timer is ready but reacquiring the state exhausts the fixed
    /// deadline, a still-blocking predicate times out without another waiter
    /// registration. When the signal and deadline are both ready, the deadline
    /// is selected first. A zero timeout still checks the predicate once.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `action`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget fixed after acquiring the
    ///   state lock.
    /// * `predicate` - Predicate returning `true` when the caller may continue.
    /// * `action` - Action receiving mutable state once `predicate` succeeds.
    ///
    /// # Returns
    ///
    /// A future resolving to [`WaitTimeoutResult::Ready`] with the value
    /// returned by `action`, or [`WaitTimeoutResult::TimedOut`] if the fixed
    /// condition-wait budget expires first.
    ///
    /// # Errors
    ///
    /// The returned future reports deadline overflow and Timer domain,
    /// registration, or completion errors when waiting is required.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled. It also panics if the registry exhausts registration
    /// identifiers.
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
    /// deadline is reused across wakeups and followed by one final locked
    /// predicate check. Predicate readiness wins over a successful timeout. A
    /// Timer registration or completion error is settled before every
    /// post-wait predicate result and prevents `action` from running. If a
    /// signal wins before the timer is ready but reacquiring the state exhausts
    /// the fixed deadline, a still-blocking predicate times out without another
    /// waiter registration. When the signal and deadline are both ready, the
    /// deadline is selected first. A zero timeout still checks the predicate
    /// once.
    ///
    /// # Type Parameters
    ///
    /// * `'a` - Lifetime shared by the monitor, closures, and returned future.
    /// * `R` - Value returned by `action`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget fixed after acquiring the
    ///   state lock.
    /// * `predicate` - Predicate returning `true` while waiting must continue.
    /// * `action` - Action receiving mutable state when waiting finishes.
    ///
    /// # Returns
    ///
    /// A future resolving to [`WaitTimeoutResult::Ready`] with the value
    /// returned by `action`, or [`WaitTimeoutResult::TimedOut`] if the fixed
    /// condition-wait budget expires while `predicate` remains true.
    ///
    /// # Errors
    ///
    /// The returned future reports deadline overflow and Timer domain,
    /// registration, or completion errors when waiting is required.
    ///
    /// # Panics
    ///
    /// The returned future propagates a panic from `predicate` or `action` when
    /// it is polled. It also panics if the registry exhausts registration
    /// identifiers.
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
            let started_at = self.timer.now();
            if !predicate(&*guard) {
                return Ok(WaitTimeoutResult::Ready(action(&mut *guard)));
            }
            if timeout.is_zero() {
                return Ok(WaitTimeoutResult::TimedOut);
            }

            let deadline_at = started_at.checked_add(timeout)?;
            let mut deadline = self.timer.at(deadline_at)?;
            loop {
                let registration = self.register_waiter();
                drop(guard);
                let status = registration
                    .wait_until_signalled_or_deadline(&mut deadline)
                    .await;
                drop(registration);
                guard = self.state.lock().await;
                let status = status?;
                let deadline_reached = if status.is_timed_out() {
                    true
                } else {
                    Self::deadline_reached(&mut deadline).await?
                };
                if !predicate(&*guard) {
                    return Ok(WaitTimeoutResult::Ready(action(&mut *guard)));
                }
                if deadline_reached {
                    return Ok(WaitTimeoutResult::TimedOut);
                }
            }
        }
    }
}
