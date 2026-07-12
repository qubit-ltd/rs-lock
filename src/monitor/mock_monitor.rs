// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mock monitor with timeout time driven by a manual monotonic clock.

use std::sync::{
    Arc,
    Condvar,
    Mutex,
    MutexGuard,
};
use std::time::Duration;
use std::time::Instant;

use qubit_clock::{
    ManualAdvanceSubscription,
    ManualMonotonicClock,
    MonotonicClock,
};

#[cfg(feature = "async")]
use tokio::sync::{
    Notify,
    watch,
};

use super::mock_monitor_waiter_guard::MockMonitorWaiterGuard;
#[cfg(feature = "async")]
use super::{
    AsyncConditionWaiter,
    AsyncMonitorFuture,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
};
use super::{
    ConditionWaiter,
    NotificationWaiter,
    Notifier,
    TimeoutConditionWaiter,
    TimeoutNotificationWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

/// Monitor implementation for deterministic tests.
///
/// `MockMonitor` protects a state value like the real monitor implementations,
/// but timeout methods use an explicitly controllable manual monotonic clock.
/// Advancing that clock wakes waiters so they can recheck predicates and
/// timeout budgets.
pub struct MockMonitor<T> {
    /// Keeps the manual-clock callback registered for this monitor's lifetime.
    _advance_subscription: ManualAdvanceSubscription,
    /// Shared manual monotonic clock used by timeout methods.
    clock: Arc<ManualMonotonicClock>,
    /// Protected mock state.
    state: Arc<Mutex<MockMonitorState<T>>>,
    /// Condition variable used by blocking waiters.
    changed: Arc<Condvar>,
    /// Condition variable used to observe timeout-waiter registrations.
    timeout_waiters_changed: Condvar,
    /// Tokio notification primitive used by async notification waiters.
    #[cfg(feature = "async")]
    async_notification: Notify,
    /// Broadcasts mock state or mock time changes to async timeout waiters.
    #[cfg(feature = "async")]
    async_change_sender: watch::Sender<u64>,
}

/// State protected by [`MockMonitor`].
struct MockMonitorState<T> {
    /// User-visible protected value.
    value: T,
    /// Epoch incremented only by notification calls.
    notification_epoch: u64,
    /// Epoch incremented by notifications and mock time changes.
    change_epoch: u64,
    /// Number of active blocking and asynchronous timeout waits.
    timeout_waiters: usize,
}

impl<T: Send + 'static> MockMonitor<T> {
    /// Creates a mock monitor protecting the supplied state value.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A mock monitor with a new independent manual clock.
    pub fn new(state: T) -> Self {
        Self::from_clock(state, Arc::new(ManualMonotonicClock::new()))
    }

    /// Creates a mock monitor driven by an explicitly shared manual clock.
    ///
    /// # Parameters
    /// - `state`: Initial protected state.
    /// - `clock`: Manual clock used for all timeout deadlines.
    ///
    /// # Returns
    /// A monitor that wakes timeout waiters whenever `clock` advances.
    ///
    /// # Concurrency
    /// The clock callback briefly locks the monitor state before signaling the
    /// condition variable. Callers must not advance `clock` while executing a
    /// closure that already holds this monitor's state lock.
    pub fn from_clock(state: T, clock: Arc<ManualMonotonicClock>) -> Self {
        let state = Arc::new(Mutex::new(MockMonitorState {
            value: state,
            notification_epoch: 0,
            change_epoch: 0,
            timeout_waiters: 0,
        }));
        let changed = Arc::new(Condvar::new());
        #[cfg(feature = "async")]
        let (async_change_sender, _) = watch::channel(0);
        let callback_state = Arc::clone(&state);
        let callback_changed = Arc::clone(&changed);
        #[cfg(feature = "async")]
        let callback_change_sender = async_change_sender.clone();
        let advance_subscription = clock.subscribe_advances(move || {
            let change_epoch = {
                let mut state = callback_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.change_epoch = state.change_epoch.wrapping_add(1);
                state.change_epoch
            };
            callback_changed.notify_all();
            #[cfg(feature = "async")]
            {
                let _ = callback_change_sender.send(change_epoch);
            }
            #[cfg(not(feature = "async"))]
            let _ = change_epoch;
        });
        Self {
            _advance_subscription: advance_subscription,
            clock,
            state,
            changed,
            timeout_waiters_changed: Condvar::new(),
            #[cfg(feature = "async")]
            async_notification: Notify::new(),
            #[cfg(feature = "async")]
            async_change_sender,
        }
    }

    /// Returns the current elapsed time of the shared manual clock.
    ///
    /// # Returns
    ///
    /// The elapsed time used by timeout waits.
    pub fn elapsed(&self) -> Duration {
        self.clock.now().elapsed_since_origin()
    }

    /// Returns the manual clock used by timeout methods.
    #[must_use]
    pub fn monotonic_clock(&self) -> &ManualMonotonicClock {
        self.clock.as_ref()
    }

    /// Returns the number of timeout wait operations ready to observe changes.
    ///
    /// An asynchronous timeout wait is counted after its future is first
    /// polled, not when the future is created.
    #[must_use]
    pub fn pending_timeout_waiters(&self) -> usize {
        self.lock_state().timeout_waiters
    }

    /// Blocks in real time until enough timeout waiters are ready.
    ///
    /// `real_timeout` is only a test coordination guard and never contributes
    /// to mock time. Returns `true` when `expected_count` active waiters are
    /// ready, or `false` when the real-time guard expires or overflows. An
    /// asynchronous wait must be polled before it can contribute to the count.
    #[must_use]
    pub fn wait_for_timeout_waiters(
        &self,
        expected_count: usize,
        real_timeout: Duration,
    ) -> bool {
        let Some(real_deadline) = Instant::now().checked_add(real_timeout)
        else {
            return false;
        };
        let mut state = self.lock_state();
        while state.timeout_waiters < expected_count {
            let remaining =
                real_deadline.saturating_duration_since(Instant::now());
            let (next_state, wait_result) = self
                .timeout_waiters_changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next_state;
            if wait_result.timed_out() && state.timeout_waiters < expected_count
            {
                return false;
            }
        }
        true
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
    pub fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let state = self.lock_state();
        f(&state.value)
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
    pub fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut state = self.lock_state();
        f(&mut state.value)
    }

    /// Mutates the protected state and wakes one waiter.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    pub fn write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.write(f);
        self.notify_one();
        result
    }

    /// Mutates the protected state and wakes all waiters.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    pub fn write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.write(f);
        self.notify_all();
        result
    }

    /// Wakes one waiter if one is blocked.
    pub fn notify_one(&self) {
        let change_epoch = self.advance_notification_epoch();
        self.changed.notify_one();
        #[cfg(feature = "async")]
        self.async_notification.notify_one();
        self.notify_async_change(change_epoch);
    }

    /// Wakes all waiters.
    pub fn notify_all(&self) {
        let change_epoch = self.advance_notification_epoch();
        self.changed.notify_all();
        #[cfg(feature = "async")]
        self.async_notification.notify_waiters();
        self.notify_async_change(change_epoch);
    }

    /// Locks the internal state and recovers from poisoning.
    ///
    /// # Returns
    ///
    /// A guard for the internal mock monitor state.
    fn lock_state(&self) -> MutexGuard<'_, MockMonitorState<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers one active timeout wait and wakes registration observers.
    pub(super) fn register_timeout_waiter(&self) {
        let mut state = self.lock_state();
        state.timeout_waiters = state
            .timeout_waiters
            .checked_add(1)
            .expect("mock monitor timeout waiter count overflowed");
        drop(state);
        self.timeout_waiters_changed.notify_all();
    }

    /// Unregisters one active timeout wait and wakes registration observers.
    pub(super) fn unregister_timeout_waiter(&self) {
        let mut state = self.lock_state();
        state.timeout_waiters = state
            .timeout_waiters
            .checked_sub(1)
            .expect("mock monitor timeout waiter count underflowed");
        drop(state);
        self.timeout_waiters_changed.notify_all();
    }

    /// Creates an RAII registration for one timeout wait operation.
    fn timeout_waiter_guard(&self) -> MockMonitorWaiterGuard<'_, T> {
        MockMonitorWaiterGuard::new(self)
    }

    /// Increments the change epoch.
    ///
    /// # Arguments
    ///
    /// * `state` - Internal state whose change epoch should advance.
    ///
    /// # Returns
    ///
    /// The new change epoch.
    fn advance_change_epoch(state: &mut MockMonitorState<T>) -> u64 {
        state.change_epoch = state.change_epoch.wrapping_add(1);
        state.change_epoch
    }

    /// Increments the notification and change epochs.
    ///
    /// # Returns
    ///
    /// The new change epoch.
    fn advance_notification_epoch(&self) -> u64 {
        let mut state = self.lock_state();
        state.notification_epoch = state.notification_epoch.wrapping_add(1);
        Self::advance_change_epoch(&mut state)
    }

    /// Notifies asynchronous timeout waiters about a state or time change.
    ///
    /// # Arguments
    ///
    /// * `change_epoch` - New change epoch.
    #[cfg(feature = "async")]
    fn notify_async_change(&self, change_epoch: u64) {
        let _ = self.async_change_sender.send(change_epoch);
    }

    /// No-op when async support is disabled.
    #[cfg(not(feature = "async"))]
    fn notify_async_change(&self, _change_epoch: u64) {}
}

impl<T: Send + 'static> Notifier for MockMonitor<T> {
    /// Wakes one waiter if one is blocked.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send + 'static> NotificationWaiter for MockMonitor<T> {
    /// Blocks until a notification happens after this call starts.
    fn wait(&self) {
        let mut state = self.lock_state();
        let observed_epoch = state.notification_epoch;
        while state.notification_epoch == observed_epoch {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl<T: Send + 'static> TimeoutNotificationWaiter for MockMonitor<T> {
    /// Blocks until a notification happens or mock elapsed time reaches
    /// timeout.
    fn wait_for(&self, timeout: Duration) -> WaitTimeoutStatus {
        let target_elapsed = self.elapsed().saturating_add(timeout);
        let _waiter_guard = self.timeout_waiter_guard();
        let mut state = self.lock_state();
        let observed_epoch = state.notification_epoch;
        loop {
            if state.notification_epoch != observed_epoch {
                return WaitTimeoutStatus::Woken;
            }
            if self.elapsed() >= target_elapsed {
                return WaitTimeoutStatus::TimedOut;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl<T: Send + 'static> ConditionWaiter for MockMonitor<T> {
    type State = T;

    /// Blocks while the predicate remains true, then runs the action.
    fn wait_while<R, P, F>(&self, mut predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        let mut state = self.lock_state();
        while predicate(&state.value) {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        action(&mut state.value)
    }
}

impl<T: Send + 'static> TimeoutConditionWaiter for MockMonitor<T> {
    /// Blocks while the predicate remains true or until mock elapsed time
    /// reaches timeout.
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        let target_elapsed = self.elapsed().saturating_add(timeout);
        let _waiter_guard = self.timeout_waiter_guard();
        let mut state = self.lock_state();
        loop {
            if !predicate(&state.value) {
                return WaitTimeoutResult::Ready(action(&mut state.value));
            }
            if self.elapsed() >= target_elapsed {
                return WaitTimeoutResult::TimedOut;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncNotificationWaiter for MockMonitor<T> {
    /// Returns a future that resolves after an async notification.
    fn wait_async<'a>(&'a self) -> AsyncMonitorFuture<'a, ()> {
        let notified = self.async_notification.notified();
        Box::pin(notified)
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncTimeoutNotificationWaiter for MockMonitor<T> {
    /// Returns a future that resolves after notification or mock timeout.
    fn wait_for_async<'a>(
        &'a self,
        timeout: Duration,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutStatus> {
        let mut change_receiver = self.async_change_sender.subscribe();
        let (observed_epoch, target_elapsed) = {
            let state = self.lock_state();
            (
                state.notification_epoch,
                self.elapsed().saturating_add(timeout),
            )
        };
        Box::pin(async move {
            let _waiter_guard = self.timeout_waiter_guard();
            loop {
                {
                    let state = self.lock_state();
                    if state.notification_epoch != observed_epoch {
                        return WaitTimeoutStatus::Woken;
                    }
                    if self.elapsed() >= target_elapsed {
                        return WaitTimeoutStatus::TimedOut;
                    }
                }
                change_receiver
                    .changed()
                    .await
                    .expect("mock monitor sender should live while the monitor is borrowed");
            }
        })
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncConditionWaiter for MockMonitor<T> {
    type State = T;

    /// Returns a future that waits while the predicate remains true.
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        Box::pin(async move {
            loop {
                let notified = {
                    let mut state = self.lock_state();
                    if !predicate(&state.value) {
                        return action(&mut state.value);
                    }
                    self.async_notification.notified()
                };
                notified.await;
            }
        })
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncTimeoutConditionWaiter for MockMonitor<T> {
    /// Returns a future that waits while the predicate remains true or times
    /// out.
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        let mut change_receiver = self.async_change_sender.subscribe();
        let target_elapsed = self.elapsed().saturating_add(timeout);
        Box::pin(async move {
            let _waiter_guard = self.timeout_waiter_guard();
            loop {
                {
                    let mut state = self.lock_state();
                    if !predicate(&state.value) {
                        return WaitTimeoutResult::Ready(action(
                            &mut state.value,
                        ));
                    }
                    if self.elapsed() >= target_elapsed {
                        return WaitTimeoutResult::TimedOut;
                    }
                }
                change_receiver
                    .changed()
                    .await
                    .expect("mock monitor sender should live while the monitor is borrowed");
            }
        })
    }
}

impl<T: Send + 'static> From<T> for MockMonitor<T> {
    /// Creates a mock monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default + Send + 'static> Default for MockMonitor<T> {
    /// Creates a mock monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}
