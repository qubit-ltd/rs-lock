// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mock monitor with timeout time driven by a manual monotonic clock.

use std::collections::BTreeMap;
use std::sync::{
    Arc,
    Condvar,
    Mutex,
    MutexGuard,
};
use std::time::Duration;
use std::time::Instant;

#[cfg(feature = "async")]
use std::future::Future;

use qubit_clock::{
    ManualAdvanceSubscription,
    ManualMonotonicClock,
    MonotonicClock,
};

#[cfg(feature = "async")]
use tokio::sync::watch;

use super::mock_monitor_waiter_guard::MockMonitorWaiterGuard;
#[cfg(feature = "async")]
use super::{
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
};
use super::{
    ConditionWaiter,
    Notifier,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
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
    /// Broadcasts mock notifications and time changes to async waiters.
    #[cfg(feature = "async")]
    async_change_sender: watch::Sender<u64>,
}

/// State protected by [`MockMonitor`].
struct MockMonitorState<T> {
    /// User-visible protected value.
    value: T,
    /// Epoch incremented by notifications and mock time changes.
    change_epoch: u64,
    /// Identifier assigned to the next registered waiter.
    next_waiter_id: u64,
    /// Registered waiters and their individually assigned notification state.
    waiters: BTreeMap<u64, MockWaiterState>,
    /// Number of active blocking and asynchronous timeout waits.
    timeout_waiters: usize,
}

/// Notification state assigned to one active mock-monitor waiter.
struct MockWaiterState {
    /// Whether this waiter owns an unconsumed notification.
    notified: bool,
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
            change_epoch: 0,
            next_waiter_id: 0,
            waiters: BTreeMap::new(),
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
    pub fn with_read<R, F>(&self, f: F) -> R
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
    pub fn with_write<R, F>(&self, f: F) -> R
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
    pub fn with_write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write(f);
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
    pub fn with_write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write(f);
        self.notify_all();
        result
    }

    /// Wakes one waiter if one is blocked.
    pub fn notify_one(&self) {
        let change_epoch = {
            let mut state = self.lock_state();
            let waiter_id = state
                .waiters
                .iter()
                .find_map(|(waiter_id, waiter)| {
                    (!waiter.notified).then_some(*waiter_id)
                });
            if let Some(waiter) = waiter_id
                .and_then(|waiter_id| state.waiters.get_mut(&waiter_id))
            {
                waiter.notified = true;
            }
            Self::advance_change_epoch(&mut state)
        };
        self.changed.notify_all();
        self.notify_async_change(change_epoch);
    }

    /// Wakes all waiters.
    pub fn notify_all(&self) {
        let change_epoch = {
            let mut state = self.lock_state();
            for waiter in state.waiters.values_mut() {
                waiter.notified = true;
            }
            Self::advance_change_epoch(&mut state)
        };
        self.changed.notify_all();
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

    /// Unregisters one active waiter and wakes timeout registration observers
    /// when necessary.
    ///
    /// # Arguments
    ///
    /// * `waiter_id` - Identifier assigned when the waiter registered.
    /// * `timeout_waiter` - Whether the waiter contributes to the timeout
    ///   waiter count.
    pub(super) fn unregister_waiter(
        &self,
        waiter_id: u64,
        timeout_waiter: bool,
    ) {
        let mut state = self.lock_state();
        state
            .waiters
            .remove(&waiter_id)
            .expect("mock monitor waiter should remain registered");
        if timeout_waiter {
            state.timeout_waiters = state
                .timeout_waiters
                .checked_sub(1)
                .expect("mock monitor timeout waiter count underflowed");
        }
        drop(state);
        if timeout_waiter {
            self.timeout_waiters_changed.notify_all();
        }
    }

    /// Registers a waiter while the monitor state is already locked.
    ///
    /// # Arguments
    ///
    /// * `state` - Locked internal monitor state.
    /// * `timeout_waiter` - Whether the waiter contributes to the timeout
    ///   waiter count.
    ///
    /// # Returns
    ///
    /// The identifier assigned to the waiter.
    fn register_waiter_locked(
        state: &mut MockMonitorState<T>,
        timeout_waiter: bool,
    ) -> u64 {
        let waiter_id = state.next_waiter_id;
        state.next_waiter_id = state
            .next_waiter_id
            .checked_add(1)
            .expect("mock monitor waiter identifier overflowed");
        let previous = state.waiters.insert(
            waiter_id,
            MockWaiterState {
                notified: false,
            },
        );
        assert!(previous.is_none(), "mock monitor waiter identifier reused");
        if timeout_waiter {
            state.timeout_waiters = state
                .timeout_waiters
                .checked_add(1)
                .expect("mock monitor timeout waiter count overflowed");
        }
        waiter_id
    }

    /// Creates an RAII guard for a waiter registered under an existing lock.
    ///
    /// # Arguments
    ///
    /// * `waiter_id` - Identifier assigned to the registered waiter.
    /// * `timeout_waiter` - Whether the waiter contributes to the timeout
    ///   waiter count.
    ///
    /// # Returns
    ///
    /// A guard that unregisters the waiter on return, cancellation, or panic.
    fn waiter_guard_from_registered(
        &self,
        waiter_id: u64,
        timeout_waiter: bool,
    ) -> MockMonitorWaiterGuard<'_, T> {
        if timeout_waiter {
            self.timeout_waiters_changed.notify_all();
        }
        MockMonitorWaiterGuard::new(self, waiter_id, timeout_waiter)
    }

    /// Consumes the notification assigned to one waiter.
    ///
    /// # Arguments
    ///
    /// * `state` - Locked internal monitor state.
    /// * `waiter_id` - Identifier of the waiter checking its notification.
    ///
    /// # Returns
    ///
    /// `true` when the waiter owned an unconsumed notification.
    fn take_notification(
        state: &mut MockMonitorState<T>,
        waiter_id: u64,
    ) -> bool {
        let waiter = state
            .waiters
            .get_mut(&waiter_id)
            .expect("mock monitor waiter should remain registered");
        std::mem::take(&mut waiter.notified)
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

    /// Notifies asynchronous waiters about a notification or time change.
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

impl<T: Send + 'static> ConditionWaiter for MockMonitor<T> {
    type State = T;

    /// Blocks while the predicate remains true, then runs the action.
    fn wait_while<R, P, F>(&self, mut predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        let waiter_id = {
            let mut state = self.lock_state();
            if !predicate(&state.value) {
                return action(&mut state.value);
            }
            Self::register_waiter_locked(&mut state, false)
        };
        let _waiter_guard =
            self.waiter_guard_from_registered(waiter_id, false);
        let mut state = self.lock_state();
        loop {
            while !Self::take_notification(&mut state, waiter_id) {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            if !predicate(&state.value) {
                return action(&mut state.value);
            }
        }
    }
}

impl<T: Send + 'static> TimeoutConditionWaiter for MockMonitor<T> {
    /// Blocks while the predicate remains true or until mock elapsed time
    /// reaches timeout. The fixed target is established after the initial
    /// locked predicate check, excluding initial lock contention. At the
    /// target, readiness wins the final locked predicate check; zero timeout
    /// still checks the predicate once.
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
        let (waiter_id, target_elapsed) = {
            let mut state = self.lock_state();
            if !predicate(&state.value) {
                return WaitTimeoutResult::Ready(action(&mut state.value));
            }
            let target_elapsed = self.elapsed().saturating_add(timeout);
            if self.elapsed() >= target_elapsed {
                return WaitTimeoutResult::TimedOut;
            }
            (
                Self::register_waiter_locked(&mut state, true),
                target_elapsed,
            )
        };
        let _waiter_guard =
            self.waiter_guard_from_registered(waiter_id, true);
        let mut state = self.lock_state();
        loop {
            let notified = Self::take_notification(&mut state, waiter_id);
            let timed_out = self.elapsed() >= target_elapsed;
            if notified || timed_out {
                if !predicate(&state.value) {
                    return WaitTimeoutResult::Ready(action(&mut state.value));
                }
                if timed_out {
                    return WaitTimeoutResult::TimedOut;
                }
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncConditionWaiter for MockMonitor<T> {
    type State = T;

    /// Returns a future that waits while the predicate remains true.
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
        let mut change_receiver = self.async_change_sender.subscribe();
        async move {
            let waiter_id = {
                let mut state = self.lock_state();
                if !predicate(&state.value) {
                    return action(&mut state.value);
                }
                Self::register_waiter_locked(&mut state, false)
            };
            let _waiter_guard =
                self.waiter_guard_from_registered(waiter_id, false);
            loop {
                {
                    let mut state = self.lock_state();
                    if Self::take_notification(&mut state, waiter_id)
                        && !predicate(&state.value)
                    {
                        return action(&mut state.value);
                    }
                }
                change_receiver.changed().await.expect(
                    "mock monitor sender should live while the monitor is borrowed",
                );
            }
        }
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncTimeoutConditionWaiter for MockMonitor<T> {
    /// Returns a future that waits while the predicate remains true or times
    /// out. The future is lazy, and its fixed manual-clock target is
    /// established after the initial locked predicate check. Initial lock
    /// contention is excluded. At the target, readiness wins the final locked
    /// predicate check; zero timeout still checks the predicate once.
    #[allow(
        clippy::manual_async_fn,
        reason = "the explicit Send bound is part of the trait contract"
    )]
    fn wait_while_for_async<'a, R, P, F>(
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
        let mut change_receiver = self.async_change_sender.subscribe();
        async move {
            let (waiter_id, target_elapsed) = {
                let mut state = self.lock_state();
                if !predicate(&state.value) {
                    return WaitTimeoutResult::Ready(action(&mut state.value));
                }
                let target_elapsed = self.elapsed().saturating_add(timeout);
                if self.elapsed() >= target_elapsed {
                    return WaitTimeoutResult::TimedOut;
                }
                (
                    Self::register_waiter_locked(&mut state, true),
                    target_elapsed,
                )
            };
            let _waiter_guard =
                self.waiter_guard_from_registered(waiter_id, true);
            loop {
                {
                    let mut state = self.lock_state();
                    let notified =
                        Self::take_notification(&mut state, waiter_id);
                    let timed_out = self.elapsed() >= target_elapsed;
                    if notified || timed_out {
                        if !predicate(&state.value) {
                            return WaitTimeoutResult::Ready(action(
                                &mut state.value,
                            ));
                        }
                        if timed_out {
                            return WaitTimeoutResult::TimedOut;
                        }
                    }
                }
                change_receiver
                    .changed()
                    .await
                    .expect("mock monitor sender should live while the monitor is borrowed");
            }
        }
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
