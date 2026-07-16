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
    atomic::{
        AtomicU64,
        Ordering,
    },
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
    internal::{
        MockMonitorState,
        MockMonitorWaiterGuard,
        MockWaiterRegistry,
        MockWaiterState,
    },
};

/// Monitor implementation for deterministic tests.
///
/// `MockMonitor` protects a state value like the real monitor implementations,
/// but timeout methods use an explicitly controllable manual monotonic clock.
/// Advancing that clock wakes waiters so they can recheck predicates and
/// timeout budgets.
///
/// This type is intended for deterministic tests of capability-trait and
/// predicate-wait behavior. It does not expose a guard type and is not a
/// drop-in replacement for concrete guard-oriented monitor APIs.
/// Notification methods may be called from a state-access closure because
/// waiter registrations are protected independently from user state.
pub struct MockMonitor<T> {
    /// Keeps the manual-clock callback registered for this monitor's lifetime.
    _advance_subscription: ManualAdvanceSubscription,
    /// Shared manual monotonic clock used by timeout methods.
    clock: Arc<ManualMonotonicClock>,
    /// Protected mock state.
    state: Arc<Mutex<MockMonitorState<T>>>,
    /// Waiter registrations protected independently from user state.
    waiter_registry: Mutex<MockWaiterRegistry>,
    /// Modulo-u64 change token advanced by notifications and time changes.
    change_epoch: Arc<AtomicU64>,
    /// Gate pairing epoch checks with blocking condition-variable waits.
    change_gate: Arc<Mutex<()>>,
    /// Condition variable used by blocking waiters.
    changed: Arc<Condvar>,
    /// Condition variable used to observe timeout-waiter registrations.
    timeout_waiters_changed: Condvar,
    /// Broadcasts mock notifications and time changes to async waiters.
    #[cfg(feature = "async")]
    async_change_sender: watch::Sender<u64>,
}

impl<T> MockMonitor<T> {
    /// Creates a mock monitor protecting the supplied state value.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A mock monitor with a new independent manual clock.
    #[inline]
    pub fn new(state: T) -> Self {
        Self::from_clock(state, ManualMonotonicClock::new_shared())
    }

    /// Creates a mock monitor driven by an explicitly shared manual clock.
    ///
    /// # Arguments
    /// - `state`: Initial protected state.
    /// - `clock`: Manual clock used for all timeout deadlines.
    ///
    /// # Returns
    /// A monitor that wakes timeout waiters whenever `clock` advances.
    ///
    /// # Concurrency
    /// Advancing `clock` is safe while executing a closure that holds this
    /// monitor's state lock. The clock callback signals waiters without
    /// acquiring the protected-state lock.
    #[inline]
    pub fn from_clock(state: T, clock: Arc<ManualMonotonicClock>) -> Self {
        let state = Arc::new(Mutex::new(MockMonitorState::new(state)));
        let change_epoch = Arc::new(AtomicU64::new(0));
        let change_gate = Arc::new(Mutex::new(()));
        let changed = Arc::new(Condvar::new());
        #[cfg(feature = "async")]
        let (async_change_sender, _) = watch::channel(0);
        let callback_change_epoch = Arc::clone(&change_epoch);
        let callback_change_gate = Arc::clone(&change_gate);
        let callback_changed = Arc::clone(&changed);
        #[cfg(feature = "async")]
        let callback_change_sender = async_change_sender.clone();
        let advance_subscription = clock.subscribe_advances(move || {
            #[cfg(feature = "async")]
            Self::publish_change(
                &callback_change_gate,
                &callback_change_epoch,
                &callback_changed,
                &callback_change_sender,
            );
            #[cfg(not(feature = "async"))]
            Self::publish_change(
                &callback_change_gate,
                &callback_change_epoch,
                &callback_changed,
            );
        });
        Self {
            _advance_subscription: advance_subscription,
            clock,
            state,
            waiter_registry: Mutex::new(MockWaiterRegistry::new()),
            change_epoch,
            change_gate,
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
    #[inline(always)]
    pub fn elapsed(&self) -> Duration {
        self.clock.now().elapsed_since_origin()
    }

    /// Returns the manual clock used by timeout methods.
    #[must_use]
    #[inline(always)]
    pub fn monotonic_clock(&self) -> &ManualMonotonicClock {
        self.clock.as_ref()
    }

    /// Returns the number of timeout wait operations ready to observe changes.
    ///
    /// An asynchronous timeout wait is counted after its future is first
    /// polled, not when the future is created.
    #[must_use]
    #[inline(always)]
    pub fn pending_timeout_waiters(&self) -> usize {
        self.lock_waiter_registry().timeout_waiters
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
        let real_start = Instant::now();
        let mut waiter_registry = self.lock_waiter_registry();
        if waiter_registry.timeout_waiters >= expected_count {
            return true;
        }
        let Some(real_deadline) = real_start.checked_add(real_timeout) else {
            return false;
        };
        while waiter_registry.timeout_waiters < expected_count {
            let remaining =
                real_deadline.saturating_duration_since(Instant::now());
            let (next_registry, wait_result) = self
                .timeout_waiters_changed
                .wait_timeout(waiter_registry, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            waiter_registry = next_registry;
            if wait_result.timed_out()
                && waiter_registry.timeout_waiters < expected_count
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
    #[inline]
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
    #[inline]
    pub fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut state = self.lock_state();
        f(&mut state.value)
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
        let mut waiter_registry = self.lock_waiter_registry();
        let waiter_id =
            waiter_registry
                .waiters
                .iter()
                .find_map(|(waiter_id, waiter)| {
                    (!waiter.notified).then_some(*waiter_id)
                });
        if let Some(waiter) = waiter_id
            .and_then(|waiter_id| waiter_registry.waiters.get_mut(&waiter_id))
        {
            waiter.notified = true;
        }
        drop(waiter_registry);
        self.signal_change();
    }

    /// Wakes all waiters.
    pub fn notify_all(&self) {
        let mut waiter_registry = self.lock_waiter_registry();
        for waiter in waiter_registry.waiters.values_mut() {
            waiter.notified = true;
        }
        drop(waiter_registry);
        self.signal_change();
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
        let mut waiter_registry = self.lock_waiter_registry();
        waiter_registry
            .waiters
            .remove(&waiter_id)
            .expect("mock monitor waiter should remain registered");
        if timeout_waiter {
            waiter_registry.timeout_waiters = waiter_registry
                .timeout_waiters
                .checked_sub(1)
                .expect("mock monitor timeout waiter count underflowed");
        }
        drop(waiter_registry);
        if timeout_waiter {
            self.timeout_waiters_changed.notify_all();
        }
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

    /// Locks the waiter registry and recovers from poisoning.
    ///
    /// # Returns
    ///
    /// A guard for the internal waiter registry.
    #[inline(always)]
    fn lock_waiter_registry(&self) -> MutexGuard<'_, MockWaiterRegistry> {
        self.waiter_registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Locks state for the initial timeout condition-wait predicate check.
    ///
    /// The returned guard keeps protected state locked while the initial
    /// predicate is evaluated and the fixed timeout target is captured.
    ///
    /// # Returns
    ///
    /// A guard for the internal mock monitor state.
    fn lock_timeout_state(&self) -> MutexGuard<'_, MockMonitorState<T>> {
        self.lock_state()
    }

    /// Captures the fixed manual-clock target for one condition wait.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Manual-clock duration assigned to the condition wait.
    ///
    /// # Returns
    ///
    /// The absolute manual-clock elapsed duration at which the wait expires.
    fn condition_wait_target_elapsed(&self, timeout: Duration) -> Duration {
        self.elapsed().saturating_add(timeout)
    }

    /// Registers a waiter while the monitor state is already locked.
    ///
    /// # Arguments
    ///
    /// * `timeout_waiter` - Whether the waiter contributes to the timeout
    ///   waiter count.
    ///
    /// # Returns
    ///
    /// The identifier assigned to the waiter.
    fn register_waiter_locked(&self, timeout_waiter: bool) -> u64 {
        let mut waiter_registry = self.lock_waiter_registry();
        let waiter_id = waiter_registry.next_waiter_id;
        waiter_registry.next_waiter_id = waiter_registry
            .next_waiter_id
            .checked_add(1)
            .expect("mock monitor waiter identifier overflowed");
        let previous = waiter_registry
            .waiters
            .insert(waiter_id, MockWaiterState::new());
        assert!(previous.is_none(), "mock monitor waiter identifier reused");
        if timeout_waiter {
            waiter_registry.timeout_waiters = waiter_registry
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
    /// * `waiter_id` - Identifier of the waiter checking its notification.
    ///
    /// # Returns
    ///
    /// `true` when the waiter owned an unconsumed notification.
    fn take_notification(&self, waiter_id: u64) -> bool {
        let mut waiter_registry = self.lock_waiter_registry();
        let waiter = waiter_registry
            .waiters
            .get_mut(&waiter_id)
            .expect("mock monitor waiter should remain registered");
        std::mem::take(&mut waiter.notified)
    }

    /// Returns the current shared modulo-u64 change token.
    ///
    /// The value is only compared with a later load to detect intervening
    /// changes. Correctness assumes fewer than `2^64` changes occur during one
    /// waiter's check-to-sleep window; a complete wrap in that window could be
    /// mistaken for no change. Protected predicate state remains synchronized
    /// by the monitor mutex.
    #[inline(always)]
    fn current_change_epoch(&self) -> u64 {
        self.change_epoch.load(Ordering::Relaxed)
    }

    /// Waits until the shared change token differs from `observed_epoch`.
    ///
    /// The caller must capture `observed_epoch` before its final locked state
    /// check. This method releases `state` before acquiring the independent
    /// change gate, then compares the token under that gate before sleeping.
    /// Consequently a callback either changes the token before the comparison
    /// or blocks on the gate until the condition-variable wait atomically
    /// releases it. This relies on no complete `u64` wrap occurring in that
    /// short window. The gate is dropped before the state lock is reacquired,
    /// so no `change_gate -> state` lock edge exists.
    ///
    /// # Arguments
    ///
    /// * `state` - Protected-state guard held during the preceding check.
    /// * `observed_epoch` - Modulo-u64 token captured before that check.
    ///
    /// # Returns
    ///
    /// A newly acquired protected-state guard after a real or spurious wake.
    fn wait_for_blocking_change<'a>(
        &'a self,
        state: MutexGuard<'a, MockMonitorState<T>>,
        observed_epoch: u64,
    ) -> MutexGuard<'a, MockMonitorState<T>> {
        drop(state);
        let change_gate = self
            .change_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let change_gate = self
            .changed
            .wait_while(change_gate, |_| {
                self.current_change_epoch() == observed_epoch
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(change_gate);
        self.lock_state()
    }

    /// Publishes a monitor change through the shared change protocol.
    ///
    /// Notification callers invoke this after releasing the waiter registry.
    /// It delegates to the same gate/token/Condvar/watch helper used by clock
    /// callbacks. Blocking waiters never acquire protected state while holding
    /// the gate, preventing a lock-order cycle.
    #[inline(always)]
    fn signal_change(&self) {
        #[cfg(feature = "async")]
        Self::publish_change(
            &self.change_gate,
            &self.change_epoch,
            &self.changed,
            &self.async_change_sender,
        );
        #[cfg(not(feature = "async"))]
        Self::publish_change(
            &self.change_gate,
            &self.change_epoch,
            &self.changed,
        );
    }

    /// Serializes and publishes one modulo-u64 change token.
    ///
    /// The gate orders token allocation, blocking notification, and async watch
    /// publication identically for clock callbacks and explicit notifications.
    /// `Relaxed` ordering is sufficient because the atomic token only detects
    /// changes; predicate state remains synchronized by the monitor mutex.
    /// Token values wrap modulo `u64`, so waiters assume fewer than `2^64`
    /// publications occur within one check-to-sleep window.
    ///
    /// # Arguments
    ///
    /// * `change_gate` - Gate paired with the blocking condition variable.
    /// * `change_epoch` - Shared modulo-u64 change token.
    /// * `changed` - Condition variable signaled for blocking waiters.
    /// * `async_change_sender` - Watch sender notified for async waiters when
    ///   async support is enabled.
    fn publish_change(
        change_gate: &Mutex<()>,
        change_epoch: &AtomicU64,
        changed: &Condvar,
        #[cfg(feature = "async")] async_change_sender: &watch::Sender<u64>,
    ) {
        let _change_gate = change_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let change_token =
            change_epoch.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        changed.notify_all();
        #[cfg(feature = "async")]
        async_change_sender.send_replace(change_token);
        #[cfg(not(feature = "async"))]
        let _ = change_token;
    }
}

impl<T> Notifier for MockMonitor<T> {
    /// Wakes one waiter if one is blocked.
    #[inline(always)]
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all waiters.
    #[inline(always)]
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T> ConditionWaiter for MockMonitor<T> {
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
            self.register_waiter_locked(false)
        };
        let _waiter_guard = self.waiter_guard_from_registered(waiter_id, false);
        let mut state = self.lock_state();
        loop {
            let observed_epoch = self.current_change_epoch();
            if self.take_notification(waiter_id) && !predicate(&state.value) {
                return action(&mut state.value);
            }
            state = self.wait_for_blocking_change(state, observed_epoch);
        }
    }
}

impl<T> TimeoutConditionWaiter for MockMonitor<T> {
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
            let mut state = self.lock_timeout_state();
            if !predicate(&state.value) {
                return WaitTimeoutResult::Ready(action(&mut state.value));
            }
            let target_elapsed = self.condition_wait_target_elapsed(timeout);
            if self.elapsed() >= target_elapsed {
                return WaitTimeoutResult::TimedOut;
            }
            (self.register_waiter_locked(true), target_elapsed)
        };
        let _waiter_guard = self.waiter_guard_from_registered(waiter_id, true);
        let mut state = self.lock_state();
        loop {
            let observed_epoch = self.current_change_epoch();
            let notified = self.take_notification(waiter_id);
            let timed_out = self.elapsed() >= target_elapsed;
            if notified || timed_out {
                if !predicate(&state.value) {
                    return WaitTimeoutResult::Ready(action(&mut state.value));
                }
                if timed_out {
                    return WaitTimeoutResult::TimedOut;
                }
            }
            state = self.wait_for_blocking_change(state, observed_epoch);
        }
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncConditionWaiter for MockMonitor<T> {
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
                self.register_waiter_locked(false)
            };
            let _waiter_guard =
                self.waiter_guard_from_registered(waiter_id, false);
            loop {
                {
                    let mut state = self.lock_state();
                    if self.take_notification(waiter_id)
                        && !predicate(&state.value)
                    {
                        return action(&mut state.value);
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

#[cfg(feature = "async")]
impl<T: Send> AsyncTimeoutConditionWaiter for MockMonitor<T> {
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
                let mut state = self.lock_timeout_state();
                if !predicate(&state.value) {
                    return WaitTimeoutResult::Ready(action(&mut state.value));
                }
                let target_elapsed =
                    self.condition_wait_target_elapsed(timeout);
                if self.elapsed() >= target_elapsed {
                    return WaitTimeoutResult::TimedOut;
                }
                (self.register_waiter_locked(true), target_elapsed)
            };
            let _waiter_guard =
                self.waiter_guard_from_registered(waiter_id, true);
            loop {
                {
                    let mut state = self.lock_state();
                    let notified = self.take_notification(waiter_id);
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

impl<T> From<T> for MockMonitor<T> {
    /// Creates a mock monitor from an initial state value.
    #[inline(always)]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for MockMonitor<T> {
    /// Creates a mock monitor containing `T::default()`.
    #[inline(always)]
    fn default() -> Self {
        Self::new(T::default())
    }
}
