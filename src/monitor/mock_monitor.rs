// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow inline-tests
//! Mock monitor with timeout time driven by a manual monotonic clock.

use std::collections::BTreeMap;
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

/// Test-only timeout condition-wait phases.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimeoutConditionWaitPhase {
    /// Immediately before the timeout waiter calls `lock_state()`.
    BeforeLock,
    /// Immediately after the timeout waiter acquires the state lock.
    AfterLock,
    /// Immediately after the timeout waiter captures its fixed clock target.
    TargetCaptured,
}

/// Test-only callback run while a timeout condition wait is initialized.
#[cfg(test)]
type TimeoutConditionWaitHook =
    Arc<dyn Fn(TimeoutConditionWaitPhase) + Send + Sync + 'static>;

/// Test-only callback run after a blocking wait checks state and before sleep.
#[cfg(test)]
type ChangeWaitBoundaryHook = Arc<dyn Fn() + Send + Sync + 'static>;

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
    /// Per-monitor initialization hook for timeout-budget regressions.
    #[cfg(test)]
    timeout_condition_wait_hook: Mutex<Option<TimeoutConditionWaitHook>>,
    /// Per-monitor hook for the blocking change-check-to-sleep boundary.
    #[cfg(test)]
    change_wait_boundary_hook: Mutex<Option<ChangeWaitBoundaryHook>>,
}

/// State protected by [`MockMonitor`].
struct MockMonitorState<T> {
    /// User-visible protected value.
    value: T,
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
    /// Advancing `clock` is safe while executing a closure that holds this
    /// monitor's state lock. The clock callback signals waiters without
    /// acquiring the protected-state lock.
    pub fn from_clock(state: T, clock: Arc<ManualMonotonicClock>) -> Self {
        let state = Arc::new(Mutex::new(MockMonitorState {
            value: state,
            next_waiter_id: 0,
            waiters: BTreeMap::new(),
            timeout_waiters: 0,
        }));
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
            change_epoch,
            change_gate,
            changed,
            timeout_waiters_changed: Condvar::new(),
            #[cfg(feature = "async")]
            async_change_sender,
            #[cfg(test)]
            timeout_condition_wait_hook: Mutex::new(None),
            #[cfg(test)]
            change_wait_boundary_hook: Mutex::new(None),
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
        let mut state = self.lock_state();
        let waiter_id = state.waiters.iter().find_map(|(waiter_id, waiter)| {
            (!waiter.notified).then_some(*waiter_id)
        });
        if let Some(waiter) =
            waiter_id.and_then(|waiter_id| state.waiters.get_mut(&waiter_id))
        {
            waiter.notified = true;
        }
        self.signal_change();
    }

    /// Wakes all waiters.
    pub fn notify_all(&self) {
        let mut state = self.lock_state();
        for waiter in state.waiters.values_mut() {
            waiter.notified = true;
        }
        self.signal_change();
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

    /// Locks state for the initial timeout condition-wait predicate check.
    ///
    /// Test builds publish the before-lock and after-lock phases around the
    /// uninterrupted `lock_state()` call. The returned guard keeps protected
    /// state locked.
    ///
    /// # Returns
    ///
    /// A guard for the internal mock monitor state.
    fn lock_timeout_state(&self) -> MutexGuard<'_, MockMonitorState<T>> {
        #[cfg(test)]
        self.run_timeout_condition_wait_hook(
            TimeoutConditionWaitPhase::BeforeLock,
        );
        let state = self.lock_state();
        #[cfg(test)]
        self.run_timeout_condition_wait_hook(
            TimeoutConditionWaitPhase::AfterLock,
        );
        state
    }

    /// Captures the fixed manual-clock target for one condition wait.
    ///
    /// Test builds publish the target-captured phase only after the clock read
    /// and saturating timeout addition are complete.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Manual-clock duration assigned to the condition wait.
    ///
    /// # Returns
    ///
    /// The absolute manual-clock elapsed duration at which the wait expires.
    fn condition_wait_target_elapsed(&self, timeout: Duration) -> Duration {
        let target_elapsed = self.elapsed().saturating_add(timeout);
        #[cfg(test)]
        self.run_timeout_condition_wait_hook(
            TimeoutConditionWaitPhase::TargetCaptured,
        );
        target_elapsed
    }

    /// Installs a callback for timeout condition-wait initialization phases.
    ///
    /// The test-only callback is isolated to this monitor instance. Each
    /// blocking or asynchronous timeout condition wait invokes it before and
    /// after its initial state-lock acquisition and after capturing its fixed
    /// manual-clock target. The callback runs while protected state is held for
    /// the after-lock and target-captured phases and must return promptly.
    ///
    /// # Arguments
    ///
    /// * `hook` - Callback to run at each initialization phase.
    #[cfg(test)]
    fn set_timeout_condition_wait_hook<F>(&self, hook: F)
    where
        F: Fn(TimeoutConditionWaitPhase) + Send + Sync + 'static,
    {
        *self
            .timeout_condition_wait_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::new(hook));
    }

    /// Runs this monitor's test-only timeout condition-wait callback.
    ///
    /// The callback is cloned while its configuration lock is held, then runs
    /// without that configuration lock. The timeout waiter holds protected
    /// state during the after-lock and target-captured phases.
    ///
    /// # Arguments
    ///
    /// * `phase` - Initialization phase reached by the timeout waiter.
    #[cfg(test)]
    fn run_timeout_condition_wait_hook(
        &self,
        phase: TimeoutConditionWaitPhase,
    ) {
        let hook = self
            .timeout_condition_wait_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(phase);
        }
    }

    /// Installs a callback at the blocking change-check-to-sleep boundary.
    ///
    /// The callback is test-only and runs after protected state is released,
    /// but before the independent change gate is acquired. It may advance this
    /// monitor's manual clock to exercise the epoch race deterministically.
    ///
    /// # Arguments
    ///
    /// * `hook` - Callback to run at the blocking wait boundary.
    #[cfg(test)]
    fn set_change_wait_boundary_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .change_wait_boundary_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::new(hook));
    }

    /// Runs the test-only blocking change-check-to-sleep callback.
    ///
    /// No protected-state or change-gate lock is held while the callback runs,
    /// so it may safely advance this monitor's manual clock.
    #[cfg(test)]
    fn run_change_wait_boundary_hook(&self) {
        let hook = self
            .change_wait_boundary_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook();
        }
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
        let previous = state
            .waiters
            .insert(waiter_id, MockWaiterState { notified: false });
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

    /// Returns the current shared modulo-u64 change token.
    ///
    /// The value is only compared with a later load to detect intervening
    /// changes. Correctness assumes fewer than `2^64` changes occur during one
    /// waiter's check-to-sleep window; a complete wrap in that window could be
    /// mistaken for no change. Protected predicate state remains synchronized
    /// by the monitor mutex.
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
        #[cfg(test)]
        self.run_change_wait_boundary_hook();
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
    /// Notification callers invoke this while holding protected state after
    /// assigning per-waiter notification ownership. It delegates to the same
    /// gate/token/Condvar/watch helper used by clock callbacks. Blocking
    /// waiters never acquire protected state while holding the gate, preventing
    /// a lock-order cycle.
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
        let _waiter_guard = self.waiter_guard_from_registered(waiter_id, false);
        let mut state = self.lock_state();
        loop {
            let observed_epoch = self.current_change_epoch();
            if Self::take_notification(&mut state, waiter_id)
                && !predicate(&state.value)
            {
                return action(&mut state.value);
            }
            state = self.wait_for_blocking_change(state, observed_epoch);
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
            let mut state = self.lock_timeout_state();
            if !predicate(&state.value) {
                return WaitTimeoutResult::Ready(action(&mut state.value));
            }
            let target_elapsed = self.condition_wait_target_elapsed(timeout);
            if self.elapsed() >= target_elapsed {
                return WaitTimeoutResult::TimedOut;
            }
            (
                Self::register_waiter_locked(&mut state, true),
                target_elapsed,
            )
        };
        let _waiter_guard = self.waiter_guard_from_registered(waiter_id, true);
        let mut state = self.lock_state();
        loop {
            let observed_epoch = self.current_change_epoch();
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
            state = self.wait_for_blocking_change(state, observed_epoch);
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
                let mut state = self.lock_timeout_state();
                if !predicate(&state.value) {
                    return WaitTimeoutResult::Ready(action(&mut state.value));
                }
                let target_elapsed =
                    self.condition_wait_target_elapsed(timeout);
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            Condvar,
            Mutex,
            mpsc,
        },
        thread,
        time::Duration,
    };

    #[cfg(feature = "async")]
    use super::AsyncTimeoutConditionWaiter;
    use super::{
        MockMonitor,
        TimeoutConditionWaitPhase,
        TimeoutConditionWaiter,
        WaitTimeoutResult,
    };

    /// Maximum real time permitted for every test coordination wait.
    const REAL_TIMEOUT: Duration = Duration::from_secs(1);
    /// Manual-clock budget used by the lock-contention timeout regressions.
    const WAIT_TIMEOUT: Duration = Duration::from_millis(5);

    /// Bounded recorder for timeout condition-wait initialization phases.
    #[derive(Default)]
    struct TimeoutConditionWaitSequence {
        /// Observed phases in callback order.
        phases: Mutex<Vec<TimeoutConditionWaitPhase>>,
        /// Signals each newly observed phase.
        changed: Condvar,
    }

    impl TimeoutConditionWaitSequence {
        /// Records one timeout condition-wait initialization phase.
        ///
        /// # Arguments
        ///
        /// * `phase` - Initialization phase reported by the monitor seam.
        fn record(&self, phase: TimeoutConditionWaitPhase) {
            let mut phases = self
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            phases.push(phase);
            self.changed.notify_all();
        }

        /// Waits at most [`REAL_TIMEOUT`] until `phase` has been observed.
        ///
        /// # Arguments
        ///
        /// * `phase` - Initialization phase required before returning.
        fn wait_until_observed(&self, phase: TimeoutConditionWaitPhase) {
            let phases = self
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (phases, _) = self
                .changed
                .wait_timeout_while(phases, REAL_TIMEOUT, |phases| {
                    !phases.contains(&phase)
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                phases.contains(&phase),
                "timeout waiter should reach the expected phase within one second"
            );
        }

        /// Waits for and verifies the exact initialization phase sequence.
        ///
        /// The real-time wait is bounded by [`REAL_TIMEOUT`].
        ///
        /// # Arguments
        ///
        /// * `expected` - Exact callback sequence required by the regression.
        fn assert_sequence(&self, expected: &[TimeoutConditionWaitPhase]) {
            let phases = self
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (phases, _) = self
                .changed
                .wait_timeout_while(phases, REAL_TIMEOUT, |phases| {
                    phases.len() < expected.len()
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(phases.as_slice(), expected);
        }
    }

    /// Verifies that blocking state-lock contention does not consume timeout.
    #[test]
    fn test_mock_monitor_blocking_timeout_budget_starts_after_initial_state_lock()
     {
        let monitor = Arc::new(MockMonitor::new(false));
        let sequence = Arc::new(TimeoutConditionWaitSequence::default());
        let waiter_sequence = Arc::clone(&sequence);
        monitor.set_timeout_condition_wait_hook(move |phase| {
            waiter_sequence.record(phase);
        });

        let (done_tx, done_rx) = mpsc::channel();
        let waiter = monitor.with_write(|_| {
            let waiter_monitor = Arc::clone(&monitor);
            let waiter = thread::spawn(move || {
                let result = waiter_monitor.wait_while_for(
                    WAIT_TIMEOUT,
                    |ready| !*ready,
                    |_| (),
                );
                done_tx
                    .send(result)
                    .expect("controller should receive blocking wait result");
            });
            sequence.wait_until_observed(TimeoutConditionWaitPhase::BeforeLock);
            monitor
                .monotonic_clock()
                .advance(WAIT_TIMEOUT.saturating_mul(2))
                .expect(
                    "manual clock should advance during state-lock contention",
                );
            waiter
        });
        sequence.assert_sequence(&[
            TimeoutConditionWaitPhase::BeforeLock,
            TimeoutConditionWaitPhase::AfterLock,
            TimeoutConditionWaitPhase::TargetCaptured,
        ]);
        assert!(monitor.wait_for_timeout_waiters(1, REAL_TIMEOUT));

        monitor
            .monotonic_clock()
            .advance(WAIT_TIMEOUT.saturating_sub(Duration::from_millis(1)))
            .expect("manual clock should remain within the fresh budget");
        assert!(done_rx.try_recv().is_err());
        monitor
            .monotonic_clock()
            .advance(Duration::from_millis(1))
            .expect("manual clock should reach the fresh target");
        assert_eq!(
            done_rx
                .recv_timeout(REAL_TIMEOUT)
                .expect("blocking timeout should finish within one second"),
            WaitTimeoutResult::TimedOut,
        );
        waiter
            .join()
            .expect("blocking timeout waiter should finish");
    }

    /// Verifies that a clock advance after the final state check but before
    /// condition-variable sleep is detected through the shared change token.
    #[test]
    fn test_mock_monitor_blocking_wait_observes_change_before_sleep() {
        let monitor = Arc::new(MockMonitor::new(false));
        let hook_clock = Arc::clone(&monitor.clock);
        monitor.set_change_wait_boundary_hook(move || {
            hook_clock
                .advance(WAIT_TIMEOUT)
                .expect("manual clock should advance at the wait boundary");
        });

        let waiter_monitor = Arc::clone(&monitor);
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let result = waiter_monitor.wait_while_for(
                WAIT_TIMEOUT,
                |ready| !*ready,
                |_| (),
            );
            done_tx
                .send(result)
                .expect("controller should receive boundary wait result");
        });

        assert_eq!(
            done_rx
                .recv_timeout(REAL_TIMEOUT)
                .expect("boundary clock change should not be lost"),
            WaitTimeoutResult::TimedOut,
        );
        waiter
            .join()
            .expect("boundary timeout waiter should finish");
    }

    /// Verifies that asynchronous state-lock contention does not consume
    /// timeout.
    #[cfg(feature = "async")]
    #[test]
    fn test_mock_monitor_async_timeout_budget_starts_after_initial_state_lock()
    {
        let monitor = Arc::new(MockMonitor::new(false));
        let sequence = Arc::new(TimeoutConditionWaitSequence::default());
        let waiter_sequence = Arc::clone(&sequence);
        monitor.set_timeout_condition_wait_hook(move |phase| {
            waiter_sequence.record(phase);
        });

        let (done_tx, done_rx) = mpsc::channel();
        let waiter = monitor.with_write(|_| {
            let waiter_monitor = Arc::clone(&monitor);
            let waiter = thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("async timeout waiter runtime should build");
                let result =
                    runtime.block_on(waiter_monitor.wait_while_for_async(
                        WAIT_TIMEOUT,
                        |ready| !*ready,
                        |_| (),
                    ));
                done_tx
                    .send(result)
                    .expect("controller should receive async wait result");
            });
            sequence.wait_until_observed(TimeoutConditionWaitPhase::BeforeLock);
            monitor
                .monotonic_clock()
                .advance(WAIT_TIMEOUT.saturating_mul(2))
                .expect(
                    "manual clock should advance during state-lock contention",
                );
            waiter
        });
        sequence.assert_sequence(&[
            TimeoutConditionWaitPhase::BeforeLock,
            TimeoutConditionWaitPhase::AfterLock,
            TimeoutConditionWaitPhase::TargetCaptured,
        ]);
        assert!(monitor.wait_for_timeout_waiters(1, REAL_TIMEOUT));

        monitor
            .monotonic_clock()
            .advance(WAIT_TIMEOUT.saturating_sub(Duration::from_millis(1)))
            .expect("manual clock should remain within the fresh budget");
        assert!(done_rx.try_recv().is_err());
        monitor
            .monotonic_clock()
            .advance(Duration::from_millis(1))
            .expect("manual clock should reach the fresh target");
        assert_eq!(
            done_rx
                .recv_timeout(REAL_TIMEOUT)
                .expect("async timeout should finish within one second"),
            WaitTimeoutResult::TimedOut,
        );
        waiter.join().expect("async timeout waiter should finish");
    }
}
