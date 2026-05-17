/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Mock monitor with manually controlled timeout time.

use std::sync::{
    Condvar,
    Mutex,
    MutexGuard,
};
use std::time::Duration;

#[cfg(feature = "async")]
use tokio::sync::{
    Notify,
    watch,
};

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
/// but timeout methods use manually controlled mock elapsed time. Advancing the
/// mock time wakes waiters so they can recheck predicates and timeout budgets.
pub struct MockMonitor<T> {
    /// Protected mock state and clock state.
    state: Mutex<MockMonitorState<T>>,
    /// Condition variable used by blocking waiters.
    changed: Condvar,
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
    /// Manually controlled elapsed time.
    elapsed: Duration,
    /// Epoch incremented only by notification calls.
    notification_epoch: u64,
    /// Epoch incremented by notifications and mock time changes.
    change_epoch: u64,
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
    /// A mock monitor whose elapsed time starts at zero.
    pub fn new(state: T) -> Self {
        #[cfg(feature = "async")]
        let (async_change_sender, _) = watch::channel(0);
        Self {
            state: Mutex::new(MockMonitorState {
                value: state,
                elapsed: Duration::ZERO,
                notification_epoch: 0,
                change_epoch: 0,
            }),
            changed: Condvar::new(),
            #[cfg(feature = "async")]
            async_notification: Notify::new(),
            #[cfg(feature = "async")]
            async_change_sender,
        }
    }

    /// Returns the current mock elapsed time.
    ///
    /// # Returns
    ///
    /// The elapsed time used by timeout waits.
    pub fn elapsed(&self) -> Duration {
        self.lock_state().elapsed
    }

    /// Sets the current mock elapsed time.
    ///
    /// This wakes timeout waiters so they can recheck their budgets.
    ///
    /// # Arguments
    ///
    /// * `elapsed` - New mock elapsed time.
    pub fn set_elapsed(&self, elapsed: Duration) {
        let change_epoch = {
            let mut state = self.lock_state();
            state.elapsed = elapsed;
            Self::advance_change_epoch(&mut state)
        };
        self.changed.notify_all();
        self.notify_async_change(change_epoch);
    }

    /// Advances mock elapsed time by a relative duration.
    ///
    /// # Arguments
    ///
    /// * `duration` - Duration added to the current mock elapsed time.
    pub fn advance(&self, duration: Duration) {
        let change_epoch = {
            let mut state = self.lock_state();
            state.elapsed = state.elapsed.saturating_add(duration);
            Self::advance_change_epoch(&mut state)
        };
        self.changed.notify_all();
        self.notify_async_change(change_epoch);
    }

    /// Resets mock elapsed time to zero.
    pub fn reset_elapsed(&self) {
        self.set_elapsed(Duration::ZERO);
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

impl<T> Notifier for MockMonitor<T> {
    /// Wakes one waiter if one is blocked.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T> NotificationWaiter for MockMonitor<T> {
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

impl<T> TimeoutNotificationWaiter for MockMonitor<T> {
    /// Blocks until a notification happens or mock elapsed time reaches timeout.
    fn wait_for(&self, timeout: Duration) -> WaitTimeoutStatus {
        let mut state = self.lock_state();
        let observed_epoch = state.notification_epoch;
        let target_elapsed = state.elapsed.saturating_add(timeout);
        loop {
            if state.notification_epoch != observed_epoch {
                return WaitTimeoutStatus::Woken;
            }
            if state.elapsed >= target_elapsed {
                return WaitTimeoutStatus::TimedOut;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl<T> ConditionWaiter for MockMonitor<T> {
    type State = T;

    /// Blocks until the predicate becomes true, then runs the action.
    fn wait_until<R, P, F>(&self, mut predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while(|state| !predicate(state), action)
    }

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

impl<T> TimeoutConditionWaiter for MockMonitor<T> {
    /// Blocks until the predicate becomes true or mock elapsed time reaches timeout.
    fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.wait_while_for(timeout, |state| !predicate(state), action)
    }

    /// Blocks while the predicate remains true or until mock elapsed time reaches timeout.
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
        let mut state = self.lock_state();
        let target_elapsed = state.elapsed.saturating_add(timeout);
        loop {
            if !predicate(&state.value) {
                return WaitTimeoutResult::Ready(action(&mut state.value));
            }
            if state.elapsed >= target_elapsed {
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
impl<T: Send> AsyncNotificationWaiter for MockMonitor<T> {
    /// Returns a future that resolves after an async notification.
    fn async_wait<'a>(&'a self) -> AsyncMonitorFuture<'a, ()> {
        let notified = self.async_notification.notified();
        Box::pin(notified)
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncTimeoutNotificationWaiter for MockMonitor<T> {
    /// Returns a future that resolves after notification or mock timeout.
    fn async_wait_for<'a>(
        &'a self,
        timeout: Duration,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutStatus> {
        let target_elapsed = self.elapsed().saturating_add(timeout);
        let mut change_receiver = self.async_change_sender.subscribe();
        Box::pin(async move {
            loop {
                if self.elapsed() >= target_elapsed {
                    return WaitTimeoutStatus::TimedOut;
                }
                let notified = self.async_notification.notified();
                tokio::select! {
                    () = notified => return WaitTimeoutStatus::Woken,
                    changed = change_receiver.changed() => {
                        changed.expect("mock monitor sender should live while the monitor is borrowed");
                    }
                }
            }
        })
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncConditionWaiter for MockMonitor<T> {
    type State = T;

    /// Returns a future that waits until the predicate becomes true.
    fn async_wait_until<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.async_wait_while(move |state| !predicate(state), action)
    }

    /// Returns a future that waits while the predicate remains true.
    fn async_wait_while<'a, R, P, F>(
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
impl<T: Send> AsyncTimeoutConditionWaiter for MockMonitor<T> {
    /// Returns a future that waits until the predicate becomes true or times out.
    fn async_wait_until_for<'a, R, P, F>(
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
        self.async_wait_while_for(timeout, move |state| !predicate(state), action)
    }

    /// Returns a future that waits while the predicate remains true or times out.
    fn async_wait_while_for<'a, R, P, F>(
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
        let target_elapsed = self.elapsed().saturating_add(timeout);
        let mut change_receiver = self.async_change_sender.subscribe();
        Box::pin(async move {
            loop {
                {
                    let mut state = self.lock_state();
                    if !predicate(&state.value) {
                        return WaitTimeoutResult::Ready(action(&mut state.value));
                    }
                    if state.elapsed >= target_elapsed {
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

impl<T> From<T> for MockMonitor<T> {
    /// Creates a mock monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for MockMonitor<T> {
    /// Creates a mock monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}
