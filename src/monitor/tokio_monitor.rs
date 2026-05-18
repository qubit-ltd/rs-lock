/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tokio-based asynchronous monitor.

use std::time::{
    Duration,
    Instant,
};

use tokio::sync::{
    Mutex,
    Notify,
};

use super::{
    AsyncConditionWaiter,
    AsyncMonitorFuture,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
    Notifier,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

/// Asynchronous monitor built on Tokio synchronization primitives.
///
/// `TokioMonitor` protects one state value with a Tokio mutex and coordinates
/// waiters with a Tokio notification primitive. Notification semantics follow
/// Tokio's [`Notify`] behavior.
pub struct TokioMonitor<T> {
    /// Protected monitor state.
    state: Mutex<T>,
    /// Notification primitive used to wake async waiters.
    changed: Notify,
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
    pub fn new(state: T) -> Self {
        Self {
            state: Mutex::new(state),
            changed: Notify::new(),
        }
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
    pub async fn async_read<R, F>(&self, f: F) -> R
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
    pub async fn async_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.state.lock().await;
        f(&mut *guard)
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
    pub async fn async_write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.async_write(f).await;
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
    pub async fn async_write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.async_write(f).await;
        self.notify_all();
        result
    }

    /// Wakes one async waiter.
    pub fn notify_one(&self) {
        self.changed.notify_one();
    }

    /// Wakes all async waiters.
    pub fn notify_all(&self) {
        self.changed.notify_waiters();
    }

    /// Calculates remaining timeout budget from a call-time start instant.
    ///
    /// # Arguments
    ///
    /// * `start` - Instant captured when the public wait method was called.
    /// * `timeout` - Total timeout budget.
    ///
    /// # Returns
    ///
    /// The remaining budget, or zero when the budget is exhausted.
    fn remaining_timeout(start: Instant, timeout: Duration) -> Duration {
        timeout.checked_sub(start.elapsed()).unwrap_or_default()
    }
}

impl<T> Notifier for TokioMonitor<T> {
    /// Wakes one async waiter.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all async waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send> AsyncNotificationWaiter for TokioMonitor<T> {
    /// Returns a future that resolves after a Tokio notification.
    fn wait_async<'a>(&'a self) -> AsyncMonitorFuture<'a, ()> {
        Box::pin(self.changed.notified())
    }
}

impl<T: Send> AsyncTimeoutNotificationWaiter for TokioMonitor<T> {
    /// Returns a future that resolves after notification or timeout.
    fn wait_for_async<'a>(
        &'a self,
        timeout: Duration,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutStatus> {
        let start = Instant::now();
        let notified = self.changed.notified();
        Box::pin(async move {
            let remaining = Self::remaining_timeout(start, timeout);
            if remaining.is_zero() {
                return WaitTimeoutStatus::TimedOut;
            }
            match tokio::time::timeout(remaining, notified).await {
                Ok(()) => WaitTimeoutStatus::Woken,
                Err(_) => WaitTimeoutStatus::TimedOut,
            }
        })
    }
}

impl<T: Send> AsyncConditionWaiter for TokioMonitor<T> {
    type State = T;

    /// Returns a future that waits until the predicate becomes true.
    fn wait_until_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_async(move |state| !predicate(state), action)
    }

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
            let mut guard = self.state.lock().await;
            while predicate(&*guard) {
                let notified = self.changed.notified();
                drop(guard);
                notified.await;
                guard = self.state.lock().await;
            }
            action(&mut *guard)
        })
    }
}

impl<T: Send> AsyncTimeoutConditionWaiter for TokioMonitor<T> {
    /// Returns a future that waits until the predicate becomes true or times out.
    fn wait_until_for_async<'a, R, P, F>(
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
        self.wait_while_for_async(timeout, move |state| !predicate(state), action)
    }

    /// Returns a future that waits while the predicate remains true or times out.
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
        let start = Instant::now();
        Box::pin(async move {
            let mut guard = self.state.lock().await;
            loop {
                if !predicate(&*guard) {
                    return WaitTimeoutResult::Ready(action(&mut *guard));
                }

                let remaining = Self::remaining_timeout(start, timeout);
                if remaining.is_zero() {
                    return WaitTimeoutResult::TimedOut;
                }

                let notified = self.changed.notified();
                drop(guard);
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    guard = self.state.lock().await;
                    if !predicate(&*guard) {
                        return WaitTimeoutResult::Ready(action(&mut *guard));
                    }
                    return WaitTimeoutResult::TimedOut;
                }
                guard = self.state.lock().await;
            }
        })
    }
}

impl<T> From<T> for TokioMonitor<T> {
    /// Creates a Tokio monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for TokioMonitor<T> {
    /// Creates a Tokio monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}
