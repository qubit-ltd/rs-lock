/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Arc-wrapped mock monitor.

use std::{
    ops::Deref,
    sync::Arc,
    time::Duration,
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
    MockMonitor,
    NotificationWaiter,
    Notifier,
    TimeoutConditionWaiter,
    TimeoutNotificationWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

/// Cloneable handle around a [`MockMonitor`].
pub struct ArcMockMonitor<T> {
    /// Shared mock monitor.
    inner: Arc<MockMonitor<T>>,
}

impl<T> ArcMockMonitor<T> {
    /// Creates an Arc-wrapped mock monitor.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A cloneable mock monitor handle.
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(MockMonitor::new(state)),
        }
    }

    /// Returns the current mock elapsed time.
    pub fn elapsed(&self) -> Duration {
        self.inner.elapsed()
    }

    /// Sets the current mock elapsed time.
    ///
    /// # Arguments
    ///
    /// * `elapsed` - New mock elapsed time.
    pub fn set_elapsed(&self, elapsed: Duration) {
        self.inner.set_elapsed(elapsed);
    }

    /// Advances mock elapsed time.
    ///
    /// # Arguments
    ///
    /// * `duration` - Duration added to current mock elapsed time.
    pub fn advance(&self, duration: Duration) {
        self.inner.advance(duration);
    }

    /// Resets mock elapsed time to zero.
    pub fn reset_elapsed(&self) {
        self.inner.reset_elapsed();
    }

    /// Reads protected state.
    pub fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.inner.read(f)
    }

    /// Mutates protected state without notifying.
    pub fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.write(f)
    }

    /// Mutates protected state and wakes one waiter.
    pub fn write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.write_notify_one(f)
    }

    /// Mutates protected state and wakes all waiters.
    pub fn write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.write_notify_all(f)
    }

    /// Wakes one waiter.
    pub fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all waiters.
    pub fn notify_all(&self) {
        self.inner.notify_all();
    }
}

impl<T> Notifier for ArcMockMonitor<T> {
    /// Wakes one waiter.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T> NotificationWaiter for ArcMockMonitor<T> {
    /// Blocks until notification.
    fn wait(&self) {
        self.inner.wait();
    }
}

impl<T> TimeoutNotificationWaiter for ArcMockMonitor<T> {
    /// Blocks until notification or mock timeout.
    fn wait_for(&self, timeout: Duration) -> WaitTimeoutStatus {
        self.inner.wait_for(timeout)
    }
}

impl<T> ConditionWaiter for ArcMockMonitor<T> {
    type State = T;

    /// Blocks while the predicate remains true, then runs the action.
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.wait_while(predicate, action)
    }
}

impl<T> TimeoutConditionWaiter for ArcMockMonitor<T> {
    /// Blocks while the predicate remains true or until mock timeout expires.
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.wait_while_for(timeout, predicate, action)
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncNotificationWaiter for ArcMockMonitor<T> {
    /// Returns a future that resolves after an async notification.
    fn wait_async<'a>(&'a self) -> AsyncMonitorFuture<'a, ()> {
        self.inner.wait_async()
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncTimeoutNotificationWaiter for ArcMockMonitor<T> {
    /// Returns a future that resolves after notification or mock timeout.
    fn wait_for_async<'a>(
        &'a self,
        timeout: Duration,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutStatus> {
        self.inner.wait_for_async(timeout)
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncConditionWaiter for ArcMockMonitor<T> {
    type State = T;

    /// Returns a future that waits while the predicate remains true.
    fn wait_while_async<'a, R, P, F>(&'a self, predicate: P, action: F) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.inner.wait_while_async(predicate, action)
    }
}

#[cfg(feature = "async")]
impl<T: Send> AsyncTimeoutConditionWaiter for ArcMockMonitor<T> {
    /// Returns a future that waits while the predicate remains true or times out.
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.inner.wait_while_for_async(timeout, predicate, action)
    }
}

impl<T> AsRef<MockMonitor<T>> for ArcMockMonitor<T> {
    /// Returns a reference to the wrapped mock monitor.
    fn as_ref(&self) -> &MockMonitor<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcMockMonitor<T> {
    type Target = MockMonitor<T>;

    /// Dereferences to the wrapped mock monitor.
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> Clone for ArcMockMonitor<T> {
    /// Clones this shared mock monitor handle.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> From<T> for ArcMockMonitor<T> {
    /// Creates an Arc-wrapped mock monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcMockMonitor<T> {
    /// Creates an Arc-wrapped mock monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}
