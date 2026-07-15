// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Arc-wrapped mock monitor.

use std::{
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use qubit_clock::ManualMonotonicClock;

#[cfg(feature = "async")]
use super::{
    AsyncConditionWaiter,
    AsyncMonitorFuture,
    AsyncTimeoutConditionWaiter,
};
use super::{
    ConditionWaiter,
    MockMonitor,
    Notifier,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
};

/// Cloneable handle around a [`MockMonitor`].
pub struct ArcMockMonitor<T> {
    /// Shared mock monitor.
    inner: Arc<MockMonitor<T>>,
}

impl<T: Send + 'static> ArcMockMonitor<T> {
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

    /// Creates an Arc-wrapped mock monitor driven by a shared manual clock.
    ///
    /// # Parameters
    /// - `state`: Initial protected state.
    /// - `clock`: Manual clock used for timeout deadlines.
    ///
    /// # Returns
    /// A cloneable monitor handle sharing `clock` with other test components.
    pub fn from_clock(state: T, clock: Arc<ManualMonotonicClock>) -> Self {
        Self {
            inner: Arc::new(MockMonitor::from_clock(state, clock)),
        }
    }

    /// Returns the current mock elapsed time.
    pub fn elapsed(&self) -> Duration {
        self.inner.elapsed()
    }

    /// Returns the manual clock used by timeout methods.
    #[must_use]
    pub fn monotonic_clock(&self) -> &ManualMonotonicClock {
        self.inner.monotonic_clock()
    }

    /// Returns the number of timeout wait operations ready to observe changes.
    #[must_use]
    pub fn pending_timeout_waiters(&self) -> usize {
        self.inner.pending_timeout_waiters()
    }

    /// Blocks in real time until enough timeout waiters are ready.
    ///
    /// Returns `false` if `real_timeout` expires before `expected_count`
    /// waiters are active. The real-time guard never contributes to mock time.
    #[must_use]
    pub fn wait_for_timeout_waiters(
        &self,
        expected_count: usize,
        real_timeout: Duration,
    ) -> bool {
        self.inner
            .wait_for_timeout_waiters(expected_count, real_timeout)
    }

    /// Reads protected state.
    pub fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.inner.with_read(f)
    }

    /// Mutates protected state without notifying.
    pub fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.with_write(f)
    }

    /// Mutates protected state and wakes one waiter.
    pub fn with_write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.with_write_notify_one(f)
    }

    /// Mutates protected state and wakes all waiters.
    pub fn with_write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.with_write_notify_all(f)
    }

    /// Wakes one waiter.
    pub fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all waiters.
    pub fn notify_all(&self) {
        self.inner.notify_all();
    }

    /// Blocks until the predicate becomes true, then runs the action.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    pub fn wait_until<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        <MockMonitor<T> as ConditionWaiter>::wait_until(
            self.inner.as_ref(),
            predicate,
            action,
        )
    }

    /// Blocks while the predicate remains true, then runs the action.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    pub fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        <MockMonitor<T> as ConditionWaiter>::wait_while(
            self.inner.as_ref(),
            predicate,
            action,
        )
    }

    /// Blocks until the predicate becomes true or mock timeout expires.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum mock duration to wait.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the action result, or
    /// [`WaitTimeoutResult::TimedOut`] when mock time reaches the timeout.
    pub fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        <MockMonitor<T> as TimeoutConditionWaiter>::wait_until_for(
            self.inner.as_ref(),
            timeout,
            predicate,
            action,
        )
    }

    /// Blocks while the predicate remains true or until mock timeout expires.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum mock duration to wait.
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the action result, or
    /// [`WaitTimeoutResult::TimedOut`] when mock time reaches the timeout.
    pub fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        <MockMonitor<T> as TimeoutConditionWaiter>::wait_while_for(
            self.inner.as_ref(),
            timeout,
            predicate,
            action,
        )
    }

    /// Returns a future that waits until the predicate becomes true.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// A future resolving to the action result.
    #[cfg(feature = "async")]
    pub fn wait_until_async<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        T: Send,
        R: Send + 'a,
        P: FnMut(&T) -> bool + Send + 'a,
        F: FnOnce(&mut T) -> R + Send + 'a,
    {
        <MockMonitor<T> as AsyncConditionWaiter>::wait_until_async(
            self.inner.as_ref(),
            predicate,
            action,
        )
    }

    /// Returns a future that waits while the predicate remains true.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A future resolving to the action result.
    #[cfg(feature = "async")]
    pub fn wait_while_async<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        T: Send,
        R: Send + 'a,
        P: FnMut(&T) -> bool + Send + 'a,
        F: FnOnce(&mut T) -> R + Send + 'a,
    {
        <MockMonitor<T> as AsyncConditionWaiter>::wait_while_async(
            self.inner.as_ref(),
            predicate,
            action,
        )
    }

    /// Returns a future that waits until the predicate becomes true or times
    /// out.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum mock duration to wait.
    /// * `predicate` - Predicate that returns `true` when the state is ready.
    /// * `action` - Action to run after the predicate becomes true.
    ///
    /// # Returns
    ///
    /// A future resolving to the timed wait result.
    #[cfg(feature = "async")]
    pub fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        T: Send,
        R: Send + 'a,
        P: FnMut(&T) -> bool + Send + 'a,
        F: FnOnce(&mut T) -> R + Send + 'a,
    {
        <MockMonitor<T> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            self.inner.as_ref(),
            timeout,
            predicate,
            action,
        )
    }

    /// Returns a future that waits while the predicate remains true or times
    /// out.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum mock duration to wait.
    /// * `predicate` - Predicate that returns `true` while waiting should
    ///   continue.
    /// * `action` - Action to run after the predicate becomes false.
    ///
    /// # Returns
    ///
    /// A future resolving to the timed wait result.
    #[cfg(feature = "async")]
    pub fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        T: Send,
        R: Send + 'a,
        P: FnMut(&T) -> bool + Send + 'a,
        F: FnOnce(&mut T) -> R + Send + 'a,
    {
        <MockMonitor<T> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
            self.inner.as_ref(),
            timeout,
            predicate,
            action,
        )
    }
}

impl<T: Send + 'static> Notifier for ArcMockMonitor<T> {
    /// Wakes one waiter.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send + 'static> ConditionWaiter for ArcMockMonitor<T> {
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

impl<T: Send + 'static> TimeoutConditionWaiter for ArcMockMonitor<T> {
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
impl<T: Send + 'static> AsyncConditionWaiter for ArcMockMonitor<T> {
    type State = T;

    /// Returns a future that waits while the predicate remains true.
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.inner.wait_while_async(predicate, action)
    }
}

#[cfg(feature = "async")]
impl<T: Send + 'static> AsyncTimeoutConditionWaiter for ArcMockMonitor<T> {
    /// Returns a future that waits while the predicate remains true or times
    /// out.
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

impl<T: Send + 'static> From<T> for ArcMockMonitor<T> {
    /// Creates an Arc-wrapped mock monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default + Send + 'static> Default for ArcMockMonitor<T> {
    /// Creates an Arc-wrapped mock monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}
