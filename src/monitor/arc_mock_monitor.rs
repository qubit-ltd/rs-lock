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

#[cfg(feature = "async")]
use std::future::Future;

use qubit_clock::ManualMonotonicClock;

#[cfg(feature = "async")]
use super::{
    AsyncConditionWaiter,
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

impl<T> ArcMockMonitor<T> {
    /// Creates a shared handle from an existing Arc-wrapped monitor.
    ///
    /// # Arguments
    ///
    /// * `inner` - Existing shared monitor allocation to wrap.
    ///
    /// # Returns
    ///
    /// A handle that preserves the identity and ownership of `inner`.
    pub fn from_arc(inner: Arc<MockMonitor<T>>) -> Self {
        Self { inner }
    }

    /// Borrows the Arc that owns the wrapped monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc without changing its strong reference count.
    pub fn as_arc(&self) -> &Arc<MockMonitor<T>> {
        &self.inner
    }

    /// Consumes this handle and returns the Arc that owns the monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc, preserving the wrapped monitor allocation.
    pub fn into_arc(self) -> Arc<MockMonitor<T>> {
        self.inner
    }
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
}

impl<T: Send + 'static> Notifier for ArcMockMonitor<T> {
    /// Wakes one waiter.
    fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all waiters.
    fn notify_all(&self) {
        self.inner.notify_all();
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
    ) -> impl Future<Output = R> + Send + 'a
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
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
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
