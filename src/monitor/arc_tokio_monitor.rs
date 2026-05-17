/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Arc-wrapped Tokio monitor.

use std::{ops::Deref, sync::Arc, time::Duration};

use super::{
    AsyncConditionWaiter, AsyncMonitorFuture, AsyncNotificationWaiter, AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter, Notifier, TokioMonitor, WaitTimeoutResult, WaitTimeoutStatus,
};

/// Cloneable handle around a [`TokioMonitor`].
pub struct ArcTokioMonitor<T> {
    /// Shared Tokio monitor.
    inner: Arc<TokioMonitor<T>>,
}

impl<T> ArcTokioMonitor<T> {
    /// Creates an Arc-wrapped Tokio monitor.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A cloneable Tokio monitor handle.
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(TokioMonitor::new(state)),
        }
    }

    /// Reads protected state asynchronously.
    pub async fn async_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.inner.async_read(f).await
    }

    /// Mutates protected state asynchronously without notifying.
    pub async fn async_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.async_write(f).await
    }

    /// Mutates protected state asynchronously and wakes one waiter.
    pub async fn async_write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.async_write_notify_one(f).await
    }

    /// Mutates protected state asynchronously and wakes all waiters.
    pub async fn async_write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.async_write_notify_all(f).await
    }

    /// Wakes one async waiter.
    pub fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all async waiters.
    pub fn notify_all(&self) {
        self.inner.notify_all();
    }
}

impl<T> Notifier for ArcTokioMonitor<T> {
    /// Wakes one async waiter.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all async waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send> AsyncNotificationWaiter for ArcTokioMonitor<T> {
    /// Returns a future that resolves after an async notification.
    fn async_wait<'a>(&'a self) -> AsyncMonitorFuture<'a, ()> {
        self.inner.async_wait()
    }
}

impl<T: Send> AsyncTimeoutNotificationWaiter for ArcTokioMonitor<T> {
    /// Returns a future that resolves after notification or timeout.
    fn async_wait_for<'a>(
        &'a self,
        timeout: Duration,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutStatus> {
        self.inner.async_wait_for(timeout)
    }
}

impl<T: Send> AsyncConditionWaiter for ArcTokioMonitor<T> {
    type State = T;

    /// Returns a future that waits until the predicate becomes true.
    fn async_wait_until<'a, R, P, F>(&'a self, predicate: P, action: F) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.inner.async_wait_until(predicate, action)
    }

    /// Returns a future that waits while the predicate remains true.
    fn async_wait_while<'a, R, P, F>(&'a self, predicate: P, action: F) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.inner.async_wait_while(predicate, action)
    }
}

impl<T: Send> AsyncTimeoutConditionWaiter for ArcTokioMonitor<T> {
    /// Returns a future that waits until the predicate becomes true or times out.
    fn async_wait_until_for<'a, R, P, F>(
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
        self.inner.async_wait_until_for(timeout, predicate, action)
    }

    /// Returns a future that waits while the predicate remains true or times out.
    fn async_wait_while_for<'a, R, P, F>(
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
        self.inner.async_wait_while_for(timeout, predicate, action)
    }
}

impl<T> AsRef<TokioMonitor<T>> for ArcTokioMonitor<T> {
    /// Returns a reference to the wrapped Tokio monitor.
    fn as_ref(&self) -> &TokioMonitor<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcTokioMonitor<T> {
    type Target = TokioMonitor<T>;

    /// Dereferences to the wrapped Tokio monitor.
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> Clone for ArcTokioMonitor<T> {
    /// Clones this shared Tokio monitor handle.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> From<T> for ArcTokioMonitor<T> {
    /// Creates an Arc-wrapped Tokio monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcTokioMonitor<T> {
    /// Creates an Arc-wrapped Tokio monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}
