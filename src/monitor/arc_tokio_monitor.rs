// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Arc-wrapped Tokio monitor.

use qubit_clock::{
    TimeError,
    Timer,
};
use std::{
    future::Future,
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use super::{
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
    Notifier,
    TokioMonitor,
    WaitTimeoutResult,
};

/// Cloneable handle around a [`TokioMonitor`].
pub struct ArcTokioMonitor<T> {
    /// Shared Tokio monitor.
    inner: Arc<TokioMonitor<T>>,
}

impl<T> ArcTokioMonitor<T> {
    /// Creates an Arc-wrapped Tokio monitor.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A cloneable Tokio monitor handle.
    #[inline]
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(TokioMonitor::new(state)),
        }
    }

    /// Creates an Arc-wrapped Tokio monitor using an injected Timer.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial protected state.
    /// * `timer` - Timer driving asynchronous deadlines.
    ///
    /// # Returns
    ///
    /// A cloneable Tokio monitor handle bound to `timer`.
    pub fn with_timer(state: T, timer: Arc<dyn Timer>) -> Self {
        Self {
            inner: Arc::new(TokioMonitor::with_timer(state, timer)),
        }
    }

    /// Creates a shared handle from an existing Arc-wrapped monitor.
    ///
    /// # Parameters
    ///
    /// * `inner` - Existing shared monitor allocation to wrap.
    ///
    /// # Returns
    ///
    /// A handle that preserves the identity and ownership of `inner`.
    #[inline]
    pub fn from_arc(inner: Arc<TokioMonitor<T>>) -> Self {
        Self { inner }
    }

    /// Borrows the Arc that owns the wrapped monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc without changing its strong reference count.
    #[inline(always)]
    pub fn as_arc(&self) -> &Arc<TokioMonitor<T>> {
        &self.inner
    }

    /// Consumes this handle and returns the Arc that owns the monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc, preserving the wrapped monitor allocation.
    #[inline(always)]
    pub fn into_arc(self) -> Arc<TokioMonitor<T>> {
        self.inner
    }
}

impl<T> Notifier for ArcTokioMonitor<T> {
    /// Wakes one async waiter.
    #[inline(always)]
    fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all async waiters.
    #[inline(always)]
    fn notify_all(&self) {
        self.inner.notify_all();
    }
}

impl<T: Send> AsyncConditionWaiter for ArcTokioMonitor<T> {
    type State = T;

    /// Returns a future that waits while the predicate remains true.
    #[inline(always)]
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

impl<T: Send> AsyncTimeoutConditionWaiter for ArcTokioMonitor<T> {
    /// Returns a future that waits while the predicate remains true or times
    /// out.
    #[inline(always)]
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> impl Future<Output = Result<WaitTimeoutResult<R>, TimeError>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.inner.wait_while_for_async(timeout, predicate, action)
    }
}

impl<T> AsRef<TokioMonitor<T>> for ArcTokioMonitor<T> {
    /// Returns a reference to the wrapped Tokio monitor.
    #[inline(always)]
    fn as_ref(&self) -> &TokioMonitor<T> {
        self.inner.as_ref()
    }
}

impl<T> Deref for ArcTokioMonitor<T> {
    type Target = TokioMonitor<T>;

    /// Dereferences to the wrapped Tokio monitor.
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> Clone for ArcTokioMonitor<T> {
    /// Clones this shared Tokio monitor handle.
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> From<T> for ArcTokioMonitor<T> {
    /// Creates an Arc-wrapped Tokio monitor from an initial state value.
    #[inline(always)]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcTokioMonitor<T> {
    /// Creates an Arc-wrapped Tokio monitor containing `T::default()`.
    #[inline(always)]
    fn default() -> Self {
        Self::new(T::default())
    }
}
