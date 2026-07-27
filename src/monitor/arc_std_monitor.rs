// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Arc StdMonitor
//!
//! Provides an Arc-wrapped synchronous monitor for condition-based state
//! coordination across threads.

use qubit_clock::{MonotonicInstant, TimeError, Timer};
use std::{ops::Deref, sync::Arc, time::Duration};

use super::{
    ConditionWaiter, Monitor, Notifier, StdMonitor, TimeoutConditionWaiter, WaitTimeoutResult,
};

/// Arc-wrapped monitor for shared condition-based state coordination.
///
/// `ArcStdMonitor` stores a [`StdMonitor`] behind an [`Arc`], so callers can
/// clone the monitor handle directly without writing
/// `Arc::new(StdMonitor::new(...))`. It preserves the same guard-based waiting,
/// predicate-based waiting, and poison recovery semantics as [`StdMonitor`]. It
/// implements [`Deref`] and [`AsRef`] so callers can pass it to APIs that
/// expect a [`StdMonitor`] reference.
///
/// # Type Parameters
///
/// * `T` - The state protected by this monitor.
///
/// # Examples
///
/// ```rust
/// use std::thread;
///
/// use qubit_lock::ArcStdMonitor;
///
/// let monitor = ArcStdMonitor::new(false);
/// let waiter_monitor = monitor.clone();
///
/// let waiter = thread::spawn(move || {
///     waiter_monitor.wait_until(
///         |ready| *ready,
///         |ready| {
///             *ready = false;
///         },
///     );
/// });
///
/// monitor.with_write_notify_all(|ready| {
///     *ready = true;
/// });
///
/// waiter.join().expect("waiter should finish");
/// assert!(!monitor.with_read(|ready| *ready));
/// ```
pub struct ArcStdMonitor<T> {
    /// Shared monitor instance.
    inner: Arc<StdMonitor<T>>,
}

impl<T> ArcStdMonitor<T> {
    /// Creates an Arc-wrapped monitor protecting the supplied state value.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle initialized with the supplied state.
    ///
    /// # Panics
    ///
    /// Panics if all process-wide clock-domain identifiers are exhausted.
    #[inline]
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(StdMonitor::new(state)),
        }
    }

    /// Creates an Arc-wrapped monitor using an injected Timer.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial protected state.
    /// * `timer` - Timer driving monitor deadlines.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle bound to `timer`.
    #[inline]
    pub fn with_timer(state: T, timer: Arc<dyn Timer>) -> Self {
        Self {
            inner: Arc::new(StdMonitor::with_timer(state, timer)),
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
    pub fn from_arc(inner: Arc<StdMonitor<T>>) -> Self {
        Self { inner }
    }

    /// Borrows the Arc that owns the wrapped monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc without changing its strong reference count.
    #[must_use = "use the borrowed Arc or omit the call"]
    #[inline(always)]
    pub fn as_arc(&self) -> &Arc<StdMonitor<T>> {
        &self.inner
    }

    /// Consumes this handle and returns the Arc that owns the monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc, preserving the wrapped monitor allocation.
    #[inline(always)]
    pub fn into_arc(self) -> Arc<StdMonitor<T>> {
        self.inner
    }
}

impl<T> AsRef<StdMonitor<T>> for ArcStdMonitor<T> {
    /// Returns a reference to the underlying standard monitor.
    ///
    /// This is useful when callers need an explicit [`StdMonitor`] reference
    /// while keeping the cloneable [`ArcStdMonitor`] handle.
    #[inline(always)]
    fn as_ref(&self) -> &StdMonitor<T> {
        self.inner.as_ref()
    }
}

impl<T> Notifier for ArcStdMonitor<T> {
    /// Wakes one thread waiting on this monitor.
    #[inline(always)]
    fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all threads waiting on this monitor.
    #[inline(always)]
    fn notify_all(&self) {
        self.inner.notify_all();
    }
}

impl<T> ConditionWaiter for ArcStdMonitor<T> {
    type State = T;

    /// Blocks while the predicate remains true, then runs the action.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate`, `action`, or the wrapped monitor.
    #[inline(always)]
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <StdMonitor<T> as ConditionWaiter>::wait_while(self.inner.as_ref(), predicate, action)
    }
}

impl<T> Monitor for ArcStdMonitor<T> {
    /// Delegates protected-state reading to the wrapped monitor.
    #[inline(always)]
    fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&Self::State) -> R,
    {
        self.inner.with_read(f)
    }

    /// Delegates protected-state mutation to the wrapped monitor.
    #[inline(always)]
    fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R,
    {
        self.inner.with_write(f)
    }
}

impl<T> TimeoutConditionWaiter for ArcStdMonitor<T> {
    /// Blocks while the predicate remains true or until an absolute deadline.
    #[inline(always)]
    fn wait_while_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <StdMonitor<T> as TimeoutConditionWaiter>::wait_while_with_deadline(
            self.inner.as_ref(),
            deadline,
            predicate,
            action,
        )
    }

    /// Blocks while the predicate remains true or until the timeout expires.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `predicate`, `action`, or the wrapped monitor.
    #[inline(always)]
    fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <StdMonitor<T> as TimeoutConditionWaiter>::wait_while_for(
            self.inner.as_ref(),
            timeout,
            predicate,
            action,
        )
    }
}

impl<T> Deref for ArcStdMonitor<T> {
    type Target = StdMonitor<T>;

    /// Dereferences this wrapper to the underlying standard monitor.
    ///
    /// Method-call dereferencing lets callers use native [`StdMonitor`] APIs
    /// directly, while this wrapper still provides cloneable ownership.
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> From<T> for ArcStdMonitor<T> {
    /// Creates an Arc-wrapped standard monitor from an initial state value.
    ///
    /// # Parameters
    ///
    /// * `value` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A cloneable standard monitor handle protecting `value`.
    ///
    /// # Panics
    ///
    /// Panics if all process-wide clock-domain identifiers are exhausted.
    #[inline(always)]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcStdMonitor<T> {
    /// Creates an Arc-wrapped monitor containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle protecting the default value for `T`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `T::default()`. Panics if all process-wide
    /// clock-domain identifiers are exhausted.
    #[inline(always)]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Clone for ArcStdMonitor<T> {
    /// Clones this monitor handle.
    ///
    /// The cloned handle shares the same protected state and condition
    /// variable with the original.
    ///
    /// # Returns
    ///
    /// A new handle sharing the same monitor state.
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
