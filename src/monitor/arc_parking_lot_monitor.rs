// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Arc ParkingLotMonitor
//!
//! Provides an Arc-wrapped synchronous monitor for condition-based state
//! coordination across threads.

use qubit_clock::{
    TimeError,
    Timer,
};
use std::{
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use super::{
    ConditionWaiter,
    Notifier,
    ParkingLotMonitor,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
};

/// Arc-wrapped monitor for shared condition-based state coordination.
///
/// `ArcParkingLotMonitor` stores a [`ParkingLotMonitor`] behind an [`Arc`], so
/// callers can clone the monitor handle directly without writing
/// `Arc::new(ParkingLotMonitor::new(...))`. It preserves the same guard-based
/// waiting and predicate-based waiting semantics as [`ParkingLotMonitor`]. It
/// implements [`Deref`] and [`AsRef`] so callers can pass it to APIs that
/// expect a [`ParkingLotMonitor`] reference.
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
/// use qubit_lock::ArcParkingLotMonitor;
///
/// let monitor = ArcParkingLotMonitor::new(false);
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
/// monitor.with_write(|ready| {
///     *ready = true;
/// });
/// monitor.notify_all();
///
/// waiter.join().expect("waiter should finish");
/// assert!(!monitor.with_read(|ready| *ready));
/// ```
pub struct ArcParkingLotMonitor<T> {
    /// Shared monitor instance.
    inner: Arc<ParkingLotMonitor<T>>,
}

impl<T> ArcParkingLotMonitor<T> {
    /// Creates an Arc-wrapped monitor protecting the supplied state value.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle initialized with the supplied state.
    #[inline]
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(ParkingLotMonitor::new(state)),
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
    pub fn with_timer(state: T, timer: Arc<dyn Timer>) -> Self {
        Self {
            inner: Arc::new(ParkingLotMonitor::with_timer(state, timer)),
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
    pub fn from_arc(inner: Arc<ParkingLotMonitor<T>>) -> Self {
        Self { inner }
    }

    /// Borrows the Arc that owns the wrapped monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc without changing its strong reference count.
    #[inline(always)]
    pub fn as_arc(&self) -> &Arc<ParkingLotMonitor<T>> {
        &self.inner
    }

    /// Consumes this handle and returns the Arc that owns the monitor.
    ///
    /// # Returns
    ///
    /// The existing Arc, preserving the wrapped monitor allocation.
    #[inline(always)]
    pub fn into_arc(self) -> Arc<ParkingLotMonitor<T>> {
        self.inner
    }
}

impl<T> AsRef<ParkingLotMonitor<T>> for ArcParkingLotMonitor<T> {
    /// Returns a reference to the underlying monitor.
    ///
    /// This is useful when callers need an explicit [`ParkingLotMonitor`]
    /// reference while keeping the cloneable [`ArcParkingLotMonitor`]
    /// handle.
    #[inline(always)]
    fn as_ref(&self) -> &ParkingLotMonitor<T> {
        self.inner.as_ref()
    }
}

impl<T> Notifier for ArcParkingLotMonitor<T> {
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

impl<T> ConditionWaiter for ArcParkingLotMonitor<T> {
    type State = T;

    /// Blocks while the predicate remains true, then runs the action.
    #[inline(always)]
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        <ParkingLotMonitor<T> as ConditionWaiter>::wait_while(
            self.inner.as_ref(),
            predicate,
            action,
        )
    }
}

impl<T> TimeoutConditionWaiter for ArcParkingLotMonitor<T> {
    /// Blocks while the predicate remains true or until the timeout expires.
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
        <ParkingLotMonitor<T> as TimeoutConditionWaiter>::wait_while_for(
            self.inner.as_ref(),
            timeout,
            predicate,
            action,
        )
    }
}

impl<T> Deref for ArcParkingLotMonitor<T> {
    type Target = ParkingLotMonitor<T>;

    /// Dereferences this wrapper to the underlying monitor.
    ///
    /// Method-call dereferencing lets callers use native [`ParkingLotMonitor`]
    /// APIs directly, while this wrapper still provides cloneable
    /// ownership.
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> From<T> for ArcParkingLotMonitor<T> {
    /// Creates an Arc-wrapped monitor from an initial state value.
    ///
    /// # Parameters
    ///
    /// * `value` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle protecting `value`.
    #[inline(always)]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ArcParkingLotMonitor<T> {
    /// Creates an Arc-wrapped monitor containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle protecting the default value for `T`.
    #[inline(always)]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Clone for ArcParkingLotMonitor<T> {
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
