// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # StdMonitor Guard
//!
//! Provides the guard returned by
//! [`StdMonitor::lock`](super::StdMonitor::lock). The guard wraps a
//! standard-library mutex guard and keeps a reference to the monitor that
//! created it, so waiting operations can use its waiter registry and Timer.

use qubit_clock::{
    MonotonicInstant,
    TimeError,
    TimerFuture,
};
use std::{
    ops::{
        Deref,
        DerefMut,
    },
    sync::MutexGuard,
    task::Poll,
    time::Duration,
};

use super::{
    std_monitor::StdMonitor,
    wait_timeout_status::WaitTimeoutStatus,
};

/// Guard returned by [`StdMonitor::lock`](super::StdMonitor::lock).
///
/// `StdMonitorGuard` is the monitor-specific counterpart of
/// [`std::sync::MutexGuard`]. While it exists, the protected state is locked.
/// Dropping the guard releases the lock. It implements [`Deref`] and
/// [`DerefMut`], so callers can read and mutate the protected state as if they
/// held `&T` or `&mut T`.
///
/// Unlike a raw `MutexGuard`, this guard also remembers the monitor that
/// created it. That lets [`Self::wait`], [`Self::wait_for`], and
/// [`Self::wait_until`] release and reacquire the correct mutex while using the
/// monitor's notification registry and Timer.
///
/// # Type Parameters
///
/// * `T` - The state protected by the monitor.
///
/// # Examples
///
/// ```rust
/// use qubit_lock::StdMonitor;
///
/// let monitor = StdMonitor::new(Vec::new());
/// {
///     let mut items = monitor.lock();
///     items.push("first");
/// }
///
/// assert_eq!(monitor.with_read(|items| items.len()), 1);
/// ```
///
/// Ignoring a monitor guard is rejected when unused must-use values are
/// denied:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
///
/// use qubit_lock::StdMonitor;
///
/// let monitor = StdMonitor::new(0);
/// monitor.lock();
/// ```
#[must_use = "dropping the guard immediately releases the monitor lock"]
pub struct StdMonitorGuard<'a, T> {
    /// StdMonitor that owns the state, waiter registry, and Timer.
    monitor: &'a StdMonitor<T>,
    /// Standard mutex guard protecting the monitor state.
    inner: Option<MutexGuard<'a, T>>,
}

impl<'a, T> StdMonitorGuard<'a, T> {
    /// Creates a guard from its owning monitor and standard mutex guard.
    ///
    /// # Parameters
    ///
    /// * `monitor` - StdMonitor whose mutex produced `inner`.
    /// * `inner` - Standard mutex guard protecting the monitor state.
    ///
    /// # Returns
    ///
    /// A monitor guard that can access state and wait for monitor notification.
    #[inline]
    pub(super) fn new(
        monitor: &'a StdMonitor<T>,
        inner: MutexGuard<'a, T>,
    ) -> Self {
        Self {
            monitor,
            inner: Some(inner),
        }
    }

    /// Waits for a notification while temporarily releasing the monitor lock.
    ///
    /// The guard stays in place while this method registers a private waiter,
    /// releases the state lock, waits for notification, and reacquires the
    /// lock. It is intended for explicit guarded-suspension loops where the
    /// caller inspects state before and after waiting.
    ///
    /// The method may block indefinitely if no notification is sent. The wait
    /// may also wake spuriously, so callers should use it inside a loop that
    /// re-checks the protected state.
    ///
    /// If the mutex is poisoned while waiting, the poisoned state is recovered
    /// and returned in the new guard.
    ///
    /// # Returns
    ///
    /// This method returns after this guard has reacquired the monitor lock.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{
    ///     sync::Arc,
    ///     thread,
    /// };
    ///
    /// use qubit_lock::StdMonitor;
    ///
    /// let monitor = Arc::new(StdMonitor::new(false));
    /// let waiter_monitor = Arc::clone(&monitor);
    ///
    /// let waiter = thread::spawn(move || {
    ///     let mut ready = waiter_monitor.lock();
    ///     while !*ready {
    ///         ready.wait();
    ///     }
    ///     *ready = false;
    /// });
    ///
    /// {
    ///     let mut ready = monitor.lock();
    ///     *ready = true;
    /// }
    /// monitor.notify_one();
    ///
    /// waiter.join().expect("waiter should finish");
    /// assert!(!monitor.with_read(|ready| *ready));
    /// ```
    #[inline]
    pub fn wait(&mut self) {
        let registration = self.monitor.waiters.register();
        let inner = self
            .inner
            .take()
            .expect("standard monitor guard slot must be occupied");
        drop(inner);
        registration.waiter().wait();
        drop(registration);
        self.inner = Some(
            self.monitor
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }

    /// Waits for a notification or timeout while temporarily releasing the
    /// lock.
    ///
    /// The guard stays in place while this method registers a private waiter,
    /// releases the state lock, and races notification against the injected
    /// Timer. It reacquires the state lock before returning.
    ///
    /// A [`WaitTimeoutStatus::Woken`] result does not prove that another thread
    /// changed the state. A
    /// [`WaitTimeoutStatus::TimedOut`] result also does not remove the need to
    /// inspect the state, because another thread may have changed it while this
    /// thread was reacquiring the lock.
    ///
    /// If the mutex is poisoned while waiting, the poisoned state is recovered
    /// and returned in the new guard.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum duration to wait before returning
    ///   [`WaitTimeoutStatus::TimedOut`].
    ///
    /// # Returns
    ///
    /// The timed-wait status after this guard has reacquired the lock.
    ///
    /// # Errors
    ///
    /// Returns Timer registration errors without releasing this guard.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use qubit_lock::{StdMonitor, WaitTimeoutStatus};
    ///
    /// let monitor = StdMonitor::new(0);
    /// let mut guard = monitor.lock();
    /// let status = guard
    ///     .wait_for(Duration::from_millis(1))
    ///     .expect("standard Timer should register");
    ///
    /// assert_eq!(*guard, 0);
    /// assert_eq!(status, WaitTimeoutStatus::TimedOut);
    /// ```
    #[inline]
    pub fn wait_for(
        &mut self,
        timeout: Duration,
    ) -> Result<WaitTimeoutStatus, TimeError> {
        let mut future = self.monitor.timer().after(timeout)?;
        self.wait_with_timer(&mut future)
    }

    /// Waits for a notification or an absolute Timer deadline.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in this monitor's Timer domain.
    ///
    /// # Returns
    ///
    /// Whether notification or the deadline completed the wait.
    ///
    /// # Errors
    ///
    /// Returns Timer registration errors without releasing this guard.
    #[inline(always)]
    pub fn wait_until(
        &mut self,
        deadline: MonotonicInstant,
    ) -> Result<WaitTimeoutStatus, TimeError> {
        let mut future = self.monitor.timer().at(deadline)?;
        self.wait_with_timer(&mut future)
    }

    /// Releases and reacquires the state guard around one fixed TimerFuture.
    pub(super) fn wait_with_timer(
        &mut self,
        future: &mut TimerFuture,
    ) -> Result<WaitTimeoutStatus, TimeError> {
        let registration = self.monitor.waiters.register();
        let waiter = std::sync::Arc::clone(registration.waiter());
        if let Poll::Ready(result) =
            super::internal::BlockingConditionWaiter::poll_timer(
                &waiter, future,
            )
        {
            return result.map(|()| WaitTimeoutStatus::TimedOut);
        }
        let inner = self
            .inner
            .take()
            .expect("standard monitor guard slot must be occupied");
        drop(inner);
        waiter.wait();
        drop(registration);
        self.inner = Some(
            self.monitor
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        match super::internal::BlockingConditionWaiter::poll_timer(
            &waiter, future,
        ) {
            Poll::Ready(result) => result.map(|()| WaitTimeoutStatus::TimedOut),
            Poll::Pending => Ok(WaitTimeoutStatus::Woken),
        }
    }
}

impl<T> Deref for StdMonitorGuard<'_, T> {
    type Target = T;

    /// Returns an immutable reference to the protected state.
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.inner
            .as_deref()
            .expect("standard monitor guard slot must be occupied")
    }
}

impl<T> DerefMut for StdMonitorGuard<'_, T> {
    /// Returns a mutable reference to the protected state.
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_deref_mut()
            .expect("standard monitor guard slot must be occupied")
    }
}
