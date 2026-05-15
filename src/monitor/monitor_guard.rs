/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Monitor Guard
//!
//! Provides the guard returned by [`Monitor::lock`](super::Monitor::lock).
//! The guard wraps a parking_lot mutex guard and keeps a reference to the
//! monitor that created it, so waiting operations can use the matching
//! condition variable.
//!

use std::{
    ops::{
        Deref,
        DerefMut,
    },
    time::Duration,
};

use parking_lot::MutexGuard;

use super::{
    monitor::Monitor,
    wait_timeout_status::WaitTimeoutStatus,
};

/// Guard returned by [`Monitor::lock`](super::Monitor::lock).
///
/// `MonitorGuard` is the monitor-specific counterpart of
/// [`parking_lot::MutexGuard`]. While it exists, the protected state is locked.
/// Dropping the guard releases the lock. It implements [`Deref`] and
/// [`DerefMut`], so callers can read and mutate the protected state as if they
/// held `&T` or `&mut T`.
///
/// Unlike a raw `MutexGuard`, this guard also remembers the monitor that
/// created it. That lets [`Self::wait`] and [`Self::wait_timeout`] release and
/// reacquire the correct mutex with the correct condition variable.
///
/// # Type Parameters
///
/// * `T` - The state protected by the monitor.
///
/// # Example
///
/// ```rust
/// use qubit_lock::Monitor;
///
/// let monitor = Monitor::new(Vec::new());
/// {
///     let mut items = monitor.lock();
///     items.push("first");
/// }
///
/// assert_eq!(monitor.read(|items| items.len()), 1);
/// ```
pub struct MonitorGuard<'a, T> {
    /// Monitor that owns the mutex and condition variable.
    monitor: &'a Monitor<T>,
    /// Parking-lot mutex guard protecting the monitor state.
    inner: MutexGuard<'a, T>,
}

impl<'a, T> MonitorGuard<'a, T> {
    /// Creates a guard from its owning monitor and parking_lot mutex guard.
    ///
    /// # Parameters
    ///
    /// * `monitor` - Monitor whose mutex produced `inner`.
    /// * `inner` - Parking-lot mutex guard protecting the monitor state.
    ///
    /// # Returns
    ///
    /// A monitor guard that can access state and wait on the monitor's
    /// condition variable.
    #[inline]
    pub(super) fn new(monitor: &'a Monitor<T>, inner: MutexGuard<'a, T>) -> Self {
        Self { monitor, inner }
    }

    /// Waits for a notification while temporarily releasing the monitor lock.
    ///
    /// This method consumes the current guard, calls the underlying
    /// [`parking_lot::Condvar::wait`], and returns the guard after the lock has
    /// been reacquired. It is intended for explicit guarded-suspension loops
    /// where the caller needs to inspect or update state before and after
    /// waiting.
    ///
    /// The method may block indefinitely if no notification is sent. Callers
    /// should still use it inside a loop that re-checks the protected state so
    /// notifications that do not make progress are handled correctly.
    ///
    /// # Returns
    ///
    /// A new guard holding the monitor lock after the wait returns.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::{
    ///     sync::Arc,
    ///     thread,
    /// };
    ///
    /// use qubit_lock::Monitor;
    ///
    /// let monitor = Arc::new(Monitor::new(false));
    /// let waiter_monitor = Arc::clone(&monitor);
    ///
    /// let waiter = thread::spawn(move || {
    ///     let mut ready = waiter_monitor.lock();
    ///     while !*ready {
    ///         ready = ready.wait();
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
    /// assert!(!monitor.read(|ready| *ready));
    /// ```
    #[inline]
    pub fn wait(mut self) -> Self {
        self.monitor.changed.wait(&mut self.inner);
        self
    }

    /// Waits for a notification or timeout while temporarily releasing the lock.
    ///
    /// This method consumes the current guard, calls the underlying
    /// [`parking_lot::Condvar::wait_for`], and returns the guard after the
    /// lock has been reacquired. The status reports whether the wait reached
    /// the timeout boundary or returned earlier.
    ///
    /// A [`WaitTimeoutStatus::Woken`] result does not prove that another thread
    /// changed the state. A [`WaitTimeoutStatus::TimedOut`] result also does
    /// not remove the need to inspect the state, because another thread may
    /// have changed it while this thread was reacquiring the lock.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum duration to wait before returning
    ///   [`WaitTimeoutStatus::TimedOut`].
    ///
    /// # Returns
    ///
    /// A tuple containing the reacquired guard and the timed-wait status.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use qubit_lock::{Monitor, WaitTimeoutStatus};
    ///
    /// let monitor = Monitor::new(0);
    /// let guard = monitor.lock();
    /// let (guard, status) = guard.wait_timeout(Duration::from_millis(1));
    ///
    /// assert_eq!(*guard, 0);
    /// assert_eq!(status, WaitTimeoutStatus::TimedOut);
    /// ```
    #[inline]
    pub fn wait_timeout(mut self, timeout: Duration) -> (Self, WaitTimeoutStatus) {
        let timeout_result = self.monitor.changed.wait_for(&mut self.inner, timeout);
        let status = if timeout_result.timed_out() {
            WaitTimeoutStatus::TimedOut
        } else {
            WaitTimeoutStatus::Woken
        };
        (self, status)
    }
}

impl<T> Deref for MonitorGuard<'_, T> {
    type Target = T;

    /// Returns an immutable reference to the protected state.
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for MonitorGuard<'_, T> {
    /// Returns a mutable reference to the protected state.
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
