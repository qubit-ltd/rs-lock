/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Arc StdMonitor
//!
//! Provides an Arc-wrapped synchronous monitor for condition-based state
//! coordination across threads.
//!

use std::sync::Arc;
use std::time::Duration;

use super::{
    StdMonitor,
    StdMonitorGuard,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

/// Arc-wrapped monitor for shared condition-based state coordination.
///
/// `ArcStdMonitor` stores a [`StdMonitor`] behind an [`Arc`], so callers can clone
/// the monitor handle directly without writing `Arc::new(StdMonitor::new(...))`.
/// It preserves the same guard-based waiting, predicate-based waiting, and
/// poison recovery semantics as [`StdMonitor`].
///
/// # Type Parameters
///
/// * `T` - The state protected by this monitor.
///
/// # Example
///
/// ```rust
/// use std::thread;
///
/// use qubit_lock::lock::ArcStdMonitor;
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
/// monitor.write(|ready| {
///     *ready = true;
/// });
/// monitor.notify_all();
///
/// waiter.join().expect("waiter should finish");
/// assert!(!monitor.read(|ready| *ready));
/// ```
///
pub struct ArcStdMonitor<T> {
    /// Shared monitor instance.
    inner: Arc<StdMonitor<T>>,
}

impl<T> ArcStdMonitor<T> {
    /// Creates an Arc-wrapped monitor protecting the supplied state value.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle initialized with the supplied state.
    #[inline]
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(StdMonitor::new(state)),
        }
    }

    /// Acquires the shared monitor and returns a guard.
    ///
    /// This delegates to [`StdMonitor::lock`]. The returned [`StdMonitorGuard`]
    /// keeps the monitor mutex locked until it is dropped. It can also wait on
    /// the monitor's condition variable through [`StdMonitorGuard::wait`] or
    /// [`StdMonitorGuard::wait_timeout`].
    ///
    /// If the underlying mutex is poisoned, this method recovers the inner
    /// state and still returns a guard.
    ///
    /// # Returns
    ///
    /// A guard that provides read and write access to the protected state.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::ArcStdMonitor;
    ///
    /// let monitor = ArcStdMonitor::new(1);
    /// {
    ///     let mut value = monitor.lock();
    ///     *value += 1;
    /// }
    ///
    /// assert_eq!(monitor.read(|value| *value), 2);
    /// ```
    #[inline]
    pub fn lock(&self) -> StdMonitorGuard<'_, T> {
        self.inner.lock()
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// This delegates to [`StdMonitor::read`]. The closure runs while the monitor
    /// mutex is held, so keep it short and avoid long blocking work.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives an immutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    #[inline]
    pub fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.inner.read(f)
    }

    /// Acquires the monitor and mutates the protected state.
    ///
    /// This delegates to [`StdMonitor::write`]. Callers should explicitly invoke
    /// [`Self::notify_one`] or [`Self::notify_all`] after changing state that a
    /// waiting thread may observe.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    #[inline]
    pub fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.write(f)
    }

    /// Waits for a notification or timeout without checking state.
    ///
    /// This delegates to [`StdMonitor::wait_notify`]. Most
    /// coordination code should prefer [`Self::wait_while`],
    /// [`Self::wait_until`], or an explicit [`StdMonitorGuard`] loop.
    ///
    /// Condition variables may wake spuriously, so
    /// [`WaitTimeoutStatus::Woken`] does not prove that a notifier changed the
    /// state.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum duration to wait for a notification.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutStatus::Woken`] if the wait returned before the timeout,
    /// or [`WaitTimeoutStatus::TimedOut`] if the timeout elapsed.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use qubit_lock::lock::{ArcStdMonitor, WaitTimeoutStatus};
    ///
    /// let monitor = ArcStdMonitor::new(false);
    /// let status = monitor.wait_notify(Duration::from_millis(1));
    ///
    /// assert_eq!(status, WaitTimeoutStatus::TimedOut);
    /// ```
    #[inline]
    pub fn wait_notify(&self, timeout: Duration) -> WaitTimeoutStatus {
        self.inner.wait_notify(timeout)
    }

    /// Waits while a predicate remains true, then mutates the protected state.
    ///
    /// This delegates to [`StdMonitor::wait_while`]. The predicate is evaluated
    /// while holding the monitor mutex, and the closure runs while the mutex is
    /// still held after the predicate stops blocking.
    ///
    /// This method may block indefinitely if no thread changes the state so
    /// that `waiting` becomes false and sends a notification.
    ///
    /// # Arguments
    ///
    /// * `waiting` - Predicate that returns `true` while the caller should
    ///   keep waiting.
    /// * `f` - Closure that receives mutable access after waiting is no longer
    ///   required.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::thread;
    ///
    /// use qubit_lock::lock::ArcStdMonitor;
    ///
    /// let monitor = ArcStdMonitor::new(Vec::<i32>::new());
    /// let worker_monitor = monitor.clone();
    ///
    /// let worker = thread::spawn(move || {
    ///     worker_monitor.wait_while(
    ///         |items| items.is_empty(),
    ///         |items| items.pop().expect("item should be ready"),
    ///     )
    /// });
    ///
    /// monitor.write(|items| items.push(7));
    /// monitor.notify_one();
    ///
    /// assert_eq!(worker.join().expect("worker should finish"), 7);
    /// ```
    #[inline]
    pub fn wait_while<R, P, F>(&self, waiting: P, f: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.inner.wait_while(waiting, f)
    }

    /// Waits until the protected state satisfies a predicate, then mutates it.
    ///
    /// This delegates to [`StdMonitor::wait_until`]. It may block indefinitely if
    /// no thread changes the state to satisfy the predicate and sends a
    /// notification.
    ///
    /// # Arguments
    ///
    /// * `ready` - Predicate that returns `true` when the state is ready.
    /// * `f` - Closure that receives mutable access to the ready state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    #[inline]
    pub fn wait_until<R, P, F>(&self, ready: P, f: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.inner.wait_until(ready, f)
    }

    /// Waits while a predicate remains true, with an overall time limit.
    ///
    /// This delegates to [`StdMonitor::wait_timeout_while`]. If `waiting` becomes
    /// false before `timeout` expires, `f` runs while the monitor lock is still
    /// held. If the timeout expires first, the closure is not called.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum total duration to wait.
    /// * `waiting` - Predicate that returns `true` while the caller should
    ///   continue waiting.
    /// * `f` - Closure that receives mutable access when waiting is no longer
    ///   required.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f` when the
    /// predicate stops blocking before the timeout. Returns
    /// [`WaitTimeoutResult::TimedOut`] when the timeout expires first.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use qubit_lock::lock::{ArcStdMonitor, WaitTimeoutResult};
    ///
    /// let monitor = ArcStdMonitor::new(Vec::<i32>::new());
    /// let result = monitor.wait_timeout_while(
    ///     Duration::from_millis(1),
    ///     |items| items.is_empty(),
    ///     |items| items.pop(),
    /// );
    ///
    /// assert_eq!(result, WaitTimeoutResult::TimedOut);
    /// ```
    #[inline]
    pub fn wait_timeout_while<R, P, F>(
        &self,
        timeout: Duration,
        waiting: P,
        f: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.inner.wait_timeout_while(timeout, waiting, f)
    }

    /// Waits until a predicate becomes true, with an overall time limit.
    ///
    /// This delegates to [`StdMonitor::wait_timeout_until`]. If `ready` becomes
    /// true before `timeout` expires, `f` runs while the monitor lock is still
    /// held. If the timeout expires first, the closure is not called.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum total duration to wait.
    /// * `ready` - Predicate that returns `true` when the caller may continue.
    /// * `f` - Closure that receives mutable access to the ready state.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f` when the
    /// predicate becomes true before the timeout. Returns
    /// [`WaitTimeoutResult::TimedOut`] when the timeout expires first.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::{
    ///     thread,
    ///     time::Duration,
    /// };
    ///
    /// use qubit_lock::lock::{ArcStdMonitor, WaitTimeoutResult};
    ///
    /// let monitor = ArcStdMonitor::new(false);
    /// let worker_monitor = monitor.clone();
    ///
    /// let worker = thread::spawn(move || {
    ///     worker_monitor.wait_timeout_until(
    ///         Duration::from_secs(1),
    ///         |ready| *ready,
    ///         |ready| {
    ///             *ready = false;
    ///             5
    ///         },
    ///     )
    /// });
    ///
    /// monitor.write(|ready| *ready = true);
    /// monitor.notify_one();
    ///
    /// assert_eq!(
    ///     worker.join().expect("worker should finish"),
    ///     WaitTimeoutResult::Ready(5),
    /// );
    /// ```
    #[inline]
    pub fn wait_timeout_until<R, P, F>(
        &self,
        timeout: Duration,
        ready: P,
        f: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.inner.wait_timeout_until(timeout, ready, f)
    }

    /// Wakes one thread waiting on this monitor's condition variable.
    ///
    /// Notifications do not carry state by themselves. A waiting thread only
    /// proceeds safely after rechecking the protected state. Call this after
    /// changing state that may make one waiter able to continue.
    #[inline]
    pub fn notify_one(&self) {
        self.inner.notify_one();
    }

    /// Wakes all threads waiting on this monitor's condition variable.
    ///
    /// Notifications do not carry state by themselves. Every awakened thread
    /// must recheck the protected state before continuing. Call this after a
    /// state change that may allow multiple waiters to make progress.
    #[inline]
    pub fn notify_all(&self) {
        self.inner.notify_all();
    }
}

impl<T: Default> Default for ArcStdMonitor<T> {
    /// Creates an Arc-wrapped monitor containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle protecting the default value for `T`.
    #[inline]
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
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
