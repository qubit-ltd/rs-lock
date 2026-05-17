/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Arc ParkingLotMonitor
//!
//! Provides an Arc-wrapped synchronous monitor for condition-based state
//! coordination across threads.
//!

use std::{
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use super::{
    ConditionWaiter,
    NotificationWaiter,
    Notifier,
    ParkingLotMonitor,
    ParkingLotMonitorGuard,
    TimeoutConditionWaiter,
    TimeoutNotificationWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

/// Arc-wrapped monitor for shared condition-based state coordination.
///
/// `ArcParkingLotMonitor` stores a [`ParkingLotMonitor`] behind an [`Arc`], so callers can clone
/// the monitor handle directly without writing `Arc::new(ParkingLotMonitor::new(...))`.
/// It preserves the same guard-based waiting and predicate-based waiting
/// semantics as [`ParkingLotMonitor`]. It implements [`Deref`] and [`AsRef`] so callers
/// can pass it to APIs that expect a [`ParkingLotMonitor`] reference.
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
/// monitor.write(|ready| {
///     *ready = true;
/// });
/// monitor.notify_all();
///
/// waiter.join().expect("waiter should finish");
/// assert!(!monitor.read(|ready| *ready));
/// ```
///
pub struct ArcParkingLotMonitor<T> {
    /// Shared monitor instance.
    inner: Arc<ParkingLotMonitor<T>>,
}

impl<T> ArcParkingLotMonitor<T> {
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
            inner: Arc::new(ParkingLotMonitor::new(state)),
        }
    }

    /// Acquires the shared monitor and returns a guard.
    ///
    /// This delegates to [`ParkingLotMonitor::lock`]. The returned [`ParkingLotMonitorGuard`]
    /// keeps the monitor mutex locked until it is dropped. It can also wait on
    /// the monitor's condition variable through [`ParkingLotMonitorGuard::wait`] or
    /// [`ParkingLotMonitorGuard::wait_timeout`].
    ///
    /// # Returns
    ///
    /// A guard that provides read and write access to the protected state.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::ArcParkingLotMonitor;
    ///
    /// let monitor = ArcParkingLotMonitor::new(1);
    /// {
    ///     let mut value = monitor.lock();
    ///     *value += 1;
    /// }
    ///
    /// assert_eq!(monitor.read(|value| *value), 2);
    /// ```
    #[inline]
    pub fn lock(&self) -> ParkingLotMonitorGuard<'_, T> {
        self.inner.lock()
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// This delegates to [`ParkingLotMonitor::read`]. The closure runs while the monitor
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
    /// This delegates to [`ParkingLotMonitor::write`]. Callers should explicitly invoke
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

    /// Mutates the protected state and wakes one waiter.
    ///
    /// This delegates to [`ParkingLotMonitor::write_notify_one`]. The closure runs while
    /// the monitor mutex is held; after it returns, the lock is released and one
    /// waiter is notified. If `f` panics, the panic is propagated and no
    /// notification is sent.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    #[inline]
    pub fn write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.write_notify_one(f)
    }

    /// Mutates the protected state and wakes all waiters.
    ///
    /// This delegates to [`ParkingLotMonitor::write_notify_all`]. The closure runs while
    /// the monitor mutex is held; after it returns, the lock is released and all
    /// waiters are notified. If `f` panics, the panic is propagated and no
    /// notification is sent.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    #[inline]
    pub fn write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.write_notify_all(f)
    }

    /// Waits for a notification without checking state.
    ///
    /// This delegates to [`ParkingLotMonitor::wait`].
    #[inline]
    pub fn wait(&self) {
        self.inner.wait();
    }

    /// Waits for a notification or timeout without checking state.
    ///
    /// This delegates to [`ParkingLotMonitor::wait_for`]. Most
    /// coordination code should prefer [`Self::wait_while`],
    /// [`Self::wait_until`], or an explicit [`ParkingLotMonitorGuard`] loop.
    ///
    /// [`WaitTimeoutStatus::Woken`] means the condition variable was notified,
    /// but it does not prove that the protected state changed in a useful way.
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
    /// use qubit_lock::{ArcParkingLotMonitor, WaitTimeoutStatus};
    ///
    /// let monitor = ArcParkingLotMonitor::new(false);
    /// let status = monitor.wait_for(Duration::from_millis(1));
    ///
    /// assert_eq!(status, WaitTimeoutStatus::TimedOut);
    /// ```
    #[inline]
    pub fn wait_for(&self, timeout: Duration) -> WaitTimeoutStatus {
        self.inner.wait_for(timeout)
    }

    /// Waits while a predicate remains true, then mutates the protected state.
    ///
    /// This delegates to [`ParkingLotMonitor::wait_while`]. The predicate is evaluated
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
    /// use qubit_lock::ArcParkingLotMonitor;
    ///
    /// let monitor = ArcParkingLotMonitor::new(Vec::<i32>::new());
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
    /// This delegates to [`ParkingLotMonitor::wait_until`]. It may block indefinitely if
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
    /// This delegates to [`ParkingLotMonitor::wait_while_for`]. If `waiting` becomes
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
    /// use qubit_lock::{ArcParkingLotMonitor, WaitTimeoutResult};
    ///
    /// let monitor = ArcParkingLotMonitor::new(Vec::<i32>::new());
    /// let result = monitor.wait_while_for(
    ///     Duration::from_millis(1),
    ///     |items| items.is_empty(),
    ///     |items| items.pop(),
    /// );
    ///
    /// assert_eq!(result, WaitTimeoutResult::TimedOut);
    /// ```
    #[inline]
    pub fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        waiting: P,
        f: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.inner.wait_while_for(timeout, waiting, f)
    }

    /// Waits until a predicate becomes true, with an overall time limit.
    ///
    /// This delegates to [`ParkingLotMonitor::wait_until_for`]. If `ready` becomes
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
    /// use qubit_lock::{ArcParkingLotMonitor, WaitTimeoutResult};
    ///
    /// let monitor = ArcParkingLotMonitor::new(false);
    /// let worker_monitor = monitor.clone();
    ///
    /// let worker = thread::spawn(move || {
    ///     worker_monitor.wait_until_for(
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
    pub fn wait_until_for<R, P, F>(&self, timeout: Duration, ready: P, f: F) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.inner.wait_until_for(timeout, ready, f)
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

impl<T> AsRef<ParkingLotMonitor<T>> for ArcParkingLotMonitor<T> {
    /// Returns a reference to the underlying monitor.
    ///
    /// This is useful when callers need an explicit [`ParkingLotMonitor`] reference while
    /// keeping the cloneable [`ArcParkingLotMonitor`] handle.
    #[inline]
    fn as_ref(&self) -> &ParkingLotMonitor<T> {
        self.inner.as_ref()
    }
}

impl<T> Notifier for ArcParkingLotMonitor<T> {
    /// Wakes one thread waiting on this monitor.
    #[inline]
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all threads waiting on this monitor.
    #[inline]
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T> NotificationWaiter for ArcParkingLotMonitor<T> {
    /// Blocks until a notification wakes this waiter.
    #[inline]
    fn wait(&self) {
        Self::wait(self);
    }
}

impl<T> TimeoutNotificationWaiter for ArcParkingLotMonitor<T> {
    /// Blocks until a notification wakes this waiter or the timeout expires.
    #[inline]
    fn wait_for(&self, timeout: Duration) -> WaitTimeoutStatus {
        Self::wait_for(self, timeout)
    }
}

impl<T> ConditionWaiter for ArcParkingLotMonitor<T> {
    type State = T;

    /// Blocks until the predicate becomes true, then runs the action.
    #[inline]
    fn wait_until<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        Self::wait_until(self, predicate, action)
    }

    /// Blocks while the predicate remains true, then runs the action.
    #[inline]
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        Self::wait_while(self, predicate, action)
    }
}

impl<T> TimeoutConditionWaiter for ArcParkingLotMonitor<T> {
    /// Blocks until the predicate becomes true or the timeout expires.
    #[inline]
    fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        Self::wait_until_for(self, timeout, predicate, action)
    }

    /// Blocks while the predicate remains true or until the timeout expires.
    #[inline]
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
        Self::wait_while_for(self, timeout, predicate, action)
    }
}

impl<T> Deref for ArcParkingLotMonitor<T> {
    type Target = ParkingLotMonitor<T>;

    /// Dereferences this wrapper to the underlying monitor.
    ///
    /// Method-call dereferencing lets callers use native [`ParkingLotMonitor`] APIs
    /// directly, while this wrapper still provides cloneable ownership.
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl<T> From<T> for ArcParkingLotMonitor<T> {
    /// Creates an Arc-wrapped monitor from an initial state value.
    ///
    /// # Arguments
    ///
    /// * `value` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A cloneable monitor handle protecting `value`.
    #[inline]
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
    #[inline]
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
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
