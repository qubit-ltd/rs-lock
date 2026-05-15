/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Monitor
//!
//! Provides a synchronous monitor built from a mutex and a condition variable.
//! A monitor protects one shared state value and binds that state to the
//! condition variable used to wait for changes. This is the same low-level
//! mechanism as using [`parking_lot::Mutex`] and [`parking_lot::Condvar`] directly,
//! but packaged so callers do not have to keep a mutex and its matching
//! condition variable as separate fields.
//!
//! The high-level APIs ([`Monitor::read`], [`Monitor::write`],
//! [`Monitor::wait_while`], and [`Monitor::wait_until`]) are intended for
//! short critical sections and simple guarded-suspension flows. The lower-level
//! [`Monitor::lock`] API returns a [`MonitorGuard`], which supports
//! [`MonitorGuard::wait`] and [`MonitorGuard::wait_timeout`] for more complex
//! state machines such as thread pools.
//!

use std::time::{
    Duration,
    Instant,
};

use parking_lot::{
    Condvar,
    Mutex,
};

use super::monitor_guard::MonitorGuard;
use super::{
    wait_timeout_result::WaitTimeoutResult,
    wait_timeout_status::WaitTimeoutStatus,
};

/// Shared state protected by a mutex and a condition variable.
///
/// `Monitor` is useful when callers need more than a short critical section.
/// It models the classic monitor object pattern: one mutex protects the state,
/// and one condition variable lets threads wait until that state changes. This
/// is the same relationship used by `parking_lot::Mutex` and
/// `parking_lot::Condvar`, but represented as one object so the condition
/// variable is not accidentally used with unrelated state.
///
/// `Monitor` deliberately has two levels of API:
///
/// * `read` and `write` acquire the mutex, run a closure, and release it.
/// * `wait_while`, `wait_until`, and their timeout variants implement common
///   predicate-based waits.
/// * `lock` returns a [`MonitorGuard`] for callers that need to write their own
///   loop around [`MonitorGuard::wait`] or [`MonitorGuard::wait_timeout`].
///
/// The underlying `parking_lot` mutex is not poisoned when a thread panics
/// while holding the lock. This keeps monitor coordination state observable
/// after panic unwinding.
///
/// # Difference from raw `Mutex` and `Condvar`
///
/// With raw parking_lot primitives, callers usually store two fields and
/// manually keep them paired:
///
/// ```rust
/// # use parking_lot::{Condvar, Mutex};
/// # struct State;
/// struct Shared {
///     state: Mutex<State>,
///     changed: Condvar,
/// }
/// ```
///
/// `Monitor<State>` stores the same pair internally. A [`MonitorGuard`] is a
/// wrapper around the parking_lot `MutexGuard`; it keeps the protected
/// state locked and knows which monitor it belongs to, so its wait methods use
/// the matching condition variable.
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
/// use qubit_lock::ArcMonitor;
///
/// let monitor = ArcMonitor::new(false);
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
pub struct Monitor<T> {
    /// Mutex protecting the monitor state.
    state: Mutex<T>,
    /// Condition variable used to wake predicate waiters after state changes.
    pub(super) changed: Condvar,
}

impl<T> Monitor<T> {
    /// Creates a monitor protecting the supplied state value.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A monitor initialized with the supplied state.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::Monitor;
    ///
    /// let monitor = Monitor::new(0_u32);
    /// assert_eq!(monitor.read(|n| *n), 0);
    /// ```
    #[inline]
    pub fn new(state: T) -> Self {
        Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
        }
    }

    /// Acquires the monitor and returns a guard for explicit state-machine code.
    ///
    /// The returned [`MonitorGuard`] keeps the monitor mutex locked until the
    /// guard is dropped. It can also be passed through
    /// [`MonitorGuard::wait`] or [`MonitorGuard::wait_timeout`] to temporarily
    /// release the lock while waiting on this monitor's condition variable.
    ///
    /// # Returns
    ///
    /// A guard that provides read and write access to the protected state.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::Monitor;
    ///
    /// let monitor = Monitor::new(1);
    /// {
    ///     let mut value = monitor.lock();
    ///     *value += 1;
    /// }
    ///
    /// assert_eq!(monitor.read(|value| *value), 2);
    /// ```
    #[inline]
    pub fn lock(&self) -> MonitorGuard<'_, T> {
        MonitorGuard::new(self, self.state.lock())
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// The closure runs while the mutex is held. Keep the closure short and do
    /// not call code that may block for a long time.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives an immutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::Monitor;
    ///
    /// let monitor = Monitor::new(10_i32);
    /// let n = monitor.read(|x| *x);
    /// assert_eq!(n, 10);
    /// ```
    #[inline]
    pub fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.lock();
        f(&*guard)
    }

    /// Acquires the monitor and mutates the protected state.
    ///
    /// The closure runs while the mutex is held. This method only changes the
    /// state; callers should explicitly call [`Self::notify_one`] or
    /// [`Self::notify_all`] after changing a condition that waiters may be
    /// observing.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::Monitor;
    ///
    /// let monitor = Monitor::new(String::new());
    /// let len = monitor.write(|s| {
    ///     s.push_str("hi");
    ///     s.len()
    /// });
    /// assert_eq!(len, 2);
    /// ```
    #[inline]
    pub fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.lock();
        f(&mut *guard)
    }

    /// Waits for a notification or timeout without checking state.
    ///
    /// This convenience method locks the monitor, waits once on the condition
    /// variable, and returns why the timed wait completed. It is useful only
    /// when the caller genuinely needs a notification wait without inspecting
    /// state before or after the wait. Most coordination code should prefer
    /// [`Self::wait_while`], [`Self::wait_until`], or the explicit
    /// [`MonitorGuard::wait_timeout`] loop.
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
    /// use qubit_lock::{Monitor, WaitTimeoutStatus};
    ///
    /// let monitor = Monitor::new(false);
    /// let status = monitor.wait_notify(Duration::from_millis(1));
    ///
    /// assert_eq!(status, WaitTimeoutStatus::TimedOut);
    /// ```
    #[inline]
    pub fn wait_notify(&self, timeout: Duration) -> WaitTimeoutStatus {
        let guard = self.lock();
        let (_guard, status) = guard.wait_timeout(timeout);
        status
    }

    /// Waits while a predicate remains true, then mutates the protected state.
    ///
    /// This is the monitor equivalent of the common `while condition { wait }`
    /// guarded-suspension pattern. The predicate is evaluated while holding the
    /// mutex. If it returns `true`, the current thread waits on the condition
    /// variable and atomically releases the mutex. After a notification, the
    /// mutex is reacquired and the predicate is evaluated again. When the
    /// predicate returns `false`, `f` runs while the mutex is still held.
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
    /// use std::{
    ///     sync::Arc,
    ///     thread,
    /// };
    ///
    /// use qubit_lock::Monitor;
    ///
    /// let monitor = Arc::new(Monitor::new(Vec::<i32>::new()));
    /// let worker_monitor = Arc::clone(&monitor);
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
    pub fn wait_while<R, P, F>(&self, mut waiting: P, f: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.lock();
        while waiting(&*guard) {
            guard = guard.wait();
        }
        f(&mut *guard)
    }

    /// Waits until the protected state satisfies a predicate, then mutates it.
    ///
    /// This is the positive-predicate counterpart of [`Self::wait_while`]. The
    /// predicate is evaluated while holding the mutex. If it returns `false`,
    /// the current thread waits on the condition variable and atomically
    /// releases the mutex. After a notification, the mutex is reacquired and
    /// the predicate is evaluated again. When the predicate returns `true`, `f`
    /// runs while the mutex is still held.
    ///
    /// This method may block indefinitely if no thread changes the state to
    /// satisfy the predicate and sends a notification.
    ///
    /// # Arguments
    ///
    /// * `ready` - Predicate that returns `true` when the state is ready.
    /// * `f` - Closure that receives mutable access to the ready state.
    ///
    /// # Returns
    ///
    /// The value returned by `f` after the predicate has become true.
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
    ///     waiter_monitor.wait_until(
    ///         |ready| *ready,
    ///         |ready| {
    ///             *ready = false;
    ///             "done"
    ///         },
    ///     )
    /// });
    ///
    /// monitor.write(|ready| *ready = true);
    /// monitor.notify_one();
    ///
    /// assert_eq!(waiter.join().expect("waiter should finish"), "done");
    /// ```
    #[inline]
    pub fn wait_until<R, P, F>(&self, mut ready: P, f: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.wait_while(|state| !ready(state), f)
    }

    /// Waits while a predicate remains true, with an overall time limit.
    ///
    /// This method is the timeout-aware form of [`Self::wait_while`]. It keeps
    /// rechecking `waiting` under the monitor lock and waits only for the
    /// remaining portion of `timeout`. If `waiting` becomes false before the
    /// timeout expires, `f` runs while the lock is still held. If the timeout
    /// expires first, the closure is not called.
    ///
    /// Timeout status alone is not used as proof that the predicate is still
    /// true; the predicate is always rechecked under the lock.
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
    /// use qubit_lock::{Monitor, WaitTimeoutResult};
    ///
    /// let monitor = Monitor::new(Vec::<i32>::new());
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
        mut waiting: P,
        f: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.lock();
        let start = Instant::now();
        loop {
            if !waiting(&*guard) {
                return WaitTimeoutResult::Ready(f(&mut *guard));
            }

            let elapsed = start.elapsed();
            let remaining = timeout.checked_sub(elapsed).unwrap_or_default();
            if remaining.is_zero() {
                return WaitTimeoutResult::TimedOut;
            }

            let (next_guard, _status) = guard.wait_timeout(remaining);
            guard = next_guard;
        }
    }

    /// Waits until a predicate becomes true, with an overall time limit.
    ///
    /// This is the positive-predicate counterpart of
    /// [`Self::wait_timeout_while`]. If `ready` becomes true before the timeout
    /// expires, `f` runs while the monitor lock is still held. If the timeout
    /// expires first, the closure is not called.
    ///
    /// Timeout status alone is not used as proof that the predicate is still
    /// false; the predicate is always rechecked under the lock.
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
    ///     sync::Arc,
    ///     thread,
    ///     time::Duration,
    /// };
    ///
    /// use qubit_lock::{Monitor, WaitTimeoutResult};
    ///
    /// let monitor = Arc::new(Monitor::new(false));
    /// let waiter_monitor = Arc::clone(&monitor);
    ///
    /// let waiter = thread::spawn(move || {
    ///     waiter_monitor.wait_timeout_until(
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
    ///     waiter.join().expect("waiter should finish"),
    ///     WaitTimeoutResult::Ready(5),
    /// );
    /// ```
    #[inline]
    pub fn wait_timeout_until<R, P, F>(
        &self,
        timeout: Duration,
        mut ready: P,
        f: F,
    ) -> WaitTimeoutResult<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.wait_timeout_while(timeout, |state| !ready(state), f)
    }

    /// Wakes one thread waiting on this monitor's condition variable.
    ///
    /// Notifications do not carry state by themselves. A waiting thread only
    /// proceeds safely after rechecking the protected state. Call this after
    /// changing state that may make one waiter able to continue.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::thread;
    ///
    /// use qubit_lock::ArcMonitor;
    ///
    /// let monitor = ArcMonitor::new(0_u32);
    /// let waiter = {
    ///     let m = monitor.clone();
    ///     thread::spawn(move || {
    ///         m.wait_until(|n| *n > 0, |n| {
    ///             *n -= 1;
    ///         });
    ///     })
    /// };
    ///
    /// monitor.write(|n| *n = 1);
    /// monitor.notify_one();
    /// waiter.join().expect("waiter should finish");
    /// ```
    #[inline]
    pub fn notify_one(&self) {
        self.changed.notify_one();
    }

    /// Wakes all threads waiting on this monitor's condition variable.
    ///
    /// Notifications do not carry state by themselves. Every awakened thread
    /// must recheck the protected state before continuing. Call this after a
    /// state change that may allow multiple waiters to make progress.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::thread;
    ///
    /// use qubit_lock::ArcMonitor;
    ///
    /// let monitor = ArcMonitor::new(false);
    /// let mut handles = Vec::new();
    /// for _ in 0..2 {
    ///     let m = monitor.clone();
    ///     handles.push(thread::spawn(move || {
    ///         m.wait_until(|ready| *ready, |_| ());
    ///     }));
    /// }
    ///
    /// monitor.write(|ready| *ready = true);
    /// monitor.notify_all();
    /// for h in handles {
    ///     h.join().expect("waiter should finish");
    /// }
    /// ```
    #[inline]
    pub fn notify_all(&self) {
        self.changed.notify_all();
    }
}

impl<T> From<T> for Monitor<T> {
    /// Creates a monitor from an initial state value.
    ///
    /// # Arguments
    ///
    /// * `value` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A monitor initialized with `value`.
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for Monitor<T> {
    /// Creates a monitor containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A monitor protecting the default value for `T`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::Monitor;
    ///
    /// let monitor: Monitor<String> = Monitor::default();
    /// assert!(monitor.read(|s| s.is_empty()));
    /// ```
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}
