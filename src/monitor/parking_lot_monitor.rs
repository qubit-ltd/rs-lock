// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # ParkingLotMonitor
//!
//! Provides a synchronous monitor built from a mutex, an explicit waiter
//! registry, and an injected [`Timer`]. A monitor protects one shared state
//! value, offers memoryless notification, and lets tests drive timed waits
//! deterministically through Timer IOC.
//!
//! The high-level APIs ([`ParkingLotMonitor::with_read`],
//! [`ParkingLotMonitor::with_write`], [`ParkingLotMonitor::wait_while`], and
//! [`ParkingLotMonitor::wait_until`]) are intended for short critical sections
//! and simple guarded-suspension flows. The lower-level
//! [`ParkingLotMonitor::lock`] API returns a [`ParkingLotMonitorGuard`], which
//! supports [`ParkingLotMonitorGuard::wait`],
//! [`ParkingLotMonitorGuard::wait_for`], and
//! [`ParkingLotMonitorGuard::wait_until`] for more complex state machines such
//! as thread pools.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;

use super::ConditionWaiter;
use super::Monitor;
use super::Notifier;
use super::TimeoutConditionWaiter;
use super::internal::BlockingWaiterRegistry;
use super::internal::default_timer;
use super::internal::wait_while_for as wait_while_for_internal;
use super::internal::wait_while_locked;
use super::internal::wait_while_with_deadline as wait_while_with_deadline_internal;
use super::parking_lot_monitor_guard::ParkingLotMonitorGuard;
use super::wait_timeout_result::WaitTimeoutResult;

/// Shared state protected by a mutex with notification and Timer-driven waits.
///
/// `ParkingLotMonitor` is useful when callers need more than a short critical
/// section. It models the classic monitor object pattern: one mutex protects
/// the state, while registered waiters receive memoryless notifications. Timed
/// waits use one fixed future from the injected [`Timer`], so production and
/// test code execute the same monitor algorithm.
///
/// `ParkingLotMonitor` deliberately has two levels of API:
///
/// * `with_read` and `with_write` acquire the mutex, run a closure, and release
///   it.
/// * `wait_while`, `wait_until`, and their timeout variants implement common
///   predicate-based waits.
/// * `lock` returns a [`ParkingLotMonitorGuard`] for callers that need to write
///   their own loop around [`ParkingLotMonitorGuard::wait`] or
///   [`ParkingLotMonitorGuard::wait_for`] or
///   [`ParkingLotMonitorGuard::wait_until`].
///
/// The underlying `parking_lot` mutex is not poisoned when a thread panics
/// while holding the lock. This keeps monitor coordination state observable
/// after panic unwinding.
///
/// Closures and predicates execute while the state mutex is held. They must
/// not re-enter the same monitor; the mutex is not reentrant and doing so can
/// deadlock.
///
/// # Difference from raw synchronization primitives
///
/// With raw parking_lot primitives, callers usually store multiple fields and
/// manually keep their notification and timeout semantics aligned:
///
/// ```rust
/// # use parking_lot::Mutex;
/// # struct State;
/// struct Shared {
///     state: Mutex<State>,
/// }
/// ```
///
/// `ParkingLotMonitor<State>` supplies notification registration and
/// Timer-driven deadlines as part of the same object. A
/// [`ParkingLotMonitorGuard`] keeps the protected state locked and knows which
/// monitor must release and reacquire it around a wait.
///
/// # Type Parameters
///
/// * `T` - The state protected by this monitor.
///
/// # Examples
///
/// ```rust
/// use std::{sync::Arc, thread};
///
/// use qubit_lock::ParkingLotMonitor;
///
/// let monitor = Arc::new(ParkingLotMonitor::new(false));
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
#[must_use = "retain and use the monitor to coordinate protected state"]
pub struct ParkingLotMonitor<T> {
    /// Mutex protecting the monitor state.
    pub(super) state: Mutex<T>,
    /// Active blocking waiters eligible for memoryless notification.
    pub(super) waiters: BlockingWaiterRegistry,
    /// Timer driving every deadline wait.
    timer: Arc<dyn Timer>,
}

impl<T> ParkingLotMonitor<T> {
    /// Creates a monitor protecting the supplied state value.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A monitor initialized with the supplied state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = ParkingLotMonitor::new(0_u32);
    /// assert_eq!(monitor.with_read(|n| *n), 0);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if all process-wide clock-domain identifiers are exhausted.
    #[inline]
    pub fn new(state: T) -> Self {
        Self::with_timer(state, default_timer())
    }

    /// Creates a monitor using an injected Timer.
    ///
    /// # Parameters
    ///
    /// * `state` - Initial state protected by the monitor.
    /// * `timer` - Timer driving all relative and absolute deadlines.
    ///
    /// # Returns
    ///
    /// A monitor bound to the supplied Timer domain.
    #[inline]
    pub fn with_timer(state: T, timer: Arc<dyn Timer>) -> Self {
        Self {
            state: Mutex::new(state),
            waiters: BlockingWaiterRegistry::new(),
            timer,
        }
    }

    /// Returns the Timer driving this monitor's deadline waits.
    ///
    /// # Returns
    ///
    /// The injected Timer and its monotonic clock domain.
    #[must_use]
    #[inline(always)]
    pub fn timer(&self) -> &dyn Timer {
        self.timer.as_ref()
    }

    /// Acquires the monitor and returns a guard for explicit state-machine
    /// code.
    ///
    /// The returned [`ParkingLotMonitorGuard`] keeps the monitor mutex locked
    /// until the guard is dropped. It can also be passed through
    /// [`ParkingLotMonitorGuard::wait`], [`ParkingLotMonitorGuard::wait_for`],
    /// or [`ParkingLotMonitorGuard::wait_until`] temporarily releases the lock
    /// while waiting on this monitor.
    ///
    /// # Returns
    ///
    /// A guard that provides read and write access to the protected state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = ParkingLotMonitor::new(1);
    /// {
    ///     let mut value = monitor.lock();
    ///     *value += 1;
    /// }
    ///
    /// assert_eq!(monitor.with_read(|value| *value), 2);
    /// ```
    #[inline]
    pub fn lock(&self) -> ParkingLotMonitorGuard<'_, T> {
        ParkingLotMonitorGuard::new(self, self.state.lock())
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// The closure runs while the mutex is held. Keep the closure short and do
    /// not call code that may block for a long time.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to read the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives an immutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = ParkingLotMonitor::new(10_i32);
    /// let n = monitor.with_read(|x| *x);
    /// assert_eq!(n, 10);
    /// ```
    #[inline]
    pub fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.lock();
        f(&*guard)
    }

    /// Acquires the monitor and mutates the protected state.
    ///
    /// The closure runs while the mutex is held. This method only changes the
    /// state. Prefer [`Self::with_write_notify_one`] or
    /// [`Self::with_write_notify_all`] when the state change may let waiters
    /// proceed. Use this lower-level helper when no notification is needed or
    /// notification is controlled separately.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to mutate the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = ParkingLotMonitor::new(String::new());
    /// let len = monitor.with_write(|s| {
    ///     s.push_str("hi");
    ///     s.len()
    /// });
    /// assert_eq!(len, 2);
    /// ```
    #[inline]
    pub fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.lock();
        f(&mut *guard)
    }

    /// Acquires the monitor, mutates the protected state, and wakes one waiter.
    ///
    /// The closure runs while the mutex is held. After the closure returns, the
    /// mutex guard is dropped and one registered condition waiter is notified.
    /// This is a convenience method for the
    /// common "update state, then notify one waiter" pattern.
    ///
    /// If `f` panics, the panic is propagated and no notification is sent.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to mutate the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`. In that case no notification is sent.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = ParkingLotMonitor::new(Vec::<i32>::new());
    /// let len = monitor.with_write_notify_one(|items| {
    ///     items.push(7);
    ///     items.len()
    /// });
    ///
    /// assert_eq!(len, 1);
    /// ```
    #[inline]
    pub fn with_write_notify_one<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.lock();
        let result = f(&mut guard);
        guard.notify_one();
        result
    }

    /// Acquires the monitor, mutates the protected state, and wakes all
    /// waiters.
    ///
    /// The closure runs while the mutex is held. After the closure returns, the
    /// mutex guard is dropped and all registered condition waiters are
    /// notified. This is a convenience method for
    /// state changes that may allow multiple waiters to make progress.
    ///
    /// If `f` panics, the panic is propagated and no notification is sent.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `F` - Closure used to mutate the protected state.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `f`. In that case no notification is sent.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = ParkingLotMonitor::new(false);
    /// let ready = monitor.with_write_notify_all(|ready| {
    ///     *ready = true;
    ///     *ready
    /// });
    ///
    /// assert!(ready);
    /// ```
    #[inline]
    pub fn with_write_notify_all<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.lock();
        let result = f(&mut guard);
        guard.notify_all();
        result
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
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `waiting` - Predicate that returns `true` while the caller should keep
    ///   waiting.
    /// * `f` - Closure that receives mutable access after waiting is no longer
    ///   required.
    ///
    /// # Returns
    ///
    /// The value returned by `f`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `waiting` or `f`. Panics if the registry
    /// exhausts registration identifiers.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{
    ///     sync::Arc,
    ///     thread,
    /// };
    ///
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = Arc::new(ParkingLotMonitor::new(Vec::<i32>::new()));
    /// let worker_monitor = Arc::clone(&monitor);
    ///
    /// let worker = thread::spawn(move || {
    ///     worker_monitor.wait_while(
    ///         |items| items.is_empty(),
    ///         |items| items.pop().expect("item should be ready"),
    ///     )
    /// });
    ///
    /// monitor.with_write_notify_one(|items| items.push(7));
    ///
    /// assert_eq!(worker.join().expect("worker should finish"), 7);
    /// ```
    pub fn wait_while<R, P, F>(&self, waiting: P, f: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        wait_while_locked(self.lock(), waiting, f, |guard| guard.wait())
    }

    /// Waits until the protected state satisfies a predicate, then mutates it.
    ///
    /// This is the positive-predicate counterpart of [`Self::wait_while`]. The
    /// predicate is evaluated while holding the mutex. If it returns `false`,
    /// the current thread registers as a condition waiter and atomically
    /// releases the mutex. After a notification, the mutex is reacquired and
    /// the predicate is evaluated again. When the predicate returns `true`, `f`
    /// runs while the mutex is still held.
    ///
    /// This method may block indefinitely if no thread changes the state to
    /// satisfy the predicate and sends a notification.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `ready` - Predicate that returns `true` when the state is ready.
    /// * `f` - Closure that receives mutable access to the ready state.
    ///
    /// # Returns
    ///
    /// The value returned by `f` after the predicate has become true.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready` or `f`. Panics if the registry exhausts
    /// registration identifiers.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{
    ///     sync::Arc,
    ///     thread,
    /// };
    ///
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = Arc::new(ParkingLotMonitor::new(false));
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
    /// monitor.with_write_notify_one(|ready| *ready = true);
    ///
    /// assert_eq!(waiter.join().expect("waiter should finish"), "done");
    /// ```
    #[inline(always)]
    pub fn wait_until<R, P, F>(&self, mut ready: P, f: F) -> R
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.wait_while(|state| !ready(state), f)
    }

    /// Waits until a predicate becomes true without changing the ready state.
    ///
    /// The predicate runs while the monitor lock is held. This convenience
    /// method is equivalent to [`Self::wait_until`] with a no-op action.
    ///
    /// # Type Parameters
    ///
    /// * `P` - Predicate deciding when the state is ready.
    ///
    /// # Parameters
    ///
    /// * `ready` - Predicate that returns `true` when the caller may continue.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready`. Panics if the registry exhausts
    /// registration identifiers.
    #[inline(always)]
    pub fn wait_until_ready<P>(&self, ready: P)
    where
        P: FnMut(&T) -> bool,
    {
        self.wait_until(ready, |_| ());
    }

    /// Waits while a predicate remains true with a relative condition-wait
    /// budget.
    ///
    /// This method is the timeout-aware form of [`Self::wait_while`]. It keeps
    /// rechecking `waiting` under the monitor lock and waits only for the
    /// remaining portion of `timeout`. If `waiting` becomes false on the
    /// deciding locked predicate check, `f` runs while the lock is still held.
    /// If the Timer completes while `waiting` remains true on that check, the
    /// closure is not called.
    ///
    /// The timeout budget is aligned with
    /// [`std::sync::Condvar::wait_timeout_while`]: it starts after acquiring
    /// the monitor lock and before the first predicate check. Initial lock
    /// contention is excluded, but predicate work consumes the budget. One
    /// fixed deadline is reused across wakeups, and the method may return after
    /// the timeout while reacquiring the monitor lock. After a successful Timer
    /// completion, readiness wins one final locked predicate check. A Timer
    /// registration or completion error takes precedence over every post-wait
    /// predicate result and prevents `f` from running. A zero timeout still
    /// checks the predicate once.
    ///
    /// Timeout status alone is not used as proof that the predicate is still
    /// true; the predicate is always rechecked under the lock.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget after acquiring the lock.
    /// * `waiting` - Predicate that returns `true` while the caller should
    ///   continue waiting.
    /// * `f` - Closure that receives mutable access when waiting is no longer
    ///   required.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f` when the
    /// predicate stops blocking on the deciding locked check. Returns
    /// [`WaitTimeoutResult::TimedOut`] when the Timer completes and the
    /// predicate still blocks on that check.
    ///
    /// # Errors
    ///
    /// Returns Timer registration or completion errors rather than reporting
    /// them as timeouts. After waiting begins, such an error takes precedence
    /// over post-wait readiness.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `waiting` or `f`. Panics if the registry
    /// exhausts registration identifiers.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use qubit_lock::{ParkingLotMonitor, WaitTimeoutResult};
    ///
    /// let monitor = ParkingLotMonitor::new(Vec::<i32>::new());
    /// let result = monitor.wait_while_for(
    ///     Duration::from_millis(1),
    ///     |items| items.is_empty(),
    ///     |items| items.pop(),
    /// );
    ///
    /// assert!(matches!(result, Ok(WaitTimeoutResult::TimedOut)));
    /// ```
    pub fn wait_while_for<R, P, F>(
        &self,
        timeout: Duration,
        waiting: P,
        f: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        wait_while_for_internal(
            self.timer(),
            timeout,
            || self.lock(),
            waiting,
            f,
            |guard, future| guard.wait_with_timer(future),
        )
    }

    /// Waits while a predicate remains true with one operation-wide timeout.
    ///
    /// The Timer fixes an absolute deadline before this method attempts to
    /// acquire the monitor lock. Initial lock contention, predicate
    /// evaluation, condition waiting, and lock reacquisition therefore consume
    /// the same budget. When `waiting` becomes false on the deciding locked
    /// check, `f` runs without a time limit while the state remains locked.
    ///
    /// Reaching the deadline does not interrupt mutex acquisition or
    /// reacquisition, so this method may return after `timeout`.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `waiting` - Predicate that returns `true` while waiting continues.
    /// * `f` - Closure receiving mutable state after waiting becomes false.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f`, or
    /// [`WaitTimeoutResult::TimedOut`] when the fixed deadline has passed while
    /// `waiting` remains true on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns a deadline overflow before acquiring the monitor lock or
    /// running `waiting`. Returns Timer domain, registration, or completion
    /// errors when waiting is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `waiting` or `f`. Panics if the registry
    /// exhausts registration identifiers.
    #[inline]
    pub fn wait_while_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        waiting: P,
        f: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        let deadline = self.timer().clock().deadline_after(timeout)?;
        self.wait_while_with_deadline(deadline, waiting, f)
    }

    /// Waits while a predicate remains true or until an absolute deadline.
    ///
    /// The deadline includes initial lock contention and predicate evaluation.
    /// A ready predicate wins even when the deadline has passed.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding whether waiting must continue.
    /// * `F` - Action run once waiting finishes.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the monitor Timer's clock domain.
    /// * `waiting` - Predicate returning `true` while waiting must continue.
    /// * `f` - Action receiving mutable state when waiting finishes.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f`, or
    /// [`WaitTimeoutResult::TimedOut`] when the Timer completes and `waiting`
    /// remains true on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns Timer domain, registration, or completion errors when waiting
    /// is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `waiting` or `f`. Panics if the registry
    /// exhausts registration identifiers.
    pub fn wait_while_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        waiting: P,
        f: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        wait_while_with_deadline_internal(
            self.timer(),
            deadline,
            || self.lock(),
            waiting,
            f,
            |guard, future| guard.wait_with_timer(future),
        )
    }

    /// Waits until a predicate becomes true with a relative condition-wait
    /// budget.
    ///
    /// This is the positive-predicate counterpart of
    /// [`Self::wait_while_for`]. If `ready` becomes true on the deciding
    /// locked predicate check, `f` runs while the monitor lock is still held.
    /// If the Timer completes while `ready` remains false on that check, the
    /// closure is not called.
    ///
    /// The timeout budget is aligned with
    /// [`std::sync::Condvar::wait_timeout_while`]: it starts after acquiring
    /// the monitor lock and before the first predicate check. Initial lock
    /// contention is excluded, but predicate work consumes the budget. One
    /// fixed deadline is reused across wakeups, and the method may return after
    /// the timeout while reacquiring the monitor lock. After a successful Timer
    /// completion, readiness wins one final locked predicate check. A Timer
    /// registration or completion error takes precedence over every post-wait
    /// predicate result and prevents `f` from running. A zero timeout still
    /// checks the predicate once.
    ///
    /// Timeout status alone is not used as proof that the predicate is still
    /// false; the predicate is always rechecked under the lock.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget after acquiring the lock.
    /// * `ready` - Predicate that returns `true` when the caller may continue.
    /// * `f` - Closure that receives mutable access to the ready state.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f` when the
    /// predicate becomes true on the deciding locked check. Returns
    /// [`WaitTimeoutResult::TimedOut`] when the Timer completes and the
    /// predicate remains false on that check.
    ///
    /// # Errors
    ///
    /// Returns Timer registration or completion errors rather than reporting
    /// them as timeouts. After waiting begins, such an error takes precedence
    /// over post-wait readiness.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready` or `f`. Panics if the registry exhausts
    /// registration identifiers.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{
    ///     sync::Arc,
    ///     thread,
    ///     time::Duration,
    /// };
    ///
    /// use qubit_lock::{ParkingLotMonitor, WaitTimeoutResult};
    ///
    /// let monitor = Arc::new(ParkingLotMonitor::new(false));
    /// let waiter_monitor = Arc::clone(&monitor);
    ///
    /// let waiter = thread::spawn(move || {
    ///     waiter_monitor.wait_until_for(
    ///         Duration::from_secs(1),
    ///         |ready| *ready,
    ///         |ready| {
    ///             *ready = false;
    ///             5
    ///         },
    ///     )
    /// });
    ///
    /// monitor.with_write_notify_one(|ready| *ready = true);
    ///
    /// let outcome = waiter
    ///     .join()
    ///     .expect("waiter should finish")
    ///     .expect("timer registration should succeed");
    /// assert_eq!(outcome, WaitTimeoutResult::Ready(5));
    /// ```
    #[inline(always)]
    pub fn wait_until_for<R, P, F>(
        &self,
        timeout: Duration,
        mut ready: P,
        f: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.wait_while_for(timeout, |state| !ready(state), f)
    }

    /// Waits until a predicate becomes true with one operation-wide timeout.
    ///
    /// The Timer fixes an absolute deadline before initial lock acquisition.
    /// Initial lock contention therefore consumes the timeout. A ready
    /// predicate wins on the deciding locked check even when the deadline has
    /// already passed. The closure then runs without a time limit while the
    /// monitor lock remains held.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `ready` - Predicate that returns `true` when the caller may continue.
    /// * `f` - Closure receiving mutable access to the ready state.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f`, or
    /// [`WaitTimeoutResult::TimedOut`] when the total budget expires and the
    /// predicate remains false on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns deadline construction or Timer errors from
    /// [`Self::wait_while_with_total_timeout`].
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready` or `f`. Panics if the registry exhausts
    /// registration identifiers.
    #[inline(always)]
    pub fn wait_until_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        mut ready: P,
        f: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.wait_while_with_total_timeout(timeout, |state| !ready(state), f)
    }

    /// Waits until a predicate becomes true or an absolute deadline passes.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Value returned by `f`.
    /// * `P` - Predicate deciding when the state is ready.
    /// * `F` - Action run once the state is ready.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the monitor Timer's clock domain.
    /// * `ready` - Predicate returning `true` when the caller may continue.
    /// * `f` - Action receiving mutable state once `ready` succeeds.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with the value returned by `f`, or
    /// [`WaitTimeoutResult::TimedOut`] when the Timer completes and `ready`
    /// remains false on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns Timer domain, registration, or completion errors from
    /// [`Self::wait_while_with_deadline`] when waiting is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready` or `f`. Panics if the registry exhausts
    /// registration identifiers.
    #[inline(always)]
    pub fn wait_until_with_deadline<R, P, F>(
        &self,
        deadline: MonotonicInstant,
        mut ready: P,
        f: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        self.wait_while_with_deadline(deadline, |state| !ready(state), f)
    }

    /// Waits until a predicate becomes true or a timeout expires without
    /// changing the ready state.
    ///
    /// This convenience method is equivalent to [`Self::wait_until_for`] with
    /// a no-op action. Its timeout budget and deadline-boundary semantics are
    /// unchanged.
    ///
    /// # Type Parameters
    ///
    /// * `P` - Predicate deciding when the state is ready.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative condition-wait budget after acquiring the lock.
    /// * `ready` - Predicate that returns `true` when the caller may continue.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with `()` when the predicate becomes true,
    /// or [`WaitTimeoutResult::TimedOut`] when the Timer completes while the
    /// predicate remains false on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns Timer registration or completion errors rather than reporting
    /// them as timeouts. After waiting begins, such an error takes precedence
    /// over post-wait readiness.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready`. Panics if the registry exhausts
    /// registration identifiers.
    #[inline(always)]
    pub fn wait_until_ready_for<P>(
        &self,
        timeout: Duration,
        ready: P,
    ) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&T) -> bool,
    {
        self.wait_until_for(timeout, ready, |_| ())
    }

    /// Waits for readiness with one operation-wide timeout.
    ///
    /// This convenience method does not mutate ready state after the predicate
    /// succeeds. Its initial-lock, deadline-boundary, and Timer error semantics
    /// are identical to [`Self::wait_until_with_total_timeout`].
    ///
    /// # Type Parameters
    ///
    /// * `P` - Predicate deciding when the state is ready.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Relative budget fixed before initial lock acquisition.
    /// * `ready` - Predicate that returns `true` when the caller may continue.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with `()` when `ready` becomes true, or
    /// [`WaitTimeoutResult::TimedOut`] when the total budget expires and
    /// `ready` remains false on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns deadline construction or Timer errors from
    /// [`Self::wait_until_with_total_timeout`].
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready`. Panics if the registry exhausts
    /// registration identifiers.
    #[inline(always)]
    pub fn wait_until_ready_with_total_timeout<P>(
        &self,
        timeout: Duration,
        ready: P,
    ) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&T) -> bool,
    {
        self.wait_until_with_total_timeout(timeout, ready, |_| ())
    }

    /// Waits until a predicate becomes true or an absolute deadline passes.
    ///
    /// # Type Parameters
    ///
    /// * `P` - Predicate deciding when the state is ready.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Absolute deadline in the monitor Timer's clock domain.
    /// * `ready` - Predicate returning `true` when the caller may continue.
    ///
    /// # Returns
    ///
    /// [`WaitTimeoutResult::Ready`] with `()` when `ready` succeeds, or
    /// [`WaitTimeoutResult::TimedOut`] when the Timer completes and `ready`
    /// remains false on the deciding locked check.
    ///
    /// # Errors
    ///
    /// Returns Timer domain, registration, or completion errors from
    /// [`Self::wait_until_with_deadline`] when waiting is required.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `ready`. Panics if the registry exhausts
    /// registration identifiers.
    #[inline(always)]
    pub fn wait_until_ready_with_deadline<P>(
        &self,
        deadline: MonotonicInstant,
        ready: P,
    ) -> Result<WaitTimeoutResult<()>, TimeError>
    where
        P: FnMut(&T) -> bool,
    {
        self.wait_until_with_deadline(deadline, ready, |_| ())
    }

    /// Wakes one registered condition waiter.
    ///
    /// Notifications do not carry state by themselves. A waiting thread only
    /// proceeds safely after rechecking the protected state. Prefer
    /// [`Self::with_write_notify_one`] when one operation both changes the
    /// condition and wakes a waiter. This lower-level method remains useful
    /// when state was changed through an explicit guard or notification is
    /// conditional.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, thread};
    ///
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = Arc::new(ParkingLotMonitor::new(0_u32));
    /// let waiter = {
    ///     let m = monitor.clone();
    ///     thread::spawn(move || {
    ///         m.wait_until(|n| *n > 0, |n| {
    ///             *n -= 1;
    ///         });
    ///     })
    /// };
    ///
    /// {
    ///     let mut n = monitor.lock();
    ///     *n = 1;
    /// }
    /// monitor.notify_one();
    /// waiter.join().expect("waiter should finish");
    /// ```
    #[inline(always)]
    pub fn notify_one(&self) {
        self.waiters.notify_one();
    }

    /// Wakes all registered condition waiters.
    ///
    /// Notifications do not carry state by themselves. Every awakened thread
    /// must recheck the protected state before continuing. Prefer
    /// [`Self::with_write_notify_all`] when one operation both changes the
    /// condition and wakes waiters. This lower-level method remains useful
    /// when state was changed through an explicit guard or notification is
    /// conditional.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, thread};
    ///
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor = Arc::new(ParkingLotMonitor::new(false));
    /// let mut handles = Vec::new();
    /// for _ in 0..2 {
    ///     let m = monitor.clone();
    ///     handles.push(thread::spawn(move || {
    ///         m.wait_until(|ready| *ready, |_| ());
    ///     }));
    /// }
    ///
    /// {
    ///     let mut ready = monitor.lock();
    ///     *ready = true;
    /// }
    /// monitor.notify_all();
    /// for h in handles {
    ///     h.join().expect("waiter should finish");
    /// }
    /// ```
    #[inline(always)]
    pub fn notify_all(&self) {
        self.waiters.notify_all();
    }
}

impl<T> Notifier for ParkingLotMonitor<T> {
    /// Wakes one thread waiting on this monitor.
    #[inline(always)]
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all threads waiting on this monitor.
    #[inline(always)]
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T> ConditionWaiter for ParkingLotMonitor<T> {
    type State = T;

    /// Blocks while the predicate remains true, then runs the action.
    #[inline(always)]
    fn wait_while<R, P, F>(&self, predicate: P, action: F) -> R
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        Self::wait_while(self, predicate, action)
    }
}

impl<T> Monitor for ParkingLotMonitor<T> {
    /// Reads protected state while holding the monitor lock.
    #[inline(always)]
    fn with_read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&Self::State) -> R,
    {
        Self::with_read(self, f)
    }

    /// Mutates protected state while holding the monitor lock.
    #[inline(always)]
    fn with_write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::State) -> R,
    {
        Self::with_write(self, f)
    }
}

impl<T> TimeoutConditionWaiter for ParkingLotMonitor<T> {
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
        Self::wait_while_with_deadline(self, deadline, predicate, action)
    }

    /// Blocks while the predicate remains true with one operation-wide budget.
    #[inline(always)]
    fn wait_while_with_total_timeout<R, P, F>(
        &self,
        timeout: Duration,
        predicate: P,
        action: F,
    ) -> Result<WaitTimeoutResult<R>, TimeError>
    where
        P: FnMut(&Self::State) -> bool,
        F: FnOnce(&mut Self::State) -> R,
    {
        Self::wait_while_with_total_timeout(self, timeout, predicate, action)
    }

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
        Self::wait_while_for(self, timeout, predicate, action)
    }
}

impl<T> From<T> for ParkingLotMonitor<T> {
    /// Creates a monitor from an initial state value.
    ///
    /// # Parameters
    ///
    /// * `value` - Initial state protected by the monitor.
    ///
    /// # Returns
    ///
    /// A monitor initialized with `value`.
    ///
    /// # Panics
    ///
    /// Panics if all process-wide clock-domain identifiers are exhausted.
    #[inline(always)]
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for ParkingLotMonitor<T> {
    /// Creates a monitor containing `T::default()`.
    ///
    /// # Returns
    ///
    /// A monitor protecting the default value for `T`.
    ///
    /// # Panics
    ///
    /// Propagates a panic from `T::default()`. Panics if all process-wide
    /// clock-domain identifiers are exhausted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use qubit_lock::ParkingLotMonitor;
    ///
    /// let monitor: ParkingLotMonitor<String> = ParkingLotMonitor::default();
    /// assert!(monitor.with_read(|s| s.is_empty()));
    /// ```
    #[inline(always)]
    fn default() -> Self {
        Self::new(T::default())
    }
}
