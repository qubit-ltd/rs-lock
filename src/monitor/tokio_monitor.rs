// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow inline-tests
//! Tokio-based asynchronous monitor.

use std::time::Duration;

use tokio::sync::{
    Mutex,
    Notify,
};
use tokio::time::Instant;

use super::{
    AsyncConditionWaiter,
    AsyncMonitorFuture,
    AsyncNotificationWaiter,
    AsyncTimeoutConditionWaiter,
    AsyncTimeoutNotificationWaiter,
    Notifier,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};

/// Test-only synchronization point between dropping the state guard and
/// polling the notification future.
#[cfg(test)]
struct NotificationRegistrationBoundaryHook {
    /// Address of the monitor instance controlled by this hook.
    target: usize,
    /// Channel used to report that a waiter dropped the protected-state guard.
    entered: std::sync::mpsc::Sender<()>,
    /// Condition used to release waiters to poll their notification futures.
    release: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    /// Real-time upper bound for waiting on the test controller.
    real_timeout: Duration,
}

/// Globally installed registration-boundary hook for the inline regression.
#[cfg(test)]
static NOTIFICATION_REGISTRATION_BOUNDARY_HOOK: std::sync::Mutex<
    Option<std::sync::Arc<NotificationRegistrationBoundaryHook>>,
> = std::sync::Mutex::new(None);

/// Restores the registration-boundary hook to its disabled state when the
/// owning test exits or panics.
#[cfg(test)]
struct NotificationRegistrationBoundaryHookGuard;

#[cfg(test)]
impl Drop for NotificationRegistrationBoundaryHookGuard {
    /// Removes the installed hook so unrelated tests cannot observe it.
    fn drop(&mut self) {
        *NOTIFICATION_REGISTRATION_BOUNDARY_HOOK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// Installs the registration-boundary hook used by the inline regression.
///
/// # Arguments
///
/// * `hook` - Barrier pair that controls the notification registration window.
///
/// # Returns
///
/// A guard that removes the hook when it is dropped.
#[cfg(test)]
fn install_notification_registration_boundary_hook(
    hook: std::sync::Arc<NotificationRegistrationBoundaryHook>,
) -> NotificationRegistrationBoundaryHookGuard {
    let mut installed = NOTIFICATION_REGISTRATION_BOUNDARY_HOOK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(installed.is_none(), "registration boundary hook installed twice");
    *installed = Some(hook);
    NotificationRegistrationBoundaryHookGuard
}

/// Runs the test-only hook after the state guard is released and before the
/// notification future is first polled.
///
/// The hook blocks the current test worker for at most the configured
/// real-time bound.
///
/// # Arguments
///
/// * `target` - Address of the monitor that reached the registration boundary.
#[cfg(test)]
fn run_notification_registration_boundary_hook(target: usize) {
    let hook = NOTIFICATION_REGISTRATION_BOUNDARY_HOOK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(hook) = hook.filter(|hook| hook.target == target) {
        hook.entered
            .send(())
            .expect("registration-window controller should receive waiter");
        let (released, release_changed) = &*hook.release;
        let released = released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (released, _) = release_changed
            .wait_timeout_while(released, hook.real_timeout, |released| {
                !*released
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            *released,
            "registration-window controller should release waiter"
        );
    }
}

/// Asynchronous monitor built on Tokio synchronization primitives.
///
/// `TokioMonitor` protects one state value with a Tokio mutex and coordinates
/// waiters with a Tokio notification primitive. Notification semantics follow
/// Tokio's [`Notify`] behavior.
pub struct TokioMonitor<T> {
    /// Protected monitor state.
    state: Mutex<T>,
    /// Notification primitive used to wake async waiters.
    changed: Notify,
}

impl<T> TokioMonitor<T> {
    /// Creates an asynchronous monitor protecting the supplied state.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial protected state.
    ///
    /// # Returns
    ///
    /// A Tokio-based monitor.
    pub fn new(state: T) -> Self {
        Self {
            state: Mutex::new(state),
            changed: Notify::new(),
        }
    }

    /// Acquires the monitor and reads the protected state.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives an immutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    pub async fn with_read_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.state.lock().await;
        f(&*guard)
    }

    /// Acquires the monitor and mutates the protected state.
    ///
    /// This does not notify waiters automatically.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    pub async fn with_write_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.state.lock().await;
        f(&mut *guard)
    }

    /// Mutates the protected state and wakes one waiter.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    pub async fn with_write_notify_one_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write_async(f).await;
        self.notify_one();
        result
    }

    /// Mutates the protected state and wakes all waiters.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference to the state.
    ///
    /// # Returns
    ///
    /// The value returned by the closure.
    pub async fn with_write_notify_all_async<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = self.with_write_async(f).await;
        self.notify_all();
        result
    }

    /// Wakes one async waiter.
    pub fn notify_one(&self) {
        self.changed.notify_one();
    }

    /// Wakes all async waiters.
    pub fn notify_all(&self) {
        self.changed.notify_waiters();
    }

    /// Calculates remaining timeout budget from a call-time start instant.
    ///
    /// # Arguments
    ///
    /// * `start` - Instant captured when the public wait method was called.
    /// * `timeout` - Total timeout budget.
    ///
    /// # Returns
    ///
    /// The remaining budget, or zero when the budget is exhausted.
    fn remaining_timeout(start: Instant, timeout: Duration) -> Duration {
        timeout.checked_sub(start.elapsed()).unwrap_or_default()
    }
}

impl<T> Notifier for TokioMonitor<T> {
    /// Wakes one async waiter.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Wakes all async waiters.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send> AsyncNotificationWaiter for TokioMonitor<T> {
    /// Returns a future that resolves after a Tokio notification.
    fn wait_async<'a>(&'a self) -> AsyncMonitorFuture<'a, ()> {
        Box::pin(self.changed.notified())
    }
}

impl<T: Send> AsyncTimeoutNotificationWaiter for TokioMonitor<T> {
    /// Returns a future that resolves after notification or timeout.
    fn wait_for_async<'a>(
        &'a self,
        timeout: Duration,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutStatus> {
        let start = Instant::now();
        let notified = self.changed.notified();
        Box::pin(async move {
            let remaining = Self::remaining_timeout(start, timeout);
            if remaining.is_zero() {
                return WaitTimeoutStatus::TimedOut;
            }
            match tokio::time::timeout(remaining, notified).await {
                Ok(()) => WaitTimeoutStatus::Woken,
                Err(_) => WaitTimeoutStatus::TimedOut,
            }
        })
    }
}

impl<T: Send> AsyncConditionWaiter for TokioMonitor<T> {
    type State = T;

    /// Returns a future that waits until the predicate becomes true.
    fn wait_until_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_async(move |state| !predicate(state), action)
    }

    /// Returns a future that waits while the predicate remains true.
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, R>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        Box::pin(async move {
            let mut guard = self.state.lock().await;
            while predicate(&*guard) {
                let notified = self.changed.notified();
                drop(guard);
                #[cfg(test)]
                run_notification_registration_boundary_hook(
                    std::ptr::from_ref(self).addr(),
                );
                notified.await;
                guard = self.state.lock().await;
            }
            action(&mut *guard)
        })
    }
}

impl<T: Send> AsyncTimeoutConditionWaiter for TokioMonitor<T> {
    /// Returns a future that waits until the predicate becomes true or times
    /// out.
    fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_for_async(
            timeout,
            move |state| !predicate(state),
            action,
        )
    }

    /// Returns a future that waits while the predicate remains true or times
    /// out.
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> AsyncMonitorFuture<'a, WaitTimeoutResult<R>>
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        let start = Instant::now();
        Box::pin(async move {
            let mut guard = self.state.lock().await;
            loop {
                if !predicate(&*guard) {
                    return WaitTimeoutResult::Ready(action(&mut *guard));
                }

                let remaining = Self::remaining_timeout(start, timeout);
                if remaining.is_zero() {
                    return WaitTimeoutResult::TimedOut;
                }

                let notified = self.changed.notified();
                drop(guard);
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    guard = self.state.lock().await;
                    if !predicate(&*guard) {
                        return WaitTimeoutResult::Ready(action(&mut *guard));
                    }
                    return WaitTimeoutResult::TimedOut;
                }
                guard = self.state.lock().await;
            }
        })
    }
}

impl<T> From<T> for TokioMonitor<T> {
    /// Creates a Tokio monitor from an initial state value.
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for TokioMonitor<T> {
    /// Creates a Tokio monitor containing `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };
    use std::sync::{
        Arc,
        Condvar,
        Mutex,
        mpsc,
    };
    use std::time::Duration;

    use super::{
        AsyncConditionWaiter,
        NotificationRegistrationBoundaryHook,
        TokioMonitor,
        install_notification_registration_boundary_hook,
    };

    /// Verifies that two condition waiters register before releasing the state
    /// lock, so two notifications cannot collapse into one retained permit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_tokio_monitor_notify_one_does_not_lose_registered_condition_waiter()
    {
        const REAL_TIMEOUT: Duration = Duration::from_millis(500);
        const WAITER_COUNT: usize = 2;

        let resources = Arc::new(AtomicUsize::new(0));
        let monitor = Arc::new(TokioMonitor::new(Arc::clone(&resources)));
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook = Arc::new(NotificationRegistrationBoundaryHook {
            target: Arc::as_ptr(&monitor).addr(),
            entered: entered_tx,
            release: Arc::clone(&release),
            real_timeout: REAL_TIMEOUT,
        });
        let _hook_guard =
            install_notification_registration_boundary_hook(hook);
        let (done_tx, done_rx) = mpsc::channel();
        let mut waiters = Vec::with_capacity(WAITER_COUNT);

        for _ in 0..WAITER_COUNT {
            let waiter_monitor = Arc::clone(&monitor);
            let done_tx = done_tx.clone();
            waiters.push(tokio::spawn(async move {
                waiter_monitor
                    .wait_until_async(
                        |available| available.load(Ordering::Acquire) > 0,
                        |available| {
                            available.fetch_sub(1, Ordering::AcqRel);
                        },
                    )
                    .await;
                done_tx
                    .send(())
                    .expect("test should receive waiter completion");
            }));
        }
        drop(done_tx);

        for waiter in 0..WAITER_COUNT {
            entered_rx.recv_timeout(REAL_TIMEOUT).unwrap_or_else(|_| {
                panic!(
                    "waiter {} of {WAITER_COUNT} should reach the registration window",
                    waiter + 1,
                )
            });
        }
        resources.store(WAITER_COUNT, Ordering::Release);
        monitor.notify_one();
        monitor.notify_one();
        let (released, release_changed) = &*release;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        release_changed.notify_all();

        for completion in 0..WAITER_COUNT {
            done_rx.recv_timeout(REAL_TIMEOUT).unwrap_or_else(|_| {
                panic!(
                    "waiter completion {} of {WAITER_COUNT} should arrive",
                    completion + 1,
                )
            });
        }
        for waiter in waiters {
            waiter.await.expect("condition waiter task should finish");
        }
        assert_eq!(0, resources.load(Ordering::Acquire));
    }
}
