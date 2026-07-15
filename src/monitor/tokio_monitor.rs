// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow inline-tests
//! Tokio-based asynchronous monitor.

use std::{
    future::Future,
    time::Duration,
};

use tokio::sync::{
    Mutex,
    Notify,
};
use tokio::time::Instant;

use super::{
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
    Notifier,
    WaitTimeoutResult,
};

/// Test-only synchronization point between dropping the state guard and
/// awaiting the registered notification future.
#[cfg(test)]
struct NotificationRegistrationBoundaryHook {
    /// Address of the monitor instance controlled by this hook.
    target: usize,
    /// Channel used to report that a waiter dropped the protected-state guard.
    entered: std::sync::mpsc::SyncSender<()>,
    /// Condition used to release waiters to await their notification futures.
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
/// * `hook` - Bounded coordination state for the registration window.
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

/// Runs the test-only hook after the notification is registered and the state
/// guard is released, but before the notification future is awaited.
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
            .try_send(())
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
/// waiters with a Tokio notification primitive. Notifications have memoryless
/// condition-variable semantics: they select already registered waiters but
/// carry no protected state, so every wake is followed by a predicate recheck.
/// Waiter selection has no fairness or FIFO guarantee.
///
/// Dropping a pending condition-wait future cancels the wait, releases any held
/// state guard, and unregisters its Tokio notification waiter. A `notify_one`
/// signal selected concurrently with cancellation follows [`Notify`]'s
/// cancellation behavior and is offered to another waiter. A timed wait creates
/// a timer only for a nonzero remaining budget immediately before an actual
/// condition-wait suspension; polling that timer requires a Tokio runtime with
/// the time driver enabled. Initial mutex contention, an immediately ready
/// predicate, and a zero or already exhausted budget do not create a timer and
/// therefore do not require the time driver.
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

    /// Selects at most one registered async waiter without a fairness
    /// guarantee.
    pub fn notify_one(&self) {
        self.changed.notify_one();
    }

    /// Selects all registered async waiters without retaining protected state.
    pub fn notify_all(&self) {
        self.changed.notify_waiters();
    }

    /// Calculates remaining timeout budget from the condition-wait start.
    ///
    /// # Arguments
    ///
    /// * `start` - Instant captured immediately before the first condition-wait
    ///   suspension.
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
    /// Selects at most one registered async waiter without a fairness
    /// guarantee.
    fn notify_one(&self) {
        Self::notify_one(self);
    }

    /// Selects all registered async waiters without retaining protected state.
    fn notify_all(&self) {
        Self::notify_all(self);
    }
}

impl<T: Send> AsyncConditionWaiter for TokioMonitor<T> {
    type State = T;

    /// Returns a future that rechecks the protected predicate until it becomes
    /// true.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action`.
    fn wait_until_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        self.wait_while_async(move |state| !predicate(state), action)
    }

    /// Returns a future that rechecks the protected predicate while it remains
    /// true.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action`.
    #[allow(
        clippy::manual_async_fn,
        reason = "the explicit Send bound is part of the trait contract"
    )]
    fn wait_while_async<'a, R, P, F>(
        &'a self,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = R> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        async move {
            let mut guard = self.state.lock().await;
            while predicate(&*guard) {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                drop(guard);
                #[cfg(test)]
                run_notification_registration_boundary_hook(
                    std::ptr::from_ref(self).addr(),
                );
                notified.await;
                guard = self.state.lock().await;
            }
            action(&mut *guard)
        }
    }
}

impl<T: Send> AsyncTimeoutConditionWaiter for TokioMonitor<T> {
    /// Returns a future that rechecks the predicate until it becomes true or
    /// the timeout expires.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action`. The method creates a timer only for a nonzero remaining
    /// budget immediately before an actual condition-wait suspension; the
    /// current Tokio runtime must then have its time driver enabled or Tokio
    /// will panic. Initial mutex contention, an immediately ready predicate,
    /// and a zero or already exhausted budget do not create a timer and do not
    /// require the time driver. The timeout uses one fixed deadline across
    /// wakeups and performs one final locked predicate check at the deadline.
    /// Readiness wins over timeout, and zero timeout still checks the predicate
    /// once.
    fn wait_until_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
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

    /// Returns a future that rechecks the predicate while it remains true or
    /// until the timeout expires.
    ///
    /// The waiter registers before releasing the state lock. Notifications
    /// carry no state and provide no fairness guarantee. Dropping the returned
    /// future while it is pending cancels and unregisters the wait without
    /// running `action`. The method creates a timer only for a nonzero remaining
    /// budget immediately before an actual condition-wait suspension; the
    /// current Tokio runtime must then have its time driver enabled or Tokio
    /// will panic. Initial mutex contention, an immediately ready predicate,
    /// and a zero or already exhausted budget do not create a timer and do not
    /// require the time driver. The timeout uses one fixed deadline across
    /// wakeups and performs one final locked predicate check at the deadline.
    /// Readiness wins over timeout, and zero timeout still checks the predicate
    /// once.
    #[allow(
        clippy::manual_async_fn,
        reason = "the explicit Send bound is part of the trait contract"
    )]
    fn wait_while_for_async<'a, R, P, F>(
        &'a self,
        timeout: Duration,
        mut predicate: P,
        action: F,
    ) -> impl Future<Output = WaitTimeoutResult<R>> + Send + 'a
    where
        R: Send + 'a,
        P: FnMut(&Self::State) -> bool + Send + 'a,
        F: FnOnce(&mut Self::State) -> R + Send + 'a,
    {
        async move {
            let mut guard = self.state.lock().await;
            if !predicate(&*guard) {
                return WaitTimeoutResult::Ready(action(&mut *guard));
            }
            let start = Instant::now();
            loop {
                let remaining = Self::remaining_timeout(start, timeout);
                if remaining.is_zero() {
                    return WaitTimeoutResult::TimedOut;
                }

                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                drop(guard);
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    guard = self.state.lock().await;
                    if !predicate(&*guard) {
                        return WaitTimeoutResult::Ready(action(&mut *guard));
                    }
                    return WaitTimeoutResult::TimedOut;
                }
                guard = self.state.lock().await;
                if !predicate(&*guard) {
                    return WaitTimeoutResult::Ready(action(&mut *guard));
                }
            }
        }
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
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        Condvar,
        Mutex,
        mpsc,
    };
    use std::task::{
        Context,
        Poll,
    };
    use std::time::Duration;

    use super::{
        AsyncConditionWaiter,
        NotificationRegistrationBoundaryHook,
        TokioMonitor,
        install_notification_registration_boundary_hook,
    };

    /// Preserves one lock future after proving that its first poll contended.
    struct ContendedLockFuture<F> {
        /// Pinned lock future retained across every poll.
        inner: Pin<Box<F>>,
        /// Bounded signal emitted when the first lock poll returns pending.
        pending: Option<mpsc::SyncSender<()>>,
        /// Permission to continue polling after the contention is observed.
        proceed: mpsc::Receiver<()>,
        /// Real-time upper bound for waiting for permission to proceed.
        real_timeout: Duration,
    }

    impl<F> ContendedLockFuture<F> {
        /// Wraps a lock future and gates it after its first pending poll.
        ///
        /// # Arguments
        ///
        /// * `inner` - Lock future whose queue position must be preserved.
        /// * `pending` - Bounded signal proving that the first poll contended.
        /// * `proceed` - Permission to resume polling the same lock future.
        /// * `real_timeout` - Maximum real time to wait for permission.
        ///
        /// # Returns
        ///
        /// A future that resolves to the wrapped lock future's output.
        fn new(
            inner: F,
            pending: mpsc::SyncSender<()>,
            proceed: mpsc::Receiver<()>,
            real_timeout: Duration,
        ) -> Self {
            Self {
                inner: Box::pin(inner),
                pending: Some(pending),
                proceed,
                real_timeout,
            }
        }
    }

    impl<F: Future> Future for ContendedLockFuture<F> {
        type Output = F::Output;

        /// Polls the retained lock future and gates its first pending result.
        ///
        /// The first poll must return pending. This method reports that result
        /// without dropping the lock future, waits for bounded controller
        /// permission, and then continues polling the same future.
        fn poll(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Self::Output> {
            let this = &mut *self;
            let result = this.inner.as_mut().poll(context);
            let Some(pending) = this.pending.take() else {
                return result;
            };
            assert!(
                result.is_pending(),
                "producer lock should contend on its first poll"
            );
            pending
                .try_send(())
                .expect("second waiter should observe producer contention");
            this.proceed
                .recv_timeout(this.real_timeout)
                .expect("controller should release contended producer");
            this.inner.as_mut().poll(context)
        }
    }

    /// Verifies that two condition waiters register before releasing the state
    /// lock, so two notifications cannot collapse into one retained permit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_tokio_monitor_notify_one_does_not_lose_registered_condition_waiter()
    {
        const REAL_TIMEOUT: Duration = Duration::from_secs(1);
        const WAITER_COUNT: usize = 2;

        let monitor = Arc::new(TokioMonitor::new(0_usize));
        let (entered_tx, entered_rx) = mpsc::sync_channel(WAITER_COUNT);
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

        let first_monitor = Arc::clone(&monitor);
        let first_done_tx = done_tx.clone();
        waiters.push(tokio::spawn(async move {
            first_monitor
                .wait_until_async(
                    |available| *available > 0,
                    |available| *available -= 1,
                )
                .await;
            first_done_tx
                .send(())
                .expect("test should receive first waiter completion");
        }));
        entered_rx
            .recv_timeout(REAL_TIMEOUT)
            .expect("first waiter should reach the registration window");

        let (holding_tx, holding_rx) = mpsc::sync_channel(1);
        let (producer_pending_tx, producer_pending_rx) = mpsc::sync_channel(1);
        let second_monitor = Arc::clone(&monitor);
        let second_done_tx = done_tx.clone();
        waiters.push(tokio::spawn(async move {
            let mut holding_tx = Some(holding_tx);
            let mut producer_pending_rx = Some(producer_pending_rx);
            second_monitor
                .wait_until_async(
                    move |available| {
                        if *available == 0 {
                            holding_tx
                                .take()
                                .expect("second waiter should report contention once")
                                .try_send(())
                                .expect("controller should observe held state lock");
                            producer_pending_rx
                                .take()
                                .expect("second waiter should await producer once")
                                .recv_timeout(REAL_TIMEOUT)
                                .expect("producer lock should become pending");
                        }
                        *available > 0
                    },
                    |available| *available -= 1,
                )
                .await;
            second_done_tx
                .send(())
                .expect("test should receive second waiter completion");
        }));
        drop(done_tx);
        holding_rx
            .recv_timeout(REAL_TIMEOUT)
            .expect("second waiter should hold the state lock");

        let (producer_proceed_tx, producer_proceed_rx) = mpsc::sync_channel(1);
        let (producer_done_tx, producer_done_rx) = mpsc::sync_channel(1);
        let producer_monitor = Arc::clone(&monitor);
        let producer = tokio::spawn(async move {
            let lock = producer_monitor.state.lock();
            let mut guard = ContendedLockFuture::new(
                lock,
                producer_pending_tx,
                producer_proceed_rx,
                REAL_TIMEOUT,
            )
            .await;
            *guard = WAITER_COUNT;
            drop(guard);
            producer_monitor.notify_one();
            producer_monitor.notify_one();
            producer_done_tx
                .try_send(())
                .expect("controller should observe producer completion");
        });

        entered_rx
            .recv_timeout(REAL_TIMEOUT)
            .expect("second waiter should reach the registration window");
        producer_proceed_tx
            .try_send(())
            .expect("producer should receive permission to acquire state lock");
        producer_done_rx
            .recv_timeout(REAL_TIMEOUT)
            .expect("producer should update state and notify both waiters");
        producer.await.expect("producer task should finish");

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
        assert_eq!(0, monitor.with_read_async(|available| *available).await);
    }
}
