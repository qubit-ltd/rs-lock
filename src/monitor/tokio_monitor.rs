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
    future::{
        Future,
        poll_fn,
    },
    sync::{
        Arc,
        Mutex as StdMutex,
    },
    task::Poll,
    time::Duration,
};

use tokio::sync::{
    Mutex,
    Notify,
};

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
    assert!(
        installed.is_none(),
        "registration boundary hook installed twice"
    );
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

/// One independently signalled Tokio condition waiter.
struct TokioConditionWaiter {
    /// Private signal that cannot transfer a selection to another waiter.
    signal: Notify,
}

impl TokioConditionWaiter {
    /// Creates an unsignalled condition waiter.
    fn new() -> Self {
        Self {
            signal: Notify::new(),
        }
    }
}

/// Removes an active waiter registration on cancellation or normal exit.
struct TokioConditionWaiterRegistration<'a> {
    /// Registry containing this waiter while it remains selectable.
    registry: &'a StdMutex<Vec<Arc<TokioConditionWaiter>>>,
    /// Independently signalled waiter owned by the pending condition wait.
    waiter: Arc<TokioConditionWaiter>,
}

/// Test-only callback run after timeout budget calculation and before waiter
/// registration.
#[cfg(test)]
type TimeoutBeforeRegistrationHook = Arc<dyn Fn() + Send + Sync>;

/// Test-only callback run after timed waiter registration and state release,
/// but before polling the signal or fixed timer.
#[cfg(test)]
type TimeoutAfterRegistrationHook = Arc<dyn Fn() + Send + Sync>;

/// Test-only callback run after a timed waiter consumes a signal, but before
/// it reacquires the protected state.
#[cfg(test)]
type TimeoutBeforeStateReacquireHook = Arc<dyn Fn() + Send + Sync>;

impl Drop for TokioConditionWaiterRegistration<'_> {
    /// Removes this waiter if no notification has selected it yet.
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
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
/// state guard, and unregisters its Tokio notification waiter without running
/// the action or rolling back protected-state changes. If `notify_one` has
/// already selected that waiter, cancellation discards that selection instead
/// of transferring it to another or future waiter. After an initial predicate
/// check requires waiting with a nonzero budget, a timed wait creates one timer
/// before registering its first waiter and reuses that fixed deadline across
/// wakeups. Registration and state-reacquisition time consume the
/// condition-wait budget; a signal cannot restart or extend it. When a signal
/// and the deadline are both ready, the deadline is selected first, followed
/// by one final locked predicate check. Polling the timer requires a Tokio
/// runtime with the time driver enabled. Initial mutex contention, an
/// immediately ready predicate, and a zero budget do not create a timer and
/// therefore do not require the time driver.
pub struct TokioMonitor<T> {
    /// Protected monitor state.
    state: Mutex<T>,
    /// Active condition waiters eligible for memoryless notification.
    waiters: StdMutex<Vec<Arc<TokioConditionWaiter>>>,
    /// Per-monitor timeout initialization hook for deadline regressions.
    #[cfg(test)]
    timeout_before_registration_hook:
        StdMutex<Option<TimeoutBeforeRegistrationHook>>,
    /// Per-monitor post-registration hook for deadline races.
    #[cfg(test)]
    timeout_after_registration_hook:
        StdMutex<Option<TimeoutAfterRegistrationHook>>,
    /// Per-monitor hook for deadline races while reacquiring protected state.
    #[cfg(test)]
    timeout_before_state_reacquire_hook:
        StdMutex<Option<TimeoutBeforeStateReacquireHook>>,
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
            waiters: StdMutex::new(Vec::new()),
            #[cfg(test)]
            timeout_before_registration_hook: StdMutex::new(None),
            #[cfg(test)]
            timeout_after_registration_hook: StdMutex::new(None),
            #[cfg(test)]
            timeout_before_state_reacquire_hook: StdMutex::new(None),
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
        let waiter = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop();
        if let Some(waiter) = waiter {
            waiter.signal.notify_one();
        }
    }

    /// Selects all registered async waiters without retaining protected state.
    pub fn notify_all(&self) {
        let waiters = {
            let mut registry = self
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *registry)
        };
        for waiter in waiters {
            waiter.signal.notify_one();
        }
    }

    /// Registers one waiter while the protected state lock is still held.
    ///
    /// # Returns
    ///
    /// A registration that removes the waiter if it is cancelled or leaves the
    /// wait before notification selects it.
    fn register_waiter(&self) -> TokioConditionWaiterRegistration<'_> {
        let waiter = Arc::new(TokioConditionWaiter::new());
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::clone(&waiter));
        TokioConditionWaiterRegistration {
            registry: &self.waiters,
            waiter,
        }
    }

    /// Installs the test-only timeout initialization callback.
    ///
    /// # Arguments
    ///
    /// * `hook` - Callback invoked after the timeout budget is calculated and
    ///   before the waiter acquires the registry mutex.
    #[cfg(test)]
    fn set_timeout_before_registration_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .timeout_before_registration_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::new(hook));
    }

    /// Runs the test-only timeout initialization callback without holding its
    /// configuration mutex.
    #[cfg(test)]
    fn run_timeout_before_registration_hook(&self) {
        let hook = self
            .timeout_before_registration_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Installs the test-only post-registration timeout callback.
    ///
    /// # Arguments
    ///
    /// * `hook` - Callback invoked after timed waiter registration and state
    ///   release, before the signal or fixed timer is polled.
    #[cfg(test)]
    fn set_timeout_after_registration_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .timeout_after_registration_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::new(hook));
    }

    /// Runs the test-only post-registration timeout callback without holding
    /// its configuration mutex.
    #[cfg(test)]
    fn run_timeout_after_registration_hook(&self) {
        let hook = self
            .timeout_after_registration_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Installs the test-only pre-reacquisition timeout callback.
    ///
    /// # Arguments
    ///
    /// * `hook` - Callback invoked after a signal is consumed and before the
    ///   timed waiter reacquires the protected state.
    #[cfg(test)]
    fn set_timeout_before_state_reacquire_hook<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self
            .timeout_before_state_reacquire_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::new(hook));
    }

    /// Runs the test-only pre-reacquisition timeout callback without holding
    /// its configuration mutex.
    #[cfg(test)]
    fn run_timeout_before_state_reacquire_hook(&self) {
        let hook = self
            .timeout_before_state_reacquire_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook();
        }
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
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
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
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
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
                let registration = self.register_waiter();
                drop(guard);
                #[cfg(test)]
                run_notification_registration_boundary_hook(
                    std::ptr::from_ref(self).addr(),
                );
                registration.waiter.signal.notified().await;
                drop(registration);
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
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
    /// After an initial blocking predicate with a nonzero budget, the method
    /// creates one timer before waiter registration. The current Tokio runtime
    /// must then have its time driver enabled or Tokio will panic. Registration
    /// time consumes the budget. Initial mutex contention, an immediately ready
    /// predicate, and a zero budget do not create a timer or require the time
    /// driver. The fixed deadline is reused across wakeups and followed by one
    /// final locked predicate check. Predicate readiness wins over timeout. If
    /// a signal wins before the timer is ready but reacquiring the state
    /// exhausts the fixed deadline, a still-blocking predicate times out
    /// without another waiter registration. When the signal and deadline are
    /// both ready, the deadline is selected first. A zero timeout still checks
    /// the predicate once.
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
    /// running `action` or rolling back protected-state changes. A notification
    /// that already selected this waiter is discarded rather than transferred.
    /// After an initial blocking predicate with a nonzero budget, the method
    /// creates one timer before waiter registration. The current Tokio runtime
    /// must then have its time driver enabled or Tokio will panic. Registration
    /// time consumes the budget. Initial mutex contention, an immediately ready
    /// predicate, and a zero budget do not create a timer or require the time
    /// driver. The fixed deadline is reused across wakeups and followed by one
    /// final locked predicate check. Predicate readiness wins over timeout. If
    /// a signal wins before the timer is ready but reacquiring the state
    /// exhausts the fixed deadline, a still-blocking predicate times out
    /// without another waiter registration. When the signal and deadline are
    /// both ready, the deadline is selected first. A zero timeout still checks
    /// the predicate once.
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
            if timeout.is_zero() {
                return WaitTimeoutResult::TimedOut;
            }

            // Tokio turns an unrepresentable relative deadline into a
            // far-future timer, avoiding `Instant` addition overflow.
            let deadline = tokio::time::sleep(timeout);
            tokio::pin!(deadline);
            #[cfg(test)]
            self.run_timeout_before_registration_hook();
            loop {
                let registration = self.register_waiter();
                drop(guard);
                #[cfg(test)]
                self.run_timeout_after_registration_hook();
                let timed_out = {
                    let notified = registration.waiter.signal.notified();
                    tokio::pin!(notified);
                    poll_fn(|context| {
                        if deadline.as_mut().poll(context).is_ready() {
                            Poll::Ready(true)
                        } else if notified.as_mut().poll(context).is_ready() {
                            Poll::Ready(false)
                        } else {
                            Poll::Pending
                        }
                    })
                    .await
                };
                drop(registration);
                if timed_out {
                    guard = self.state.lock().await;
                    if !predicate(&*guard) {
                        return WaitTimeoutResult::Ready(action(&mut *guard));
                    }
                    return WaitTimeoutResult::TimedOut;
                }
                #[cfg(test)]
                self.run_timeout_before_state_reacquire_hook();
                guard = self.state.lock().await;
                if !predicate(&*guard) {
                    return WaitTimeoutResult::Ready(action(&mut *guard));
                }
                if deadline.as_ref().is_elapsed()
                    || tokio::time::Instant::now()
                        >= deadline.as_ref().deadline()
                {
                    return WaitTimeoutResult::TimedOut;
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
        atomic::{
            AtomicUsize,
            Ordering,
        },
        mpsc,
    };
    use std::task::{
        Context,
        Poll,
        Wake,
        Waker,
    };
    use std::time::Duration;

    use super::{
        AsyncConditionWaiter,
        AsyncTimeoutConditionWaiter,
        NotificationRegistrationBoundaryHook,
        TokioMonitor,
        WaitTimeoutResult,
        install_notification_registration_boundary_hook,
    };

    /// Counts wakeups delivered to one manually polled future.
    #[derive(Default)]
    struct WakeCounter {
        /// Number of wakeups observed by this waker.
        wakes: AtomicUsize,
    }

    impl Wake for WakeCounter {
        /// Records a wakeup that consumes the waker's shared owner.
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        /// Records a wakeup through a borrowed shared owner.
        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl WakeCounter {
        /// Returns the number of wakeups observed so far.
        fn count(&self) -> usize {
            self.wakes.load(Ordering::SeqCst)
        }
    }

    /// Polls one future once with a wake-counting context.
    ///
    /// # Arguments
    ///
    /// * `future` - Pinned future to poll.
    /// * `wake_counter` - Counter backing the poll context's waker.
    ///
    /// # Returns
    ///
    /// The result of this single poll.
    fn poll_once<F: Future>(
        future: Pin<&mut F>,
        wake_counter: &Arc<WakeCounter>,
    ) -> Poll<F::Output> {
        let waker = Waker::from(Arc::clone(wake_counter));
        let mut context = Context::from_waker(&waker);
        future.poll(&mut context)
    }

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

    /// Verifies that `notify_one` without a registered waiter has no effect on
    /// a condition waiter registered later.
    #[test]
    fn test_tokio_monitor_notify_one_without_waiter_is_not_retained() {
        let monitor = TokioMonitor::new(());
        monitor.notify_one();

        let predicate_checks = Arc::new(AtomicUsize::new(0));
        let waiter_checks = Arc::clone(&predicate_checks);
        let mut waiter = Box::pin(monitor.wait_while_async(
            move |_| {
                waiter_checks.fetch_add(1, Ordering::SeqCst);
                true
            },
            |_| (),
        ));
        let wake_counter = Arc::new(WakeCounter::default());

        assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
        assert_eq!(
            1,
            predicate_checks.load(Ordering::SeqCst),
            "a notification sent without waiters must not reach a future waiter"
        );
    }

    /// Verifies that cancelling the waiter selected by `notify_one` discards
    /// that selection instead of transferring it to another waiter.
    #[test]
    fn test_tokio_monitor_cancelled_selected_waiter_discards_notification() {
        let monitor = TokioMonitor::new(());
        let first_checks = Arc::new(AtomicUsize::new(0));
        let second_checks = Arc::new(AtomicUsize::new(0));
        let first_waiter_checks = Arc::clone(&first_checks);
        let second_waiter_checks = Arc::clone(&second_checks);
        let mut first_waiter = Some(Box::pin(monitor.wait_while_async(
            move |_| {
                first_waiter_checks.fetch_add(1, Ordering::SeqCst);
                true
            },
            |_| (),
        )));
        let mut second_waiter = Some(Box::pin(monitor.wait_while_async(
            move |_| {
                second_waiter_checks.fetch_add(1, Ordering::SeqCst);
                true
            },
            |_| (),
        )));
        let first_wakes = Arc::new(WakeCounter::default());
        let second_wakes = Arc::new(WakeCounter::default());

        assert!(
            poll_once(
                first_waiter
                    .as_mut()
                    .expect("first waiter should still exist")
                    .as_mut(),
                &first_wakes,
            )
            .is_pending()
        );
        assert!(
            poll_once(
                second_waiter
                    .as_mut()
                    .expect("second waiter should still exist")
                    .as_mut(),
                &second_wakes,
            )
            .is_pending()
        );

        monitor.notify_one();
        assert_eq!(
            1,
            first_wakes.count() + second_wakes.count(),
            "notify_one should select exactly one registered waiter"
        );

        let transferred_wakes = if first_wakes.count() == 1 {
            drop(first_waiter.take());
            let transferred_wakes = second_wakes.count();
            monitor.notify_one();
            assert_eq!(
                1,
                second_wakes.count(),
                "the unselected waiter should receive the next notification"
            );
            assert!(
                poll_once(
                    second_waiter
                        .as_mut()
                        .expect("unselected waiter should still exist")
                        .as_mut(),
                    &second_wakes,
                )
                .is_pending()
            );
            assert_eq!(
                2,
                second_checks.load(Ordering::SeqCst),
                "the unselected waiter should recheck exactly once"
            );
            drop(second_waiter.take());
            transferred_wakes
        } else {
            drop(second_waiter.take());
            let transferred_wakes = first_wakes.count();
            monitor.notify_one();
            assert_eq!(
                1,
                first_wakes.count(),
                "the unselected waiter should receive the next notification"
            );
            assert!(
                poll_once(
                    first_waiter
                        .as_mut()
                        .expect("unselected waiter should still exist")
                        .as_mut(),
                    &first_wakes,
                )
                .is_pending()
            );
            assert_eq!(
                2,
                first_checks.load(Ordering::SeqCst),
                "the unselected waiter should recheck exactly once"
            );
            drop(first_waiter.take());
            transferred_wakes
        };

        let future_checks = Arc::new(AtomicUsize::new(0));
        let future_waiter_checks = Arc::clone(&future_checks);
        let mut future_waiter = Box::pin(monitor.wait_while_async(
            move |_| {
                future_waiter_checks.fetch_add(1, Ordering::SeqCst);
                true
            },
            |_| (),
        ));
        let future_wakes = Arc::new(WakeCounter::default());
        assert!(poll_once(future_waiter.as_mut(), &future_wakes).is_pending());

        assert_eq!(
            0, transferred_wakes,
            "cancelled selection must not wake the unselected waiter"
        );
        assert_eq!(
            1,
            future_checks.load(Ordering::SeqCst),
            "cancelled selection must not be retained for a future waiter"
        );
    }

    /// Verifies that timeout budget consumed before waiter registration is not
    /// restarted when the timer begins.
    #[tokio::test(flavor = "current_thread")]
    async fn test_tokio_monitor_timeout_includes_registration_delay() {
        const TIMEOUT: Duration = Duration::from_millis(5);
        const REGISTRATION_DELAY: Duration = Duration::from_millis(20);

        let monitor = TokioMonitor::new(());
        let delay_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_delay_gate = Arc::clone(&delay_gate);
        monitor.set_timeout_before_registration_hook(move || {
            let (released, release_changed) = &*hook_delay_gate;
            let released = released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (_released, wait_result) = release_changed
                .wait_timeout_while(released, REGISTRATION_DELAY, |released| {
                    !*released
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                wait_result.timed_out(),
                "registration delay gate should consume the timeout budget"
            );
        });
        let mut waiter =
            Box::pin(monitor.wait_while_for_async(TIMEOUT, |_| true, |_| ()));

        assert_eq!(
            Ok(WaitTimeoutResult::TimedOut),
            tokio::time::timeout(TIMEOUT, waiter.as_mut()).await,
            "the monitor deadline should expire before registration finishes"
        );
    }

    /// Verifies that an unrepresentable relative deadline becomes a far-future
    /// timer instead of overflowing `Instant` arithmetic.
    #[tokio::test(flavor = "current_thread")]
    async fn test_tokio_monitor_timeout_duration_max_does_not_overflow() {
        let monitor = TokioMonitor::new(());
        let mut waiter = Box::pin(monitor.wait_while_for_async(
            Duration::MAX,
            |_| true,
            |_| (),
        ));
        let wake_counter = Arc::new(WakeCounter::default());

        assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
    }

    /// Verifies that simultaneous signal and deadline readiness performs the
    /// final predicate check without registering another expired waiter.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_tokio_monitor_simultaneous_signal_and_deadline_do_not_reregister()
     {
        const TIMEOUT: Duration = Duration::from_millis(5);
        const DEADLINE_DELAY: Duration = Duration::from_millis(20);
        const REAL_TIMEOUT: Duration = Duration::from_secs(1);

        let monitor = Arc::new(TokioMonitor::new(true));
        let (entered_tx, entered_rx) = mpsc::sync_channel(2);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_release = Arc::clone(&release);
        monitor.set_timeout_after_registration_hook(move || {
            entered_tx
                .try_send(())
                .expect("controller should observe timed waiter registration");
            let (released, release_changed) = &*hook_release;
            let released = released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (released, _) = release_changed
                .wait_timeout_while(released, REAL_TIMEOUT, |released| {
                    !*released
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(*released, "controller should release timed waiter");
        });

        let waiter_monitor = Arc::clone(&monitor);
        let waiter = tokio::spawn(async move {
            waiter_monitor
                .wait_while_for_async(TIMEOUT, |blocked| *blocked, |_| ())
                .await
        });
        entered_rx
            .recv_timeout(REAL_TIMEOUT)
            .expect("timed waiter should reach the poll boundary");
        monitor.notify_one();

        let delay_gate = Mutex::new(false);
        let delay_changed = Condvar::new();
        let delayed = delay_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (delayed, delay_result) = delay_changed
            .wait_timeout_while(delayed, DEADLINE_DELAY, |delayed| !*delayed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(delay_result.timed_out(), "deadline delay should elapse");
        drop(delayed);

        let (released, release_changed) = &*release;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        release_changed.notify_all();

        let reregistered =
            entered_rx.recv_timeout(Duration::from_millis(50)).is_ok();
        if reregistered {
            monitor
                .with_write_notify_all_async(|blocked| *blocked = false)
                .await;
        }
        let result = tokio::time::timeout(REAL_TIMEOUT, waiter)
            .await
            .expect("timed waiter should finish within one second")
            .expect("timed waiter task should not panic");

        assert!(
            !reregistered,
            "an elapsed waiter must not register after its final predicate check"
        );
        assert_eq!(WaitTimeoutResult::TimedOut, result);
    }

    /// Verifies that a signal selected before the deadline cannot restart the
    /// wait after state reacquisition consumes the remaining budget.
    #[tokio::test(flavor = "current_thread")]
    async fn test_tokio_monitor_signal_reacquire_crossing_deadline_does_not_reregister()
     {
        const TIMEOUT: Duration = Duration::from_millis(5);
        const REACQUIRE_DELAY: Duration = Duration::from_millis(20);

        let monitor = Arc::new(TokioMonitor::new(true));
        let registrations = Arc::new(AtomicUsize::new(0));
        let hook_registrations = Arc::clone(&registrations);
        let hook_monitor = Arc::downgrade(&monitor);
        monitor.set_timeout_after_registration_hook(move || {
            if hook_registrations.fetch_add(1, Ordering::SeqCst) == 0 {
                hook_monitor
                    .upgrade()
                    .expect("monitor should outlive its waiter")
                    .notify_one();
            }
        });
        monitor.set_timeout_before_state_reacquire_hook(|| {
            let delay_gate = Mutex::new(false);
            let delay_changed = Condvar::new();
            let delayed = delay_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (_delayed, delay_result) = delay_changed
                .wait_timeout_while(delayed, REACQUIRE_DELAY, |delayed| {
                    !*delayed
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                delay_result.timed_out(),
                "state reacquisition delay should consume the timeout budget"
            );
        });
        let mut waiter = Box::pin(monitor.wait_while_for_async(
            TIMEOUT,
            |blocked| *blocked,
            |_| (),
        ));
        let wake_counter = Arc::new(WakeCounter::default());

        assert_eq!(
            Poll::Ready(WaitTimeoutResult::TimedOut),
            poll_once(waiter.as_mut(), &wake_counter),
            "a blocking predicate must time out after reacquisition consumes the budget"
        );
        assert_eq!(
            1,
            registrations.load(Ordering::SeqCst),
            "an elapsed waiter must not register after reacquiring state"
        );
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
        let _hook_guard = install_notification_registration_boundary_hook(hook);
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
