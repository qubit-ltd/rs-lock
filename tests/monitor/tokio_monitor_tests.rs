// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TokioMonitor`](qubit_lock::TokioMonitor).

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
        mpsc,
    },
    task::{
        Context,
        Poll,
        Wake,
        Waker,
    },
    thread,
    time::Duration,
};

use super::failing_timer_tests::{
    assert_backend_unavailable,
    completion_failing_timer,
    registration_failing_timer,
};
use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
    MonotonicInstant,
    TimeError,
    Timer,
    TimerFuture,
    TokioRuntimeError,
};
use qubit_lock::{
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
    Notifier,
    TokioMonitor,
    WaitTimeoutResult,
};

/// Returns a future after proving at compile time that it is sendable.
fn assert_send<F: Future + Send>(future: F) -> F {
    future
}

/// Timer wrapper that stays pending for two polls before forwarding completion.
struct TwicePendingTimer<T> {
    /// Wrapped Timer providing the eventual result.
    inner: T,
    /// Number of times the registered future has been polled.
    poll_count: Arc<AtomicUsize>,
}

impl<T> TwicePendingTimer<T> {
    /// Creates a wrapper and exposes its poll counter to the test.
    ///
    /// # Parameters
    ///
    /// * `inner` - Timer whose completion is deferred.
    ///
    /// # Returns
    ///
    /// The wrapper and its shared poll counter.
    fn new(inner: T) -> (Self, Arc<AtomicUsize>) {
        let poll_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                inner,
                poll_count: Arc::clone(&poll_count),
            },
            poll_count,
        )
    }
}

impl<T> Timer for TwicePendingTimer<T>
where
    T: Timer,
{
    /// Returns the wrapped Timer's monotonic clock.
    fn clock(&self) -> &dyn MonotonicClock {
        self.inner.clock()
    }

    /// Defers the wrapped future's completion until its third poll.
    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        let mut future = self.inner.at(deadline)?;
        let poll_count = Arc::clone(&self.poll_count);
        Ok(Box::pin(std::future::poll_fn(move |context| {
            let prior_polls = poll_count.fetch_add(1, Ordering::SeqCst);
            if prior_polls < 2 {
                Poll::Pending
            } else {
                future.as_mut().poll(context)
            }
        })))
    }
}

#[tokio::test]
async fn test_tokio_monitor_completion_error_wins_over_post_wait_readiness() {
    let monitor =
        TokioMonitor::with_timer(false, Arc::new(completion_failing_timer()));
    let mut predicate_checks = 0;
    let mut action_calls = 0;

    let result = monitor
        .wait_until_for_async(
            Duration::from_secs(1),
            |_| {
                predicate_checks += 1;
                predicate_checks > 1
            },
            |_| {
                action_calls += 1;
            },
        )
        .await;

    let error =
        result.expect_err("Timer completion failure should outrank readiness");
    assert_backend_unavailable(error);
    assert_eq!(predicate_checks, 1);
    assert_eq!(action_calls, 0);
}

/// Verifies a Timer failure observed after notification still outranks the
/// newly ready predicate.
#[tokio::test]
async fn test_tokio_monitor_post_notification_timer_error_wins_over_readiness()
{
    let (timer, timer_polls) =
        TwicePendingTimer::new(completion_failing_timer());
    let monitor = TokioMonitor::with_timer(false, Arc::new(timer));
    let predicate_checks = Arc::new(AtomicUsize::new(0));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let predicate_check_counter = Arc::clone(&predicate_checks);
    let action_call_counter = Arc::clone(&action_calls);
    let mut waiter = Box::pin(monitor.wait_until_for_async(
        Duration::from_secs(1),
        move |ready| {
            predicate_check_counter.fetch_add(1, Ordering::SeqCst);
            *ready
        },
        move |_| {
            action_call_counter.fetch_add(1, Ordering::SeqCst);
        },
    ));
    let wake_counter = Arc::new(WakeCounter::default());

    assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
    assert_eq!(timer_polls.load(Ordering::SeqCst), 1);

    monitor
        .with_write_notify_one_async(|ready| *ready = true)
        .await;
    let result = match poll_once(waiter.as_mut(), &wake_counter) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("notification should complete the wait"),
    };
    drop(waiter);

    let error =
        result.expect_err("post-notification Timer failure should be visible");
    assert_backend_unavailable(error);
    assert_eq!(timer_polls.load(Ordering::SeqCst), 3);
    assert_eq!(predicate_checks.load(Ordering::SeqCst), 1);
    assert_eq!(action_calls.load(Ordering::SeqCst), 0);
}

/// Verifies that fallible ambient construction reports a missing runtime.
#[test]
fn test_tokio_monitor_try_current_reports_missing_runtime() {
    let error = match TokioMonitor::try_current(false) {
        Ok(_) => panic!("construction outside a runtime should fail"),
        Err(error) => error,
    };

    assert!(matches!(error, TokioRuntimeError::NotEntered { .. }));
}

/// Verifies that infallible ambient construction identifies its runtime
/// requirement in the panic message.
#[test]
#[should_panic(expected = "cannot create Tokio monitor")]
fn test_tokio_monitor_current_panics_outside_runtime() {
    let _monitor = TokioMonitor::current(false);
}

#[tokio::test]
async fn test_tokio_monitor_uses_injected_manual_timer_without_real_delay() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(TokioMonitor::with_timer(false, clock.new_timer()));
    assert_eq!(clock.now().domain(), monitor.timer().clock().now().domain(),);
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_until_for_async(
                Duration::from_secs(8),
                |ready| *ready,
                |_| (),
            )
            .await
    });

    let reached = clock.advance_to_next_deadline_async().await;
    assert_eq!(Duration::from_secs(8), reached.elapsed_since_origin());

    assert_time_result_eq!(
        Ok(WaitTimeoutResult::TimedOut),
        waiter.await.expect("waiter task should finish"),
    );
}

/// Verifies that the monitor's retained Tokio timer can be polled by another
/// runtime while the captured runtime remains responsible for time progress.
#[test]
fn test_tokio_monitor_uses_timer_across_runtimes() {
    let target = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("target runtime should build");
    let polling = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("polling runtime should build");
    let monitor = target.block_on(async { TokioMonitor::current(false) });
    let mut wait = Box::pin(monitor.wait_until_for_async(
        Duration::from_secs(5),
        |ready| *ready,
        |_| (),
    ));

    let early_result = polling.block_on(async {
        tokio::select! {
            result = &mut wait => Some(result),
            () = tokio::time::sleep(Duration::from_secs(1)) => None,
        }
    });
    assert!(
        early_result.is_none(),
        "advancing the polling runtime must not complete the timeout"
    );

    target.block_on(tokio::time::advance(Duration::from_secs(5)));
    assert_time_result_eq!(
        Ok(WaitTimeoutResult::TimedOut),
        polling.block_on(wait),
    );
}

#[tokio::test]
async fn test_tokio_monitor_cancellation_removes_manual_timer_registration() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(TokioMonitor::with_timer(false, clock.new_timer()));
    let waiter_monitor = Arc::clone(&monitor);
    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_until_for_async(
                Duration::from_secs(8),
                |ready| *ready,
                |_| (),
            )
            .await
    });

    let _deadline = clock.wait_for_next_deadline_async().await;
    waiter.abort();
    let error = waiter
        .await
        .expect_err("aborted waiter task should be cancelled");
    assert!(error.is_cancelled());
    assert_eq!(clock.pending_waiters(), 0);
}

#[tokio::test]
async fn test_tokio_monitor_propagates_timer_registration_error() {
    let monitor =
        TokioMonitor::with_timer(false, Arc::new(registration_failing_timer()));

    let result = monitor
        .wait_until_for_async(Duration::from_secs(1), |ready| *ready, |_| ())
        .await;

    let error = result.expect_err("failing Timer should reject registration");
    assert_backend_unavailable(error);
    assert!(!monitor.with_read_async(|ready| *ready).await);
}

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
}

impl WakeCounter {
    /// Returns the number of wakeups observed so far.
    fn count(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
}

/// Polls one future once with a wake-counting context.
///
/// # Parameters
///
/// * `future` - Pinned future to poll.
/// * `wake_counter` - Counter backing the poll context's waker.
///
/// # Returns
///
/// The result of this single poll.
fn poll_once<F: Future + ?Sized>(
    future: Pin<&mut F>,
    wake_counter: &Arc<WakeCounter>,
) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(wake_counter));
    let mut context = Context::from_waker(&waker);
    future.poll(&mut context)
}

/// Verifies that `notify_one` without a registered waiter has no effect on a
/// condition waiter registered later.
#[test]
fn test_tokio_monitor_notify_one_without_waiter_is_not_retained() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = TokioMonitor::with_timer((), clock.new_timer());
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

/// Verifies that two registered condition waiters receive two notifications.
#[tokio::test]
async fn test_tokio_monitor_notify_one_does_not_lose_registered_waiter() {
    let monitor = TokioMonitor::current(0_usize);
    let first = monitor.wait_until_async(
        |available| *available > 0,
        |available| *available -= 1,
    );
    let second = monitor.wait_until_async(
        |available| *available > 0,
        |available| *available -= 1,
    );
    tokio::pin!(first);
    tokio::pin!(second);
    let first_wakes = Arc::new(WakeCounter::default());
    let second_wakes = Arc::new(WakeCounter::default());

    assert!(poll_once(first.as_mut(), &first_wakes).is_pending());
    assert!(poll_once(second.as_mut(), &second_wakes).is_pending());
    monitor.with_write_async(|available| *available = 2).await;
    monitor.notify_one();
    monitor.notify_one();

    first.await;
    second.await;
    assert_eq!(monitor.with_read_async(|available| *available).await, 0);
}

/// Verifies that state reacquisition cannot extend a fixed timeout deadline.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_signal_reacquire_crossing_deadline_times_out() {
    const TIMEOUT: Duration = Duration::from_millis(5);
    const REAL_TIMEOUT: Duration = Duration::from_secs(1);

    let monitor = Arc::new(TokioMonitor::current(()));
    let wait = monitor.wait_while_for_async(TIMEOUT, |_| true, |_| ());
    tokio::pin!(wait);
    let wake_counter = Arc::new(WakeCounter::default());
    assert!(poll_once(wait.as_mut(), &wake_counter).is_pending());

    let holder_monitor = Arc::clone(&monitor);
    let (holding_tx, holding_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let holder = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("holder runtime should build");
        runtime.block_on(holder_monitor.with_write_async(|_| {
            holding_tx
                .send(())
                .expect("controller should observe held state lock");
            release_rx
                .recv_timeout(REAL_TIMEOUT)
                .expect("holder should receive release permission");
        }));
    });
    holding_rx
        .recv_timeout(REAL_TIMEOUT)
        .expect("holder should acquire the state lock");

    monitor.notify_one();
    assert!(
        poll_once(wait.as_mut(), &wake_counter).is_pending(),
        "selected waiter should block while reacquiring state",
    );
    tokio::time::advance(TIMEOUT).await;
    release_tx
        .send(())
        .expect("holder should receive release permission");
    holder.join().expect("holder thread should finish");

    assert_time_result_eq!(wait.await, Ok(WaitTimeoutResult::TimedOut));
}

/// Verifies that `Notifier::notify_all` selects every waiter registered at the
/// call boundary without retaining a signal for a future waiter.
#[test]
fn test_tokio_monitor_notify_all_selects_registered_waiters_only() {
    const REGISTERED_WAITERS: usize = 2;

    let clock = ManualMonotonicClock::new_shared();
    let monitor = TokioMonitor::with_timer(true, clock.new_timer());
    let predicate_checks = Arc::new(AtomicUsize::new(0));
    let mut waiters = Vec::with_capacity(REGISTERED_WAITERS);
    let mut wake_counters = Vec::with_capacity(REGISTERED_WAITERS);
    for _ in 0..REGISTERED_WAITERS {
        let waiter_checks = Arc::clone(&predicate_checks);
        let mut waiter = Box::pin(monitor.wait_while_async(
            move |blocked| {
                waiter_checks.fetch_add(1, Ordering::SeqCst);
                *blocked
            },
            |_| (),
        ));
        let wake_counter = Arc::new(WakeCounter::default());
        assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
        waiters.push(waiter);
        wake_counters.push(wake_counter);
    }

    <TokioMonitor<bool> as Notifier>::notify_all(&monitor);
    assert!(
        wake_counters.iter().all(|counter| counter.count() == 1),
        "notify_all should select every registered waiter exactly once"
    );
    drop(waiters);

    let future_checks = Arc::clone(&predicate_checks);
    let mut future_waiter = Box::pin(monitor.wait_while_async(
        move |blocked| {
            future_checks.fetch_add(1, Ordering::SeqCst);
            *blocked
        },
        |_| (),
    ));
    let future_wakes = Arc::new(WakeCounter::default());
    assert!(poll_once(future_waiter.as_mut(), &future_wakes).is_pending());
    assert_eq!(0, future_wakes.count());
    assert_eq!(
        REGISTERED_WAITERS + 1,
        predicate_checks.load(Ordering::SeqCst)
    );
}

/// Verifies that cancelling the waiter selected by `notify_one` discards that
/// selection instead of transferring it to another waiter.
#[test]
fn test_tokio_monitor_cancelled_selected_waiter_discards_notification() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = TokioMonitor::with_timer((), clock.new_timer());
    let predicate_checks =
        [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    let mut waiters: Vec<Option<Pin<Box<dyn Future<Output = ()> + Send>>>> =
        predicate_checks
            .iter()
            .map(|checks| {
                let waiter_checks = Arc::clone(checks);
                Some(Box::pin(monitor.wait_while_async(
                    move |_| {
                        waiter_checks.fetch_add(1, Ordering::SeqCst);
                        true
                    },
                    |_| (),
                ))
                    as Pin<Box<dyn Future<Output = ()> + Send>>)
            })
            .collect();
    let wake_counters = [
        Arc::new(WakeCounter::default()),
        Arc::new(WakeCounter::default()),
    ];

    for (waiter, wake_counter) in waiters.iter_mut().zip(&wake_counters) {
        assert!(
            poll_once(
                waiter
                    .as_mut()
                    .expect("registered waiter should still exist")
                    .as_mut(),
                wake_counter,
            )
            .is_pending()
        );
    }

    monitor.notify_one();
    assert_eq!(
        1,
        wake_counters
            .iter()
            .map(|counter| counter.count())
            .sum::<usize>(),
        "notify_one should select exactly one registered waiter"
    );

    let selected = wake_counters
        .iter()
        .position(|counter| counter.count() == 1)
        .expect("notify_one should select one waiter");
    let unselected = 1 - selected;
    drop(waiters[selected].take());
    let transferred_wakes = wake_counters[unselected].count();
    monitor.notify_one();
    assert_eq!(
        1,
        wake_counters[unselected].count(),
        "the unselected waiter should receive the next notification"
    );
    assert!(
        poll_once(
            waiters[unselected]
                .as_mut()
                .expect("unselected waiter should still exist")
                .as_mut(),
            &wake_counters[unselected],
        )
        .is_pending()
    );
    assert_eq!(
        2,
        predicate_checks[unselected].load(Ordering::SeqCst),
        "the unselected waiter should recheck exactly once"
    );
    drop(waiters[unselected].take());

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

/// Verifies a deadline wins when both timeout and notification are ready
/// before the waiter is polled again.
#[tokio::test]
async fn test_tokio_monitor_deadline_wins_over_simultaneous_notification() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = TokioMonitor::with_timer(false, clock.new_timer());
    let mut waiter = Box::pin(monitor.wait_until_for_async(
        Duration::from_secs(1),
        |ready| *ready,
        |_| (),
    ));
    let wake_counter = Arc::new(WakeCounter::default());

    assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
    monitor.notify_one();
    clock
        .advance(Duration::from_secs(1))
        .expect("manual clock should reach the deadline");

    assert!(matches!(
        poll_once(waiter.as_mut(), &wake_counter),
        Poll::Ready(Ok(WaitTimeoutResult::TimedOut)),
    ));
}

/// Verifies a panicking combined mutation does not notify one waiter.
#[tokio::test]
async fn test_tokio_monitor_panicking_write_notify_one_does_not_notify() {
    assert_panicking_combined_mutation_does_not_notify(false).await;
}

/// Verifies a panicking combined mutation does not notify all waiters.
#[tokio::test]
async fn test_tokio_monitor_panicking_write_notify_all_does_not_notify() {
    assert_panicking_combined_mutation_does_not_notify(true).await;
}

/// Checks that notification is skipped when a combined mutation panics.
///
/// # Parameters
///
/// * `notify_all` - Whether to exercise the notify-all variant.
async fn assert_panicking_combined_mutation_does_not_notify(notify_all: bool) {
    let monitor = Arc::new(TokioMonitor::current(false));
    let mut waiter = Box::pin(monitor.wait_until_async(|ready| *ready, |_| ()));
    let wake_counter = Arc::new(WakeCounter::default());
    assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());

    let mutation_monitor = Arc::clone(&monitor);
    let mutation = tokio::spawn(async move {
        if notify_all {
            mutation_monitor
                .with_write_notify_all_async(|_| panic!("mutation failed"))
                .await;
        } else {
            mutation_monitor
                .with_write_notify_one_async(|_| panic!("mutation failed"))
                .await;
        }
    });

    assert!(
        mutation
            .await
            .expect_err("mutation should panic")
            .is_panic()
    );
    assert_eq!(wake_counter.count(), 0);
    drop(waiter);
}

/// Verifies that an unrepresentable relative deadline is reported by Timer.
#[tokio::test(flavor = "current_thread")]
async fn test_tokio_monitor_reports_timeout_duration_overflow() {
    let monitor = TokioMonitor::current(());
    let mut waiter =
        Box::pin(monitor.wait_while_for_async(Duration::MAX, |_| true, |_| ()));
    let wake_counter = Arc::new(WakeCounter::default());

    assert!(matches!(
        poll_once(waiter.as_mut(), &wake_counter),
        Poll::Ready(Err(TimeError::InstantOverflow)),
    ));
}

/// Verifies that all Tokio condition-wait trait methods return `Send` futures.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_condition_wait_futures_are_send() {
    let monitor = TokioMonitor::current(true);

    assert!(
        assert_send(
            <TokioMonitor<bool> as AsyncConditionWaiter>::wait_until_async(
                &monitor,
                |ready| *ready,
                |ready| *ready,
            ),
        )
        .await,
    );
    assert_send(
        <TokioMonitor<bool> as AsyncConditionWaiter>::wait_until_ready_async(
            &monitor,
            |ready| *ready,
        ),
    )
    .await;
    assert!(
        assert_send(
            <TokioMonitor<bool> as AsyncConditionWaiter>::wait_while_async(
                &monitor,
                |ready| !*ready,
                |ready| *ready,
            ),
        )
        .await,
    );
    assert_time_result_eq!(
        assert_send(
            <TokioMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
                &monitor,
                Duration::ZERO,
                |ready| *ready,
                |ready| *ready,
            ),
        )
        .await,
        Ok(WaitTimeoutResult::Ready(true)),
    );
    assert_time_result_eq!(
        assert_send(
            <TokioMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_until_ready_for_async(
                &monitor,
                Duration::ZERO,
                |ready| *ready,
            ),
        )
        .await,
        Ok(WaitTimeoutResult::Ready(())),
    );
    assert_time_result_eq!(
        assert_send(
            <TokioMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
                &monitor,
                Duration::ZERO,
                |ready| !*ready,
                |ready| *ready,
            ),
        )
        .await,
        Ok(WaitTimeoutResult::Ready(true)),
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_helpers_delegate_to_state() {
    let monitor = TokioMonitor::current(vec![1]);

    monitor.with_write_async(|items| items.push(2)).await;
    assert_eq!(
        monitor.with_read_async(|items| items.clone()).await,
        vec![1, 2],
    );

    let one_result = monitor
        .with_write_notify_one_async(|items| {
            items.push(3);
            items.len()
        })
        .await;
    assert_eq!(one_result, 3);

    let all_result = monitor
        .with_write_notify_all_async(|items| {
            items.push(4);
            items.len()
        })
        .await;
    assert_eq!(all_result, 4);

    monitor.notify_one();
    monitor.notify_all();

    let empty_monitor = TokioMonitor::current(Vec::<i32>::default());
    assert!(
        empty_monitor
            .with_read_async(|items| items.is_empty())
            .await
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_traits_delegate_to_monitor_methods() {
    let monitor = TokioMonitor::current(vec![1, 2]);

    <TokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <TokioMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    monitor.with_write_async(|items| items.clear()).await;
    let condition_wait =
        <TokioMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_while_async(
            &monitor,
            |items| items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(condition_wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut condition_wait)
            .await
            .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(2))
        .await;
    assert_eq!(condition_wait.await, 2);

    let timeout_condition_wait =
        <TokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            &monitor,
            Duration::from_secs(1),
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(timeout_condition_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut timeout_condition_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(1))
        .await;
    assert_time_result_eq!(
        timeout_condition_wait.await,
        Ok(WaitTimeoutResult::Ready(1)),
    );
}

/// Verifies that time before the first poll does not consume timeout budget.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_uses_condition_wait_budget() {
    let monitor = TokioMonitor::current(false);
    let wait = monitor.wait_while_for_async(
        Duration::from_millis(5),
        |ready| !*ready,
        |_| 7,
    );

    tokio::time::advance(Duration::from_millis(10)).await;

    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
        "an unpolled condition wait should retain its full budget",
    );

    tokio::time::advance(Duration::from_millis(4)).await;
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_time_result_eq!(wait.await, Ok(WaitTimeoutResult::TimedOut));
}

/// Verifies that initial mutex contention does not consume timeout budget.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_excludes_initial_lock_contention_from_timeout()
 {
    let monitor = Arc::new(TokioMonitor::current(false));
    let holder_monitor = Arc::clone(&monitor);
    let (holding_tx, holding_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let holder = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("holder runtime should build");
        runtime.block_on(holder_monitor.with_write_async(|_| {
            holding_tx
                .send(())
                .expect("test should observe the held state lock");
            release_rx
                .recv()
                .expect("holder should receive release permission");
        }));
    });
    holding_rx
        .recv()
        .expect("holder should acquire the state lock");

    let wait = monitor.wait_while_for_async(
        Duration::from_millis(5),
        |ready| !*ready,
        |_| (),
    );
    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    tokio::time::advance(Duration::from_millis(10)).await;

    release_tx
        .send(())
        .expect("holder should receive release permission");
    holder.join().expect("holder thread should finish");
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
        "initial state-lock contention should not consume the wait budget",
    );

    tokio::time::advance(Duration::from_millis(5)).await;
    assert_time_result_eq!(wait.await, Ok(WaitTimeoutResult::TimedOut));
}

/// Verifies that notifications reuse one fixed timeout deadline.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_reuses_fixed_timeout_deadline()
{
    let monitor = TokioMonitor::current(false);
    let wait = monitor.wait_while_for_async(
        Duration::from_millis(10),
        |ready| !*ready,
        |_| (),
    );
    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );

    for _ in 0..2 {
        tokio::time::advance(Duration::from_millis(4)).await;
        monitor.notify_one();
        assert!(
            std::future::poll_fn(|context| {
                Poll::Ready(wait.as_mut().poll(context))
            })
            .await
            .is_pending(),
        );
    }

    tokio::time::advance(Duration::from_millis(2)).await;
    assert_time_result_eq!(wait.await, Ok(WaitTimeoutResult::TimedOut));
}

/// Verifies that zero timeout evaluates the initial predicate exactly once.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_zero_timeout_checks_predicate_once()
 {
    let checks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let predicate_checks = Arc::clone(&checks);
    let monitor = TokioMonitor::current(false);

    let result = monitor
        .wait_while_for_async(
            Duration::ZERO,
            move |ready| {
                predicate_checks
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                !*ready
            },
            |_| (),
        )
        .await;

    assert_time_result_eq!(result, Ok(WaitTimeoutResult::TimedOut));
    assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// Verifies that a condition wait runs its action after notification.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_until_runs_action_after_notify() {
    let monitor = TokioMonitor::current(false);
    let mut waiter = Box::pin(monitor.wait_until_async(
        |ready| *ready,
        |ready| {
            *ready = false;
            7
        },
    ));
    let wake_counter = Arc::new(WakeCounter::default());

    assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
    monitor
        .with_write_notify_one_async(|ready| *ready = true)
        .await;

    assert_eq!(waiter.await, 7);
    assert!(!monitor.with_read_async(|ready| *ready).await);
}

/// Verifies that notification returns a ready timed condition-wait result.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_returns_ready_after_notify() {
    let monitor = TokioMonitor::current(false);
    let mut waiter = Box::pin(monitor.wait_while_for_async(
        Duration::from_secs(1),
        |ready| !*ready,
        |ready| {
            *ready = false;
            9
        },
    ));
    let wake_counter = Arc::new(WakeCounter::default());

    assert!(poll_once(waiter.as_mut(), &wake_counter).is_pending());
    monitor.with_write_async(|ready| *ready = true).await;
    monitor.notify_one();

    assert_time_result_eq!(waiter.await, Ok(WaitTimeoutResult::Ready(9)),);
}

/// Verifies that an unnotified ready predicate wins the final deadline check.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_rechecks_state_after_timeout()
{
    let monitor = TokioMonitor::current(false);
    let wait = monitor.wait_while_for_async(
        Duration::from_millis(20),
        |ready| !*ready,
        |ready| {
            *ready = false;
            9
        },
    );
    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
        "initial blocking check should register the deadline timer",
    );

    monitor.with_write_async(|ready| *ready = true).await;
    tokio::time::advance(Duration::from_millis(20)).await;

    assert_time_result_eq!(wait.await, Ok(WaitTimeoutResult::Ready(9)),);
}
