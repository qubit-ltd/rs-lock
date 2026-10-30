// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared fault-injecting Timer factories and assertions.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_clock::ClockDomain;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_clock::TimerUnavailableError;
use qubit_clock::test_util::FaultInjectingTimer;
use qubit_clock::test_util::TimerFailurePoint;

/// Clock wrapper that reports when an operation fixes a relative deadline.
struct DeadlineSignalingClock<C> {
    /// Wrapped clock providing the real monotonic time behavior.
    inner: C,
    /// One-shot signal emitted by the first clock sample.
    sampled_tx: Mutex<Option<Sender<()>>>,
}

impl<C> DeadlineSignalingClock<C> {
    /// Creates a clock wrapper that signals the supplied channel on its first
    /// relative deadline calculation.
    ///
    /// # Parameters
    ///
    /// * `inner` - Clock providing the real deadline behavior.
    /// * `sampled_tx` - Channel notified when the wrapper fixes a deadline.
    ///
    /// # Returns
    ///
    /// A clock wrapper retaining the one-shot sampling signal.
    fn new(inner: C, sampled_tx: Sender<()>) -> Self {
        Self {
            inner,
            sampled_tx: Mutex::new(Some(sampled_tx)),
        }
    }
}

impl<C> MonotonicClock for DeadlineSignalingClock<C>
where
    C: MonotonicClock,
{
    /// Returns the wrapped clock's stable monotonic domain identity.
    fn domain(&self) -> ClockDomain {
        self.inner.domain()
    }

    /// Returns the wrapped clock's current monotonic instant.
    fn now(&self) -> MonotonicInstant {
        self.inner.now()
    }

    /// Reports the first completed relative deadline calculation.
    fn deadline_after(&self, duration: Duration) -> Result<MonotonicInstant, TimeError> {
        let deadline = self.inner.deadline_after(duration)?;
        if let Some(sampled_tx) = self
            .sampled_tx
            .lock()
            .expect("deadline signal lock should not be poisoned")
            .take()
        {
            sampled_tx
                .send(())
                .expect("deadline sampling receiver should remain connected");
        }
        Ok(deadline)
    }

    /// Creates a timer in the wrapped clock's monotonic domain.
    fn new_timer(&self) -> Arc<dyn Timer> {
        self.inner.new_timer()
    }
}

/// Timer wrapper that exposes a deadline-signaling clock.
pub(super) struct DeadlineSignalingTimer<T, C> {
    /// Wrapped timer providing deadline futures.
    inner: T,
    /// Clock wrapper that reports relative deadline calculation.
    clock: DeadlineSignalingClock<C>,
}

impl<T, C> DeadlineSignalingTimer<T, C> {
    /// Creates a timer wrapper with a clock that signals the supplied channel.
    ///
    /// # Parameters
    ///
    /// * `inner` - Timer providing the real deadline behavior.
    /// * `clock` - Clock from the same domain as `inner`.
    /// * `sampled_tx` - Channel notified when the clock fixes a deadline.
    ///
    /// # Returns
    ///
    /// A timer wrapper retaining the clock sampling signal.
    pub(super) fn new(inner: T, clock: C, sampled_tx: Sender<()>) -> Self {
        Self {
            inner,
            clock: DeadlineSignalingClock::new(clock, sampled_tx),
        }
    }
}

impl<T, C> Timer for DeadlineSignalingTimer<T, C>
where
    T: Timer,
    C: MonotonicClock,
{
    /// Returns the deadline-signaling monotonic clock.
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    /// Registers the deadline with the wrapped timer.
    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        self.inner.at(deadline)
    }
}

/// Timer wrapper that holds completion until its test gate opens.
pub(super) struct GatedCompletionTimer<T> {
    /// Wrapped timer providing the eventual result.
    inner: T,
    /// Gate state shared with the test controller.
    state: Arc<Mutex<CompletionState>>,
}

/// Controller for opening a gated Timer completion.
#[derive(Clone)]
pub(super) struct CompletionGate {
    state: Arc<Mutex<CompletionState>>,
}

/// State shared by the gated Timer and its controller.
struct CompletionState {
    open: bool,
    waker: Option<Waker>,
    first_poll: Option<Sender<()>>,
}

impl<T> GatedCompletionTimer<T> {
    /// Creates a closed gate and a one-shot first-poll observation channel.
    pub(super) fn new(inner: T) -> (Self, CompletionGate, Receiver<()>) {
        let (first_poll_tx, first_poll_rx) = std::sync::mpsc::channel();
        let state = Arc::new(Mutex::new(CompletionState {
            open: false,
            waker: None,
            first_poll: Some(first_poll_tx),
        }));
        (
            Self {
                inner,
                state: Arc::clone(&state),
            },
            CompletionGate { state },
            first_poll_rx,
        )
    }
}

impl CompletionGate {
    /// Opens completion and wakes the most recently registered poller.
    pub(super) fn open(&self) {
        let waker = {
            let mut state = self.state.lock().expect("completion gate should not poison");
            state.open = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Timer for GatedCompletionTimer<T>
where
    T: Timer,
{
    /// Returns the wrapped timer's monotonic clock.
    fn clock(&self) -> &dyn MonotonicClock {
        self.inner.clock()
    }

    /// Defers completion until the shared gate opens.
    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        let mut future = self.inner.at(deadline)?;
        let state = Arc::clone(&self.state);
        Ok(Box::pin(poll_fn(move |context| {
            let (open, first_poll) = {
                let mut state = state.lock().expect("completion gate should not poison");
                if !state.open {
                    state.waker = Some(context.waker().clone());
                }
                (state.open, state.first_poll.take())
            };
            if let Some(first_poll) = first_poll {
                let _ = first_poll.send(());
            }
            if open {
                future.as_mut().poll(context)
            } else {
                Poll::Pending
            }
        })))
    }
}

/// Creates a Timer that rejects every future-deadline registration.
///
/// # Returns
///
/// A fault-injecting Timer reporting backend unavailability at registration.
pub(super) fn registration_failing_timer() -> FaultInjectingTimer {
    FaultInjectingTimer::backend_unavailable(
        TimerFailurePoint::Registration,
        "test",
        "test timer backend unavailable",
    )
}

/// Creates a Timer whose registered futures fail on completion.
///
/// # Returns
///
/// A fault-injecting Timer reporting backend unavailability at completion.
pub(super) fn completion_failing_timer() -> FaultInjectingTimer {
    FaultInjectingTimer::backend_unavailable(TimerFailurePoint::Completion, "test", "test timer backend unavailable")
}

/// Verifies the stable category and source of the failing test Timer.
///
/// # Parameters
///
/// * `error` - Error propagated from a timed monitor operation.
pub(super) fn assert_backend_unavailable(error: TimeError) {
    let TimeError::TimerUnavailable {
        source: TimerUnavailableError::BackendUnavailable { backend, source },
    } = error
    else {
        panic!("failing Timer should report backend unavailability");
    };
    assert_eq!("test", backend);
    assert_eq!("test timer backend unavailable", source.to_string());
}

#[cfg(test)]
mod gated_timer_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use qubit_clock::Timer;

    use super::GatedCompletionTimer;
    use super::assert_backend_unavailable;
    use super::completion_failing_timer;

    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn future(timer: &impl Timer) -> qubit_clock::TimerFuture {
        let deadline = timer.clock().now().checked_add(Duration::from_secs(1)).unwrap();
        timer.at(deadline).unwrap()
    }

    #[test]
    fn test_gated_completion_timer_pending_then_wakes_and_errors() {
        let (timer, gate, first_poll_rx) = GatedCompletionTimer::new(completion_failing_timer());
        let mut future = future(&timer);
        let counter = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        first_poll_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        assert!(first_poll_rx.try_recv().is_err());

        gate.open();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(Err(error)) = future.as_mut().poll(&mut context) else {
            panic!("opened gate should expose completion error");
        };
        assert_backend_unavailable(error);
    }

    #[test]
    fn test_gated_completion_timer_open_before_first_poll() {
        let (timer, gate, first_poll_rx) = GatedCompletionTimer::new(completion_failing_timer());
        gate.open();
        let mut future = future(&timer);
        let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
        let mut context = Context::from_waker(&waker);

        let Poll::Ready(Err(error)) = future.as_mut().poll(&mut context) else {
            panic!("pre-opened gate should expose completion error on first poll");
        };
        assert_backend_unavailable(error);
        first_poll_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}
