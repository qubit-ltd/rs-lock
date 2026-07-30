// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared fault-injecting Timer factories and assertions.

use std::{
    future::poll_fn,
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
        mpsc::Sender,
    },
    task::Poll,
    time::Duration,
};

use qubit_clock::{
    MonotonicClock,
    MonotonicInstant,
    TimeError,
    Timer,
    TimerFuture,
    TimerUnavailableError,
    test_util::{
        FaultInjectingTimer,
        TimerFailurePoint,
    },
};

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
    fn domain(&self) -> qubit_clock::ClockDomain {
        self.inner.domain()
    }

    /// Returns the wrapped clock's current monotonic instant.
    fn now(&self) -> MonotonicInstant {
        self.inner.now()
    }

    /// Reports the first completed relative deadline calculation.
    fn deadline_after(
        &self,
        duration: Duration,
    ) -> Result<MonotonicInstant, TimeError> {
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

/// Timer wrapper that remains pending once before forwarding completion.
pub(super) struct OncePendingTimer<T> {
    /// Wrapped timer providing the eventual result.
    inner: T,
    /// Number of polls observed across registered futures.
    poll_count: Arc<AtomicUsize>,
}

impl<T> OncePendingTimer<T> {
    /// Creates a wrapper and exposes its shared poll counter.
    pub(super) fn new(inner: T) -> (Self, Arc<AtomicUsize>) {
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

impl<T> Timer for OncePendingTimer<T>
where
    T: Timer,
{
    /// Returns the wrapped timer's monotonic clock.
    fn clock(&self) -> &dyn MonotonicClock {
        self.inner.clock()
    }

    /// Defers the wrapped future's first completion poll.
    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        let mut future = self.inner.at(deadline)?;
        let poll_count = Arc::clone(&self.poll_count);
        Ok(Box::pin(poll_fn(move |context| {
            if poll_count.fetch_add(1, Ordering::SeqCst) == 0 {
                Poll::Pending
            } else {
                future.as_mut().poll(context)
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
    FaultInjectingTimer::backend_unavailable(
        TimerFailurePoint::Completion,
        "test",
        "test timer backend unavailable",
    )
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
