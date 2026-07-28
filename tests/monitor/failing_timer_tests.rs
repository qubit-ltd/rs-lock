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
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    task::Poll,
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
