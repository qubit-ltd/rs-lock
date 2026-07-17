// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Test Timer that rejects every deadline registration.

use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
    MonotonicInstant,
    TimeError,
    Timer,
    TimerFuture,
};

/// Timer used to verify synchronous registration-error propagation.
pub(super) struct FailingTimer {
    /// Clock defining the Timer's otherwise valid domain.
    clock: ManualMonotonicClock,
}

impl FailingTimer {
    /// Creates a Timer that reports [`TimeError::TimerUnavailable`].
    ///
    /// # Returns
    ///
    /// A failing Timer with its own monotonic domain.
    pub(super) fn new() -> Self {
        Self {
            clock: ManualMonotonicClock::new(),
        }
    }
}

impl Timer for FailingTimer {
    /// Returns this Timer's manual monotonic clock.
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    /// Rejects every deadline registration.
    fn at(
        &self,
        _deadline: MonotonicInstant,
    ) -> Result<TimerFuture, TimeError> {
        Err(TimeError::TimerUnavailable)
    }
}
