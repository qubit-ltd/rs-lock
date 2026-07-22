// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared fault-injecting Timer factories and assertions.

use qubit_clock::{
    TimeError,
    TimerUnavailableError,
    test_util::{
        FaultInjectingTimer,
        TimerFailurePoint,
    },
};

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
