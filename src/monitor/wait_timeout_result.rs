/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Wait Timeout Result
//!
//! Provides the result returned by predicate-based timed monitor waits.
//!

/// Result of waiting for a predicate with an overall timeout.
///
/// This type is returned by
/// [`Monitor::wait_timeout_while`](super::Monitor::wait_timeout_while) and
/// [`Monitor::wait_timeout_until`](super::Monitor::wait_timeout_until). It is
/// more explicit than `Option<R>`: a ready predicate produces [`Self::Ready`],
/// while an expired timeout produces [`Self::TimedOut`].
///
/// # Type Parameters
///
/// * `R` - The value produced after the protected state satisfies the
///   predicate.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_lock::monitor::{Monitor, WaitTimeoutResult};
///
/// let monitor = Monitor::new(true);
/// let result = monitor.wait_timeout_until(
///     Duration::from_secs(1),
///     |ready| *ready,
///     |ready| {
///         *ready = false;
///         "ready"
///     },
/// );
///
/// assert_eq!(result, WaitTimeoutResult::Ready("ready"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutResult<R> {
    /// The predicate became ready before the timeout and produced this value.
    Ready(R),
    /// The timeout elapsed before the predicate became ready.
    TimedOut,
}
