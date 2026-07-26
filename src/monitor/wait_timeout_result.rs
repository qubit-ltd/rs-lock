// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Wait Timeout Result
//!
//! Provides the result returned by predicate-based timed monitor waits.

/// Result of waiting for a predicate with an overall timeout.
///
/// This type is returned by
/// [`StdMonitor::wait_while_for`](super::StdMonitor::wait_while_for) and
/// [`StdMonitor::wait_until_for`](super::StdMonitor::wait_until_for). It is
/// more explicit than `Option<R>`: a ready predicate produces [`Self::Ready`],
/// while an expired timeout produces [`Self::TimedOut`].
///
/// # Type Parameters
///
/// * `R` - The value produced after the protected state satisfies the
///   predicate.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_lock::{StdMonitor, WaitTimeoutResult};
///
/// let monitor = StdMonitor::new(true);
/// let result = monitor.wait_until_for(
///     Duration::from_secs(1),
///     |ready| *ready,
///     |ready| {
///         *ready = false;
///         "ready"
///     },
/// );
///
/// let outcome = result.expect("timer registration should succeed");
/// assert_eq!(outcome, WaitTimeoutResult::Ready("ready"));
/// ```
#[must_use = "check whether the predicate became ready or the wait timed out"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutResult<R> {
    /// The predicate became ready before the timeout and produced this value.
    Ready(R),
    /// The timeout elapsed before the predicate became ready.
    TimedOut,
}

impl<R> WaitTimeoutResult<R> {
    /// Returns `true` when the result contains a ready value.
    ///
    /// # Returns
    ///
    /// `true` for [`Self::Ready`], otherwise `false`.
    #[inline(always)]
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        match self {
            Self::Ready(_) => true,
            Self::TimedOut => false,
        }
    }

    /// Returns `true` when the timeout elapsed before the predicate was ready.
    ///
    /// # Returns
    ///
    /// `true` for [`Self::TimedOut`], otherwise `false`.
    #[inline(always)]
    #[must_use]
    pub const fn is_timed_out(&self) -> bool {
        match self {
            Self::Ready(_) => false,
            Self::TimedOut => true,
        }
    }

    /// Converts this result into an [`Option`].
    ///
    /// # Returns
    ///
    /// `Some(value)` for [`Self::Ready`], or `None` for [`Self::TimedOut`].
    #[inline]
    pub fn into_option(self) -> Option<R> {
        match self {
            Self::Ready(value) => Some(value),
            Self::TimedOut => None,
        }
    }

    /// Maps a ready value while preserving timeout status.
    ///
    /// # Parameters
    ///
    /// * `f` - Closure applied to the contained value when this result is
    ///   [`Self::Ready`].
    ///
    /// # Returns
    ///
    /// [`Self::Ready`] containing the mapped value, or
    /// [`WaitTimeoutResult::TimedOut`] when this result timed out.
    #[inline]
    pub fn map<U, F: FnOnce(R) -> U>(self, f: F) -> WaitTimeoutResult<U> {
        match self {
            Self::Ready(value) => WaitTimeoutResult::Ready(f(value)),
            Self::TimedOut => WaitTimeoutResult::TimedOut,
        }
    }
}
