// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Wait Timeout Status
//!
//! Provides the status returned by one timed monitor wait.

/// Result of a timed wait operation.
///
/// This status is returned by
/// [`StdMonitorGuard::wait_for`](super::StdMonitorGuard::wait_for). It
/// describes why a timed wait returned, but callers must still re-check the
/// protected state because notification does not imply predicate truth.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_lock::{StdMonitor, WaitTimeoutStatus};
///
/// let monitor = StdMonitor::new(false);
/// let mut guard = monitor.lock();
/// let status = guard
///     .wait_for(Duration::from_millis(1))
///     .expect("standard Timer should register");
/// assert_eq!(status, WaitTimeoutStatus::TimedOut);
/// ```
///
/// Ignoring the status returned by a guard wait is rejected when unused
/// must-use values are denied:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
///
/// use qubit_lock::WaitTimeoutStatus;
///
/// WaitTimeoutStatus::TimedOut;
/// ```
#[must_use = "check whether the condition wait woke or reached its timeout"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutStatus {
    /// The wait returned before the timeout elapsed.
    ///
    /// This usually means another thread called
    /// [`StdMonitor::notify_one`](super::StdMonitor::notify_one)
    /// or
    /// [`StdMonitor::notify_all`](super::StdMonitor::notify_all),
    /// but it may also be a spurious wakeup. Always re-check the guarded
    /// state before acting on this status.
    Woken,
    /// The wait reached the timeout boundary.
    ///
    /// Even after this status, callers should inspect the protected state
    /// because another thread may have changed it while the waiting thread was
    /// reacquiring the mutex.
    TimedOut,
}

impl WaitTimeoutStatus {
    /// Returns `true` when the wait returned before the timeout elapsed.
    ///
    /// # Returns
    ///
    /// `true` for [`Self::Woken`], otherwise `false`.
    #[inline(always)]
    #[must_use]
    pub const fn is_woken(&self) -> bool {
        match self {
            Self::Woken => true,
            Self::TimedOut => false,
        }
    }

    /// Returns `true` when the wait reached the timeout boundary.
    ///
    /// # Returns
    ///
    /// `true` for [`Self::TimedOut`], otherwise `false`.
    #[inline(always)]
    #[must_use]
    pub const fn is_timed_out(&self) -> bool {
        match self {
            Self::Woken => false,
            Self::TimedOut => true,
        }
    }
}
