/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Wait Timeout Status
//!
//! Provides the status returned by one timed condition-variable wait.
//!

/// Result of a timed wait operation.
///
/// This status is returned by
/// [`ParkingLotMonitorGuard::wait_timeout`](super::ParkingLotMonitorGuard::wait_timeout) and
/// [`ParkingLotMonitor::wait_for`](super::ParkingLotMonitor::wait_for). It describes why a
/// timed wait returned, but callers must still re-check the protected state
/// because condition variables may wake spuriously.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_lock::{ParkingLotMonitor, WaitTimeoutStatus};
///
/// let monitor = ParkingLotMonitor::new(false);
/// let guard = monitor.lock();
/// let (_guard, status) = guard.wait_timeout(Duration::from_millis(1));
/// assert_eq!(status, WaitTimeoutStatus::TimedOut);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutStatus {
    /// The wait returned before the timeout elapsed.
    ///
    /// This usually means another thread called
    /// [`ParkingLotMonitor::notify_one`](super::ParkingLotMonitor::notify_one) or
    /// [`ParkingLotMonitor::notify_all`](super::ParkingLotMonitor::notify_all), but it may also be
    /// a spurious wakeup. Always re-check the guarded state before acting on
    /// this status.
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
    #[inline]
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
    #[inline]
    pub const fn is_timed_out(&self) -> bool {
        match self {
            Self::Woken => false,
            Self::TimedOut => true,
        }
    }
}
