/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Wait Timeout Status
//!
//! Provides the status returned by one timed condition-variable wait.
//!
//! # Author
//!
//! Haixing Hu

/// Result of a timed wait operation.
///
/// This status is returned by
/// [`MonitorGuard::wait_timeout`](super::MonitorGuard::wait_timeout) and
/// [`Monitor::wait_notify`](super::Monitor::wait_notify). It describes why a
/// timed wait returned, but callers must still re-check the protected state
/// because condition variables may wake spuriously.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
///
/// use qubit_lock::lock::{Monitor, WaitTimeoutStatus};
///
/// let monitor = Monitor::new(false);
/// let guard = monitor.lock();
/// let (_guard, status) = guard.wait_timeout(Duration::from_millis(1));
/// assert_eq!(status, WaitTimeoutStatus::TimedOut);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutStatus {
    /// The wait returned before the timeout elapsed.
    ///
    /// This usually means another thread called
    /// [`Monitor::notify_one`](super::Monitor::notify_one) or
    /// [`Monitor::notify_all`](super::Monitor::notify_all), but it may also be
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
