/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Asynchronous timeout notification-wait capability.

use std::time::Duration;

use crate::monitor::{AsyncMonitorFuture, WaitTimeoutStatus};

/// Waits asynchronously for a notification with a relative timeout.
pub trait AsyncTimeoutNotificationWaiter {
    /// Returns a future that resolves after notification or timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum relative duration to wait, measured from this
    ///   method call.
    ///
    /// # Returns
    ///
    /// A future resolving to the timeout status.
    fn async_wait_for<'a>(&'a self, timeout: Duration)
    -> AsyncMonitorFuture<'a, WaitTimeoutStatus>;
}
