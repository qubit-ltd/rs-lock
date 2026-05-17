/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Blocking timeout notification-wait capability.

use std::time::Duration;

use crate::monitor::WaitTimeoutStatus;

/// Waits for a notification with a relative timeout.
pub trait TimeoutNotificationWaiter {
    /// Blocks until a notification is observed or the timeout expires.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum relative duration to wait.
    ///
    /// # Returns
    ///
    /// A status describing whether the wait returned before the timeout.
    fn wait_for(&self, timeout: Duration) -> WaitTimeoutStatus;
}
