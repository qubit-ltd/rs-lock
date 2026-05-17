/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Asynchronous notification-wait capability.

use crate::monitor::AsyncMonitorFuture;

/// Waits asynchronously until a notification is observed.
pub trait AsyncNotificationWaiter {
    /// Returns a future that resolves after a notification wakes this waiter.
    fn async_wait<'a>(&'a self) -> AsyncMonitorFuture<'a, ()>;
}
