/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Blocking notification-wait capability.

/// Waits until a notification is observed.
///
/// This trait models a blocking, memoryless notification wait. A notification
/// sent before `wait` starts is not remembered for future waiters.
pub trait NotificationWaiter {
    /// Blocks the current thread until a notification wakes this waiter.
    fn wait(&self);
}
