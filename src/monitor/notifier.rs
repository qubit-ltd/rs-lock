/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Notification capability for monitor-style synchronization.

/// Sends notification signals to waiters.
///
/// Notifications are coordination signals. They do not carry state by
/// themselves, so condition waiters should always recheck the protected state
/// after waking.
pub trait Notifier {
    /// Wakes one waiter if one is currently blocked.
    fn notify_one(&self);

    /// Wakes all current waiters.
    fn notify_all(&self);
}
