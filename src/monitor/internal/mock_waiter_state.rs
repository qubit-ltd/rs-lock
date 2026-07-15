// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Notification ownership state for one mock-monitor waiter.

/// Notification state assigned to one active mock-monitor waiter.
pub(in crate::monitor) struct MockWaiterState {
    /// Whether this waiter owns an unconsumed notification.
    pub(in crate::monitor) notified: bool,
}

impl MockWaiterState {
    /// Creates waiter state without an assigned notification.
    ///
    /// # Returns
    ///
    /// A waiter state ready to receive a notification.
    #[inline]
    pub(in crate::monitor) fn new() -> Self {
        Self { notified: false }
    }
}
