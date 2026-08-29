// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independently signalled waiter used by the Tokio monitor.

use tokio::sync::Notify;

/// One independently signalled Tokio condition waiter.
pub(in crate::monitor) struct TokioConditionWaiter {
    /// Private signal that cannot transfer a selection to another waiter.
    signal: Notify,
}

impl TokioConditionWaiter {
    /// Creates an unsignalled condition waiter.
    ///
    /// # Returns
    ///
    /// A waiter with no retained notification.
    #[must_use]
    #[inline]
    pub(in crate::monitor) fn new() -> Self {
        Self { signal: Notify::new() }
    }

    /// Returns the notification primitive owned by this waiter.
    ///
    /// # Returns
    ///
    /// The private signal used to select this waiter.
    #[must_use]
    #[inline(always)]
    pub(in crate::monitor) fn signal(&self) -> &Notify {
        &self.signal
    }
}
