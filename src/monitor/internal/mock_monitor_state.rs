// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Protected state owned by the mock monitor.

use std::collections::BTreeMap;

use super::MockWaiterState;

/// State protected by the mock monitor.
pub(in crate::monitor) struct MockMonitorState<T> {
    /// User-visible protected value.
    pub(in crate::monitor) value: T,
    /// Identifier assigned to the next registered waiter.
    pub(in crate::monitor) next_waiter_id: u64,
    /// Registered waiters and their individually assigned notification state.
    pub(in crate::monitor) waiters: BTreeMap<u64, MockWaiterState>,
    /// Number of active blocking and asynchronous timeout waits.
    pub(in crate::monitor) timeout_waiters: usize,
}

impl<T> MockMonitorState<T> {
    /// Creates protected mock-monitor state around a user value.
    ///
    /// # Arguments
    ///
    /// * `value` - Initial user-visible value.
    ///
    /// # Returns
    ///
    /// State with no registered waiters and an initial waiter identifier of
    /// zero.
    #[inline]
    pub(in crate::monitor) fn new(value: T) -> Self {
        Self {
            value,
            next_waiter_id: 0,
            waiters: BTreeMap::new(),
            timeout_waiters: 0,
        }
    }
}
