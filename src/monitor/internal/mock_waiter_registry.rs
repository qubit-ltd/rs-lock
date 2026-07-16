// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Waiter registrations owned by the mock monitor.

use std::collections::BTreeMap;

use super::MockWaiterState;

/// Registry protected independently from the user-visible monitor state.
pub(in crate::monitor) struct MockWaiterRegistry {
    /// Identifier assigned to the next registered waiter.
    pub(in crate::monitor) next_waiter_id: u64,
    /// Registered waiters and their individually assigned notification state.
    pub(in crate::monitor) waiters: BTreeMap<u64, MockWaiterState>,
    /// Number of active blocking and asynchronous timeout waits.
    pub(in crate::monitor) timeout_waiters: usize,
}

impl MockWaiterRegistry {
    /// Creates an empty waiter registry.
    ///
    /// # Returns
    ///
    /// A registry with an initial waiter identifier of zero.
    #[inline]
    pub(in crate::monitor) fn new() -> Self {
        Self {
            next_waiter_id: 0,
            waiters: BTreeMap::new(),
            timeout_waiters: 0,
        }
    }
}
