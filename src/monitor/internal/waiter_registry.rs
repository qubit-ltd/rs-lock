// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Maintains FIFO selection for active Monitor waiters.

use std::collections::BTreeMap;

/// Mutable registry of waiters eligible for one memoryless notification.
///
/// Registration identifiers are strictly increasing and never reused, so the
/// smallest active key in the [`BTreeMap`] is the oldest active registration.
/// This only determines notification selection; it does not guarantee
/// scheduling or mutex reacquisition order.
pub(crate) struct WaiterRegistry<W> {
    /// Next nonzero registration identifier.
    next_waiter_id: u64,
    /// Waiters indexed by stable registration ID and FIFO order.
    waiters: BTreeMap<u64, W>,
}

impl<W> WaiterRegistry<W> {
    /// Creates an empty registry.
    ///
    /// # Returns
    ///
    /// A registry with no active waiters.
    #[must_use]
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            next_waiter_id: 1,
            waiters: BTreeMap::new(),
        }
    }

    /// Registers `waiter` and returns its stable cancellation identifier.
    ///
    /// # Parameters
    ///
    /// * `waiter` - Waiter made eligible for a future notification.
    ///
    /// # Returns
    ///
    /// The nonzero identifier required to unregister `waiter`.
    ///
    /// # Panics
    ///
    /// Panics when the registration identifier space is exhausted or an
    /// internal uniqueness invariant is violated.
    #[must_use = "the identifier is required to unregister the waiter"]
    #[inline]
    pub(crate) fn register(&mut self, waiter: W) -> u64 {
        let waiter_id = self.next_waiter_id;
        self.next_waiter_id = waiter_id
            .checked_add(1)
            .expect("monitor waiter identifiers exhausted");
        let previous = self.waiters.insert(waiter_id, waiter);
        assert!(
            previous.is_none(),
            "monitor waiter identifier must be unique"
        );
        waiter_id
    }

    /// Removes and returns the longest-waiting active waiter.
    ///
    /// # Returns
    ///
    /// The selected waiter, or `None` when no waiter is active.
    #[inline]
    pub(crate) fn take_one(&mut self) -> Option<W> {
        self.waiters.pop_first().map(|(_, waiter)| waiter)
    }

    /// Removes and returns every active waiter in FIFO order.
    ///
    /// # Returns
    ///
    /// All waiters active when this method was called.
    #[inline]
    pub(crate) fn take_all(&mut self) -> Vec<W> {
        std::mem::take(&mut self.waiters).into_values().collect()
    }

    /// Removes the waiter registered with `waiter_id`.
    ///
    /// # Parameters
    ///
    /// * `waiter_id` - Stable identifier returned by [`Self::register`].
    ///
    /// # Returns
    ///
    /// The removed waiter, or `None` if notification or cancellation already
    /// removed it.
    #[inline]
    pub(crate) fn unregister(&mut self, waiter_id: u64) -> Option<W> {
        self.waiters.remove(&waiter_id)
    }
}
