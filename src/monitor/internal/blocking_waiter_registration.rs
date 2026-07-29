// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Owns one active blocking Monitor waiter registration.

use std::sync::Arc;

use super::{
    BlockingConditionWaiter,
    BlockingWaiterRegistry,
};

/// RAII ownership of one active blocking waiter registration.
///
/// # Type Parameters
///
/// * `'a` - Lifetime of the registry that owns the active entry.
#[must_use = "retain the registration while the waiter remains eligible for notification"]
pub(in crate::monitor) struct BlockingWaiterRegistration<'a> {
    /// Registry from which cancellation removes the waiter.
    registry: &'a BlockingWaiterRegistry,
    /// Stable registry identifier.
    waiter_id: u64,
    /// Waiter shared with notifications and Timer Wakers.
    waiter: Arc<BlockingConditionWaiter>,
}

impl<'a> BlockingWaiterRegistration<'a> {
    /// Creates ownership of an already inserted registry entry.
    ///
    /// # Parameters
    ///
    /// * `registry` - Registry that currently owns the active entry.
    /// * `waiter_id` - Stable identifier returned when the waiter was
    ///   registered.
    /// * `waiter` - Waiter stored under `waiter_id`.
    ///
    /// # Returns
    ///
    /// A guard that unregisters the entry when dropped.
    #[inline]
    pub(super) const fn new(
        registry: &'a BlockingWaiterRegistry,
        waiter_id: u64,
        waiter: Arc<BlockingConditionWaiter>,
    ) -> Self {
        Self {
            registry,
            waiter_id,
            waiter,
        }
    }

    /// Returns the registered waiter used to block or poll a Timer.
    ///
    /// # Returns
    ///
    /// A shared reference to this registration's waiter allocation.
    #[must_use]
    #[inline(always)]
    pub(in crate::monitor) const fn waiter(
        &self,
    ) -> &Arc<BlockingConditionWaiter> {
        &self.waiter
    }
}

impl Drop for BlockingWaiterRegistration<'_> {
    /// Cancels an active registration without transferring notification.
    #[inline(always)]
    fn drop(&mut self) {
        self.registry.unregister(self.waiter_id);
    }
}
