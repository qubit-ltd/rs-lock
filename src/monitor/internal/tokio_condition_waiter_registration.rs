// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe registration for a Tokio condition waiter.

use std::sync::{
    Arc,
    Mutex,
};

use super::TokioConditionWaiter;

/// Removes an active waiter registration on cancellation or normal exit.
pub(in crate::monitor) struct TokioConditionWaiterRegistration<'a> {
    /// Registry containing this waiter while it remains selectable.
    registry: &'a Mutex<Vec<Arc<TokioConditionWaiter>>>,
    /// Independently signalled waiter owned by the pending condition wait.
    waiter: Arc<TokioConditionWaiter>,
}

impl<'a> TokioConditionWaiterRegistration<'a> {
    /// Creates a registration for a waiter already stored in `registry`.
    ///
    /// # Arguments
    ///
    /// * `registry` - Registry that currently contains `waiter`.
    /// * `waiter` - Independently signalled waiter owned by the pending wait.
    ///
    /// # Returns
    ///
    /// A guard that unregisters `waiter` when dropped.
    #[inline]
    pub(in crate::monitor) fn new(
        registry: &'a Mutex<Vec<Arc<TokioConditionWaiter>>>,
        waiter: Arc<TokioConditionWaiter>,
    ) -> Self {
        Self { registry, waiter }
    }

    /// Returns the waiter owned by this registration.
    ///
    /// # Returns
    ///
    /// The registered waiter selected by monitor notifications.
    #[inline(always)]
    pub(in crate::monitor) fn waiter(&self) -> &TokioConditionWaiter {
        &self.waiter
    }
}

impl Drop for TokioConditionWaiterRegistration<'_> {
    /// Removes this waiter if no notification has selected it yet.
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
    }
}
