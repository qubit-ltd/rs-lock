// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exposes the production waiter registry to external Loom models.

use loom::sync::Mutex;

use crate::monitor::internal::WaiterRegistry;

/// Loom-facing, mutex-protected adapter around the production waiter registry.
pub struct LoomWaiterRegistry {
    /// Production registry serialized by the model's Loom mutex.
    inner: Mutex<WaiterRegistry<usize>>,
}

impl LoomWaiterRegistry {
    /// Creates an empty waiter registry.
    ///
    /// # Returns
    ///
    /// A model adapter containing the production registry.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(WaiterRegistry::new()),
        }
    }

    /// Registers `waiter` and returns its cancellation identifier.
    ///
    /// # Parameters
    ///
    /// * `waiter` - Model value made eligible for notification.
    ///
    /// # Returns
    ///
    /// The stable identifier required to cancel `waiter`.
    #[must_use]
    #[inline]
    pub fn register(&self, waiter: usize) -> u64 {
        self.inner.lock().unwrap().register(waiter)
    }

    /// Selects and removes the longest-waiting registered waiter.
    ///
    /// # Returns
    ///
    /// The selected waiter, or `None` when no waiter was active.
    #[inline]
    pub fn take_one(&self) -> Option<usize> {
        self.inner.lock().unwrap().take_one()
    }

    /// Selects and removes every registered waiter in FIFO order.
    ///
    /// # Returns
    ///
    /// All waiters active when selection began.
    #[inline]
    pub fn take_all(&self) -> Vec<usize> {
        self.inner.lock().unwrap().take_all()
    }

    /// Cancels the registration identified by `waiter_id`.
    ///
    /// # Parameters
    ///
    /// * `waiter_id` - Identifier returned by [`Self::register`].
    ///
    /// # Returns
    ///
    /// The cancelled waiter, or `None` when selection already removed it.
    #[inline]
    pub fn unregister(&self, waiter_id: u64) -> Option<usize> {
        self.inner.lock().unwrap().unregister(waiter_id)
    }
}
