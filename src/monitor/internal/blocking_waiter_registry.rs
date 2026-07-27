// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stores active blocking Monitor waiters with memoryless notification.

use std::sync::Arc;
use std::task::Wake;

use super::{
    BlockingConditionWaiter, BlockingWaiterRegistration, WaiterRegistry,
    sync::{Mutex, recover},
};

/// Registry of blocking waiters eligible for current notifications.
pub(in crate::monitor) struct BlockingWaiterRegistry {
    /// Active waiters with FIFO notification selection.
    waiters: Mutex<WaiterRegistry<Arc<BlockingConditionWaiter>>>,
}

impl BlockingWaiterRegistry {
    /// Creates an empty waiter registry.
    ///
    /// # Returns
    ///
    /// A registry containing no notification permits or waiters.
    #[must_use]
    #[inline]
    pub(in crate::monitor) fn new() -> Self {
        Self {
            waiters: Mutex::new(WaiterRegistry::new()),
        }
    }

    /// Registers one waiter while its Monitor state lock is still held.
    ///
    /// # Returns
    ///
    /// An RAII registration that removes an unselected waiter on drop.
    ///
    /// # Panics
    ///
    /// Panics if the registry exhausts registration identifiers.
    pub(in crate::monitor) fn register(&self) -> BlockingWaiterRegistration<'_> {
        let waiter = Arc::new(BlockingConditionWaiter::new());
        let waiter_id = recover(self.waiters.lock()).register(Arc::clone(&waiter));
        BlockingWaiterRegistration::new(self, waiter_id, waiter)
    }

    /// Selects and signals at most one currently registered waiter.
    pub(in crate::monitor) fn notify_one(&self) {
        let waiter = recover(self.waiters.lock()).take_one();
        if let Some(waiter) = waiter {
            Wake::wake(waiter);
        }
    }

    /// Selects and signals every currently registered waiter.
    pub(in crate::monitor) fn notify_all(&self) {
        let waiters = {
            let mut registry = recover(self.waiters.lock());
            registry.take_all()
        };
        for waiter in waiters {
            Wake::wake(waiter);
        }
    }

    /// Removes an active registration without signalling it.
    ///
    /// # Parameters
    ///
    /// * `waiter_id` - Stable registration identifier to remove.
    pub(super) fn unregister(&self, waiter_id: u64) {
        let waiter = recover(self.waiters.lock()).unregister(waiter_id);
        drop(waiter);
    }
}
