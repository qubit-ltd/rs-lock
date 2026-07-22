// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stores active blocking Monitor waiters with memoryless notification.

use std::collections::BTreeMap;
use std::sync::{
    Arc,
    Mutex,
};
use std::task::Wake;

use super::{
    BlockingConditionWaiter,
    BlockingWaiterRegistration,
};

/// Registry of blocking waiters eligible for current notifications.
pub(in crate::monitor) struct BlockingWaiterRegistry {
    /// Active waiters keyed by stable allocation address.
    waiters: Mutex<BTreeMap<usize, Arc<BlockingConditionWaiter>>>,
}

impl BlockingWaiterRegistry {
    /// Creates an empty waiter registry.
    ///
    /// # Returns
    ///
    /// A registry containing no notification permits or waiters.
    #[must_use]
    #[inline]
    pub(in crate::monitor) const fn new() -> Self {
        Self {
            waiters: Mutex::new(BTreeMap::new()),
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
    /// Panics if an allocation address unexpectedly collides with an active
    /// waiter key.
    pub(in crate::monitor) fn register(
        &self,
    ) -> BlockingWaiterRegistration<'_> {
        let waiter = Arc::new(BlockingConditionWaiter::new());
        let key = Arc::as_ptr(&waiter) as usize;
        let previous = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, Arc::clone(&waiter));
        assert!(previous.is_none(), "blocking monitor waiter pointer reused");
        BlockingWaiterRegistration::new(self, key, waiter)
    }

    /// Selects and signals at most one currently registered waiter.
    pub(in crate::monitor) fn notify_one(&self) {
        let waiter = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_last()
            .map(|(_, waiter)| waiter);
        if let Some(waiter) = waiter {
            Wake::wake(waiter);
        }
    }

    /// Selects and signals every currently registered waiter.
    pub(in crate::monitor) fn notify_all(&self) {
        let waiters = {
            let mut registry = self
                .waiters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *registry)
        };
        for waiter in waiters.into_values() {
            Wake::wake(waiter);
        }
    }

    /// Removes an active registration without signalling it.
    ///
    /// # Parameters
    ///
    /// * `key` - Stable allocation-address key to remove.
    pub(super) fn unregister(&self, key: usize) {
        let waiter = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        drop(waiter);
    }
}
