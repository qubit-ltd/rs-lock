// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII registration for a mock-monitor waiter.

use super::MockMonitor;

/// Keeps one waiter registered for the lifetime of a wait operation.
pub(super) struct MockMonitorWaiterGuard<'a, T: Send + 'static> {
    /// Monitor that owns the waiter registration.
    monitor: &'a MockMonitor<T>,
    /// Identifier assigned to the registered waiter.
    waiter_id: u64,
    /// Whether this registration contributes to the timeout waiter count.
    timeout_waiter: bool,
}

impl<'a, T: Send + 'static> MockMonitorWaiterGuard<'a, T> {
    /// Creates a guard for an existing waiter registration.
    ///
    /// # Arguments
    ///
    /// * `monitor` - Monitor that owns the registration.
    /// * `waiter_id` - Identifier assigned to the registered waiter.
    /// * `timeout_waiter` - Whether this registration contributes to the
    ///   timeout waiter count.
    pub(super) fn new(
        monitor: &'a MockMonitor<T>,
        waiter_id: u64,
        timeout_waiter: bool,
    ) -> Self {
        Self {
            monitor,
            waiter_id,
            timeout_waiter,
        }
    }
}

impl<T: Send + 'static> Drop for MockMonitorWaiterGuard<'_, T> {
    /// Unregisters the waiter on normal return, cancellation, or panic.
    fn drop(&mut self) {
        self.monitor
            .unregister_waiter(self.waiter_id, self.timeout_waiter);
    }
}
