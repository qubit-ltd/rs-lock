// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII registration for an active mock-monitor timeout waiter.

use super::MockMonitor;

/// Keeps one timeout waiter registered for the lifetime of a wait operation.
pub(super) struct MockMonitorWaiterGuard<'a, T: Send + 'static> {
    monitor: &'a MockMonitor<T>,
}

impl<'a, T: Send + 'static> MockMonitorWaiterGuard<'a, T> {
    /// Registers one active timeout waiter on `monitor`.
    pub(super) fn new(monitor: &'a MockMonitor<T>) -> Self {
        monitor.register_timeout_waiter();
        Self { monitor }
    }
}

impl<T: Send + 'static> Drop for MockMonitorWaiterGuard<'_, T> {
    /// Unregisters the timeout waiter on normal return, cancellation, or panic.
    fn drop(&mut self) {
        self.monitor.unregister_timeout_waiter();
    }
}
