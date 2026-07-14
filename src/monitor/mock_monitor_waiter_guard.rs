// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII registration for an active mock-monitor waiter.

use super::MockMonitor;

/// Keeps one waiter registered for the lifetime of a wait operation.
pub(super) struct MockMonitorWaiterGuard<'a, T: Send + 'static> {
    /// Monitor that owns the waiter registration.
    monitor: &'a MockMonitor<T>,
    /// Identifier assigned to the registered waiter.
    waiter_id: u64,
    /// Whether this registration currently contributes to the timeout waiter
    /// count.
    timeout_waiter_active: bool,
    /// Whether the waiter is currently eligible to receive a notification.
    #[cfg(feature = "async")]
    active: bool,
}

impl<'a, T: Send + 'static> MockMonitorWaiterGuard<'a, T> {
    /// Creates a guard for an existing waiter registration.
    ///
    /// # Arguments
    ///
    /// * `monitor` - Monitor that owns the registration.
    /// * `waiter_id` - Identifier assigned to the registered waiter.
    /// * `timeout_waiter_active` - Whether this registration already
    ///   contributes to the timeout waiter count.
    /// * `active` - Whether the waiter is already eligible to receive a
    ///   notification.
    pub(super) fn new(
        monitor: &'a MockMonitor<T>,
        waiter_id: u64,
        timeout_waiter_active: bool,
        active: bool,
    ) -> Self {
        #[cfg(not(feature = "async"))]
        let _ = active;
        Self {
            monitor,
            waiter_id,
            timeout_waiter_active,
            #[cfg(feature = "async")]
            active,
        }
    }

    /// Returns the identifier of the guarded waiter registration.
    pub(super) const fn waiter_id(&self) -> u64 {
        self.waiter_id
    }

    /// Activates a reserved async waiter after its future is first polled.
    ///
    /// # Arguments
    ///
    /// * `timeout_waiter` - Whether activation should also start timeout waiter
    ///   accounting.
    #[cfg(feature = "async")]
    pub(super) fn activate_waiter(&mut self, timeout_waiter: bool) {
        if !self.active {
            self.monitor.activate_waiter(self.waiter_id, timeout_waiter);
            self.active = true;
            self.timeout_waiter_active = timeout_waiter;
        }
    }
}

impl<T: Send + 'static> Drop for MockMonitorWaiterGuard<'_, T> {
    /// Unregisters the waiter on normal return, cancellation, or panic.
    fn drop(&mut self) {
        self.monitor
            .unregister_waiter(self.waiter_id, self.timeout_waiter_active);
    }
}
