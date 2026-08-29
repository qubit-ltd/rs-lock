// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe registration for a Tokio condition waiter.

use std::future::Future;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use qubit_clock::TimeError;
use qubit_clock::TimerFuture;

use super::TokioConditionWaiter;
use super::WaiterRegistry;
use crate::monitor::WaitTimeoutStatus;

/// Removes an active waiter registration on cancellation or normal exit.
///
/// # Type Parameters
///
/// * `'a` - Lifetime of the registry that owns the active entry.
#[must_use = "retain the registration while the waiter remains eligible for notification"]
pub(in crate::monitor) struct TokioConditionWaiterRegistration<'a> {
    /// Registry containing this waiter while it remains selectable.
    registry: &'a Mutex<WaiterRegistry<Arc<TokioConditionWaiter>>>,
    /// Stable registry identifier for cancellation.
    waiter_id: u64,
    /// Independently signalled waiter owned by the pending condition wait.
    waiter: Arc<TokioConditionWaiter>,
}

impl<'a> TokioConditionWaiterRegistration<'a> {
    /// Creates a registration for a waiter already stored in `registry`.
    ///
    /// # Parameters
    ///
    /// * `registry` - Registry that currently contains `waiter`.
    /// * `waiter_id` - Stable identifier assigned to `waiter`.
    /// * `waiter` - Independently signalled waiter owned by the pending wait.
    ///
    /// # Returns
    ///
    /// A guard that unregisters `waiter` when dropped.
    #[inline]
    pub(in crate::monitor) fn new(
        registry: &'a Mutex<WaiterRegistry<Arc<TokioConditionWaiter>>>,
        waiter_id: u64,
        waiter: Arc<TokioConditionWaiter>,
    ) -> Self {
        Self {
            registry,
            waiter_id,
            waiter,
        }
    }

    /// Returns the waiter owned by this registration.
    ///
    /// # Returns
    ///
    /// The registered waiter selected by monitor notifications.
    #[must_use]
    #[inline(always)]
    pub(in crate::monitor) fn waiter(&self) -> &TokioConditionWaiter {
        &self.waiter
    }

    /// Waits until this registration is selected or the deadline is reached.
    ///
    /// Deadline completion wins when notification and timeout are both ready.
    ///
    /// # Parameters
    ///
    /// * `deadline` - Fixed Timer registration shared across predicate checks.
    ///
    /// # Returns
    ///
    /// Whether notification or the deadline completed this suspension.
    ///
    /// # Errors
    ///
    /// Returns a Timer completion error when polling `deadline` fails.
    pub(in crate::monitor) async fn wait_until_signalled_or_deadline(
        &self,
        deadline: &mut TimerFuture,
    ) -> Result<WaitTimeoutStatus, TimeError> {
        let notified = self.waiter().signal().notified();
        tokio::pin!(notified);
        poll_fn(|context| {
            if let Poll::Ready(result) = deadline.as_mut().poll(context) {
                Poll::Ready(result.map(|()| WaitTimeoutStatus::TimedOut))
            } else if notified.as_mut().poll(context).is_ready() {
                Poll::Ready(Ok(WaitTimeoutStatus::Woken))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl Drop for TokioConditionWaiterRegistration<'_> {
    /// Removes this waiter if no notification has selected it yet.
    #[inline]
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.unregister(self.waiter_id);
    }
}
