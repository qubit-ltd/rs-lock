// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe registration for a Tokio condition waiter.

use std::{
    collections::BTreeMap,
    future::{
        Future,
        poll_fn,
    },
    sync::{
        Arc,
        Mutex,
    },
    task::Poll,
};

use qubit_clock::TimerFuture;

use super::TokioConditionWaiter;
use crate::monitor::WaitTimeoutStatus;

/// Removes an active waiter registration on cancellation or normal exit.
#[must_use = "retain the registration while the waiter remains eligible for notification"]
pub(in crate::monitor) struct TokioConditionWaiterRegistration<'a> {
    /// Registry containing this waiter while it remains selectable.
    registry: &'a Mutex<BTreeMap<usize, Arc<TokioConditionWaiter>>>,
    /// Independently signalled waiter owned by the pending condition wait.
    waiter: Arc<TokioConditionWaiter>,
}

impl<'a> TokioConditionWaiterRegistration<'a> {
    /// Creates a registration for a waiter already stored in `registry`.
    ///
    /// # Parameters
    ///
    /// * `registry` - Registry that currently contains `waiter`.
    /// * `waiter` - Independently signalled waiter owned by the pending wait.
    ///
    /// # Returns
    ///
    /// A guard that unregisters `waiter` when dropped.
    #[inline]
    pub(in crate::monitor) fn new(
        registry: &'a Mutex<BTreeMap<usize, Arc<TokioConditionWaiter>>>,
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
    pub(in crate::monitor) async fn wait_until_signalled_or_deadline(
        &self,
        deadline: &mut TimerFuture,
    ) -> WaitTimeoutStatus {
        let notified = self.waiter().signal().notified();
        tokio::pin!(notified);
        poll_fn(|context| {
            if deadline.as_mut().poll(context).is_ready() {
                Poll::Ready(WaitTimeoutStatus::TimedOut)
            } else if notified.as_mut().poll(context).is_ready() {
                Poll::Ready(WaitTimeoutStatus::Woken)
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl Drop for TokioConditionWaiterRegistration<'_> {
    /// Removes this waiter if no notification has selected it yet.
    fn drop(&mut self) {
        let waiter_key = Arc::as_ptr(&self.waiter) as usize;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.remove(&waiter_key);
    }
}
