/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Aggregate asynchronous monitor capability.

use crate::monitor::{
    AsyncNotificationWaiter, AsyncTimeoutConditionWaiter, AsyncTimeoutNotificationWaiter, Notifier,
};

/// Aggregate trait for asynchronous monitor-style synchronization.
pub trait AsyncMonitor:
    Notifier + AsyncNotificationWaiter + AsyncTimeoutNotificationWaiter + AsyncTimeoutConditionWaiter
{
}

impl<T> AsyncMonitor for T where
    T: Notifier
        + AsyncNotificationWaiter
        + AsyncTimeoutNotificationWaiter
        + AsyncTimeoutConditionWaiter
{
}
