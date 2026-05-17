/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Aggregate blocking monitor capability.

use crate::monitor::{
    NotificationWaiter,
    Notifier,
    TimeoutConditionWaiter,
    TimeoutNotificationWaiter,
};

/// Aggregate trait for blocking monitor-style synchronization.
pub trait Monitor:
    Notifier + NotificationWaiter + TimeoutNotificationWaiter + TimeoutConditionWaiter
{
}

impl<T> Monitor for T where
    T: Notifier + NotificationWaiter + TimeoutNotificationWaiter + TimeoutConditionWaiter
{
}
