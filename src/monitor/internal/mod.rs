// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation types shared by monitor implementations.

mod blocking_condition_wait;
mod blocking_condition_waiter;
mod blocking_condition_waiter_state;
mod blocking_monitor_guard_wait;
mod blocking_waiter_registration;
mod blocking_waiter_registry;
mod default_timer;
pub(in crate::monitor) mod sync;
#[cfg(feature = "async-monitor")]
mod tokio_condition_waiter;
#[cfg(feature = "async-monitor")]
mod tokio_condition_waiter_registration;
mod waiter_registry;

pub(in crate::monitor) use blocking_condition_wait::{
    wait_while_for,
    wait_while_locked,
    wait_while_with_deadline,
};
pub(in crate::monitor) use blocking_condition_waiter::BlockingConditionWaiter;
pub(in crate::monitor) use blocking_monitor_guard_wait::{
    release_guard,
    wait_for_notification,
    wait_with_timer,
};
pub(in crate::monitor) use blocking_waiter_registration::BlockingWaiterRegistration;
pub(in crate::monitor) use blocking_waiter_registry::BlockingWaiterRegistry;
pub(in crate::monitor) use default_timer::default_timer;
#[cfg(feature = "async-monitor")]
pub(in crate::monitor) use tokio_condition_waiter::TokioConditionWaiter;
#[cfg(feature = "async-monitor")]
pub(in crate::monitor) use tokio_condition_waiter_registration::TokioConditionWaiterRegistration;
pub(crate) use waiter_registry::WaiterRegistry;
