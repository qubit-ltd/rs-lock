// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation types shared by monitor implementations.

mod blocking_condition_waiter;
mod blocking_waiter_registration;
mod blocking_waiter_registry;
mod default_timer;
#[cfg(feature = "async")]
mod tokio_condition_waiter;
#[cfg(feature = "async")]
mod tokio_condition_waiter_registration;

pub(in crate::monitor) use blocking_condition_waiter::BlockingConditionWaiter;
pub(in crate::monitor) use blocking_waiter_registration::BlockingWaiterRegistration;
pub(in crate::monitor) use blocking_waiter_registry::BlockingWaiterRegistry;
pub(in crate::monitor) use default_timer::default_timer;
#[cfg(feature = "async")]
pub(in crate::monitor) use tokio_condition_waiter::TokioConditionWaiter;
#[cfg(feature = "async")]
pub(in crate::monitor) use tokio_condition_waiter_registration::TokioConditionWaiterRegistration;
