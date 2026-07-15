// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation types shared by monitor implementations.

#[cfg(feature = "mock")]
mod mock_monitor_state;
#[cfg(feature = "mock")]
mod mock_monitor_waiter_guard;
#[cfg(feature = "mock")]
mod mock_waiter_state;
#[cfg(feature = "async")]
mod tokio_condition_waiter;
#[cfg(feature = "async")]
mod tokio_condition_waiter_registration;

#[cfg(feature = "mock")]
pub(in crate::monitor) use mock_monitor_state::MockMonitorState;
#[cfg(feature = "mock")]
pub(in crate::monitor) use mock_monitor_waiter_guard::MockMonitorWaiterGuard;
#[cfg(feature = "mock")]
pub(in crate::monitor) use mock_waiter_state::MockWaiterState;
#[cfg(feature = "async")]
pub(in crate::monitor) use tokio_condition_waiter::TokioConditionWaiter;
#[cfg(feature = "async")]
pub(in crate::monitor) use tokio_condition_waiter_registration::TokioConditionWaiterRegistration;
