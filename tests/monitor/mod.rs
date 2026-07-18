// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Monitor Tests
//!
//! Covers behavior corresponding to `src/monitor`.

mod arc_parking_lot_monitor_tests;
mod arc_std_monitor_tests;
#[cfg(feature = "async")]
mod arc_tokio_monitor_tests;
#[cfg(feature = "async")]
mod async_condition_waiter_tests;
#[cfg(feature = "async")]
mod async_monitor_tests;
#[cfg(feature = "async")]
mod async_timeout_condition_waiter_tests;
mod blocking_condition_waiter_tests;
mod blocking_waiter_registration_tests;
mod blocking_waiter_registry_tests;
mod condition_waiter_tests;
mod default_timer_tests;
mod failing_timer_tests;
mod monitor_tests;
mod notifier_tests;
mod parking_lot_monitor_guard_tests;
mod parking_lot_monitor_tests;
#[cfg(feature = "async")]
mod shared_async_monitor_tests;
mod shared_monitor_tests;
mod std_monitor_guard_tests;
mod std_monitor_tests;
mod timeout_condition_waiter_tests;
#[cfg(feature = "async")]
mod tokio_condition_waiter_registration_tests;
#[cfg(feature = "async")]
mod tokio_condition_waiter_tests;
#[cfg(feature = "async")]
mod tokio_monitor_tests;
mod wait_timeout_result_tests;
mod wait_timeout_status_tests;
