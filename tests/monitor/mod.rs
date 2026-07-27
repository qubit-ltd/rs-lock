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

#[cfg(not(loom))]
#[macro_use]
mod assert_time_result_eq_tests;
#[cfg(not(loom))]
#[macro_use]
mod blocking_monitor_contract_tests;

#[cfg(all(not(loom), feature = "parking-lot"))]
mod arc_parking_lot_monitor_tests;
#[cfg(not(loom))]
mod arc_std_monitor_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod arc_tokio_monitor_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod async_condition_waiter_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod async_monitor_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod async_timed_monitor_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod async_timeout_condition_waiter_tests;
#[cfg(not(loom))]
mod condition_waiter_tests;
#[cfg(not(loom))]
mod failing_timer_tests;
mod internal;
#[cfg(not(loom))]
mod mod_tests;
#[cfg(not(loom))]
mod monitor_tests;
#[cfg(not(loom))]
mod notifier_tests;
#[cfg(all(not(loom), feature = "parking-lot"))]
mod parking_lot_monitor_guard_tests;
#[cfg(all(not(loom), feature = "parking-lot"))]
mod parking_lot_monitor_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod shared_async_monitor_tests;
#[cfg(not(loom))]
mod shared_monitor_tests;
#[cfg(not(loom))]
mod std_monitor_guard_tests;
#[cfg(not(loom))]
mod std_monitor_tests;
#[cfg(not(loom))]
mod timed_monitor_tests;
#[cfg(not(loom))]
mod timeout_condition_waiter_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod tokio_monitor_tests;
#[cfg(not(loom))]
mod wait_timeout_result_tests;
#[cfg(not(loom))]
mod wait_timeout_status_tests;
