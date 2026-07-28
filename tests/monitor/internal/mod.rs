// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests mapped to private Monitor implementation modules through public APIs.

#[cfg(not(loom))]
mod blocking_condition_waiter_state_tests;
#[cfg(not(loom))]
mod blocking_condition_waiter_tests;
#[cfg(not(loom))]
mod blocking_timed_wait_tests;
#[cfg(not(loom))]
mod blocking_waiter_registration_tests;
#[cfg(all(not(loom), feature = "parking-lot"))]
mod blocking_waiter_registry_tests;
#[cfg(not(loom))]
mod default_timer_tests;
#[cfg(all(not(loom), feature = "parking-lot"))]
mod mod_tests;
#[cfg(all(loom, feature = "loom-model"))]
mod std_monitor_loom_tests;
#[cfg(not(loom))]
mod sync_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod tokio_condition_waiter_registration_tests;
#[cfg(all(not(loom), feature = "async-monitor"))]
mod tokio_condition_waiter_tests;
#[cfg(all(loom, feature = "loom-model"))]
mod waiter_registry_tests;
