// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests mapped to private Monitor implementation modules through public APIs.

mod blocking_condition_waiter_state_tests;
mod blocking_condition_waiter_tests;
mod blocking_waiter_registration_tests;
#[cfg(feature = "parking-lot")]
mod blocking_waiter_registry_tests;
mod default_timer_tests;
#[cfg(feature = "parking-lot")]
mod mod_tests;
#[cfg(feature = "async-monitor")]
mod tokio_condition_waiter_registration_tests;
#[cfg(feature = "async-monitor")]
mod tokio_condition_waiter_tests;
#[cfg(all(loom, feature = "loom-model"))]
mod waiter_registry_tests;
