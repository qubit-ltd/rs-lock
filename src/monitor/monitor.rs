// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate blocking monitor capability.

use crate::monitor::{Notifier, TimeoutConditionWaiter};

/// Aggregate trait for blocking monitor-style synchronization.
///
/// Use this trait as a static generic bound when code needs both notification
/// and timed blocking condition waits. The inherited generic methods make it
/// unsuitable as a `dyn` trait-object interface. Prefer narrower capability
/// traits when an API does not need the complete blocking monitor contract.
pub trait Monitor: Notifier + TimeoutConditionWaiter {}

impl<T> Monitor for T where T: Notifier + TimeoutConditionWaiter {}
