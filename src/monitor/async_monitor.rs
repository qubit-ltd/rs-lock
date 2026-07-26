// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate asynchronous monitor capability.

use crate::monitor::{
    AsyncTimeoutConditionWaiter,
    Notifier,
};

/// Aggregate trait for asynchronous monitor-style synchronization.
///
/// Use this trait as a static generic bound when code needs both notification
/// and timed asynchronous condition waits. The inherited return-position
/// `impl Future` methods make it unsuitable as a `dyn` trait-object interface.
/// Prefer narrower capability traits when the complete contract is unnecessary.
pub trait AsyncMonitor: Notifier + AsyncTimeoutConditionWaiter {}

impl<T> AsyncMonitor for T where T: Notifier + AsyncTimeoutConditionWaiter {}
