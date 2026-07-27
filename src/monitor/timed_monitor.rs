// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate timed blocking monitor capability.

use crate::monitor::{Monitor, TimeoutConditionWaiter};

/// Aggregate trait for blocking monitors with timeout-aware condition waits.
///
/// This trait extends the complete untimed [`Monitor`] contract with
/// [`TimeoutConditionWaiter`]. Use it when a generic API needs both monitor
/// state coordination and bounded waiting.
pub trait TimedMonitor: Monitor + TimeoutConditionWaiter {}

impl<M: ?Sized> TimedMonitor for M where M: Monitor + TimeoutConditionWaiter {}
