// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate timed blocking monitor capability.

use crate::monitor::{
    Monitor,
    TimeoutConditionWaiter,
};

/// Aggregate trait for blocking monitors with timeout-aware condition waits.
///
/// This trait extends the complete untimed [`Monitor`] contract with
/// [`TimeoutConditionWaiter`]. Use it when a generic API needs both monitor
/// state coordination and bounded waiting. Implementations support absolute
/// deadlines, condition-wait budgets that begin after initial lock
/// acquisition, and operation-wide timeouts that fix their deadline before
/// initial lock acquisition.
pub trait TimedMonitor: Monitor + TimeoutConditionWaiter {}

impl<M: ?Sized> TimedMonitor for M where M: Monitor + TimeoutConditionWaiter {}
