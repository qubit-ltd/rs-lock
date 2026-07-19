// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable state protected by a blocking condition waiter's private lock.

/// Tracks whether notification or a Timer wake has been latched.
pub(super) struct BlockingConditionWaiterState {
    /// Whether notification or a Timer wake has been latched.
    pub(super) signalled: bool,
}
