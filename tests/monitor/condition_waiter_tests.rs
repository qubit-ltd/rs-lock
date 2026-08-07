// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ConditionWaiter`](qubit_lock::ConditionWaiter).

use qubit_lock::ConditionWaiter;
use qubit_lock::StdMonitor;

/// Runs an immediately ready condition wait through a generic bound.
fn wait_through_trait<W>(waiter: &W) -> i32
where
    W: ConditionWaiter<State = bool>,
{
    waiter.wait_until(|ready| *ready, |_| 7)
}

/// Runs an immediately ready action-free condition wait through a generic
/// bound.
fn wait_until_ready_through_trait<W>(waiter: &W)
where
    W: ConditionWaiter<State = bool>,
{
    waiter.wait_until_ready(|ready| *ready);
}

/// Verifies that a concrete blocking monitor satisfies [`ConditionWaiter`].
#[test]
fn test_condition_waiter_trait_accepts_std_monitor() {
    assert_eq!(wait_through_trait(&StdMonitor::new(true)), 7);
}

/// Verifies the trait exposes an action-free wait for a ready condition.
#[test]
fn test_condition_waiter_wait_until_ready_returns_when_predicate_is_ready() {
    wait_until_ready_through_trait(&StdMonitor::new(true));
}
