// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`Notifier`](qubit_lock::Notifier).

use qubit_lock::{Notifier, StdMonitor};

/// Exercises both notification methods through a generic capability bound.
fn notify_through_trait<N: Notifier>(notifier: &N) {
    notifier.notify_one();
    notifier.notify_all();
}

#[test]
/// Verifies that a concrete blocking monitor satisfies [`Notifier`].
fn test_notifier_trait_accepts_std_monitor() {
    notify_through_trait(&StdMonitor::new(()));
}
