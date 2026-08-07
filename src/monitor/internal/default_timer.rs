// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provides the process-shared standard Timer for blocking monitors.

use std::sync::Arc;
use std::sync::OnceLock;

use qubit_clock::StdTimer;
use qubit_clock::Timer;

/// Returns the process-shared standard Timer used by default constructors.
///
/// # Returns
///
/// A shared Timer driven by one standard monotonic clock domain.
///
/// # Panics
///
/// Panics if all process-wide clock-domain identifiers are exhausted while
/// initializing the shared Timer.
#[inline]
pub(in crate::monitor) fn default_timer() -> Arc<dyn Timer> {
    static TIMER: OnceLock<Arc<dyn Timer>> = OnceLock::new();
    Arc::clone(TIMER.get_or_init(|| Arc::new(StdTimer::new())))
}
