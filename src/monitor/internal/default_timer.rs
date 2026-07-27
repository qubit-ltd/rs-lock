// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provides the process-shared standard Timer for blocking monitors.

use std::sync::{Arc, OnceLock};

use qubit_clock::{StdTimer, Timer};

/// Returns the process-shared standard Timer used by default constructors.
///
/// # Returns
///
/// A shared Timer driven by one standard monotonic clock domain.
#[inline]
pub(in crate::monitor) fn default_timer() -> Arc<dyn Timer> {
    static TIMER: OnceLock<Arc<dyn Timer>> = OnceLock::new();
    Arc::clone(TIMER.get_or_init(|| Arc::new(StdTimer::new())))
}
