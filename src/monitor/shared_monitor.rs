// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cloneable blocking monitor capability.

use crate::monitor::Monitor;

/// Aggregate trait for cloneable, shared untimed blocking monitor handles.
///
/// This is a static generic bound for APIs that retain or clone a monitor
/// handle. The inherited generic methods make it unsuitable as a `dyn`
/// trait-object interface.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_lock::Monitor;
/// use qubit_lock::SharedMonitor;
/// use qubit_lock::StdMonitor;
///
/// fn require_shared_monitor<M: SharedMonitor>() {}
///
/// let monitor = Arc::new(StdMonitor::new(true));
/// require_shared_monitor::<Arc<StdMonitor<bool>>>();
/// assert!(monitor.with_read(|state| *state));
/// ```
pub trait SharedMonitor: Monitor + Clone + Send + Sync + 'static {
    // empty
}

impl<T> SharedMonitor for T
where
    T: Monitor + Clone + Send + Sync + 'static,
{
    // empty
}
