// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exposes the production synchronous monitor to Loom models.

/// Production synchronous monitor configured with Loom synchronization.
///
/// # Type Parameters
///
/// * `T` - The state type protected by the monitor.
pub type LoomStdMonitor<T> = crate::monitor::StdMonitor<T>;
