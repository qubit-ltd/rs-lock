// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Protected state owned by the mock monitor.

/// State protected by the mock monitor.
pub(in crate::monitor) struct MockMonitorState<T> {
    /// User-visible protected value.
    pub(in crate::monitor) value: T,
}

impl<T> MockMonitorState<T> {
    /// Creates protected mock-monitor state around a user value.
    ///
    /// # Arguments
    ///
    /// * `value` - Initial user-visible value.
    ///
    /// # Returns
    ///
    /// Protected state containing `value`.
    #[inline]
    pub(in crate::monitor) fn new(value: T) -> Self {
        Self { value }
    }
}
