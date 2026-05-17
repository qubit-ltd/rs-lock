/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Cloneable blocking monitor capability.

use crate::monitor::Monitor;

/// Aggregate trait for cloneable, shared blocking monitor handles.
pub trait SharedMonitor: Monitor + Clone + Send + Sync + 'static {}

impl<T> SharedMonitor for T where T: Monitor + Clone + Send + Sync + 'static {}
