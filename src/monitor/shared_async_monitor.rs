// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cloneable asynchronous monitor capability.

use crate::monitor::AsyncMonitor;

/// Aggregate trait for cloneable, shared untimed asynchronous monitor handles.
///
/// This is a static generic bound for APIs that retain or clone an async
/// monitor handle. The inherited return-position `impl Future` methods make it
/// unsuitable as a `dyn` trait-object interface.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_lock::SharedAsyncMonitor;
/// use qubit_lock::TokioMonitor;
///
/// fn require_shared_monitor<M: SharedAsyncMonitor>() {}
///
/// require_shared_monitor::<Arc<TokioMonitor<u32>>>();
/// ```
pub trait SharedAsyncMonitor: AsyncMonitor + Clone + Send + Sync + 'static {}

impl<T> SharedAsyncMonitor for T where T: AsyncMonitor + Clone + Send + Sync + 'static {}
