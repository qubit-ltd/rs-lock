// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate asynchronous monitor capability with timeout-based waits.

use crate::monitor::AsyncMonitor;
use crate::monitor::AsyncTimeoutConditionWaiter;

/// An asynchronous [`AsyncMonitor`] that also supports timeout-based waits.
///
/// This trait is implemented automatically for every type that provides both
/// capabilities.
///
/// # Examples
///
/// ```
/// use qubit_lock::AsyncTimedMonitor;
/// use qubit_lock::TokioMonitor;
///
/// #[tokio::main]
/// async fn main() {
///     let monitor = TokioMonitor::current(false);
///     fn requires_timed_monitor<M: AsyncTimedMonitor>(_: &M) {}
///     requires_timed_monitor(&monitor);
/// }
/// ```
pub trait AsyncTimedMonitor: AsyncMonitor + AsyncTimeoutConditionWaiter {}

impl<M> AsyncTimedMonitor for M where M: AsyncMonitor + AsyncTimeoutConditionWaiter + ?Sized {}
