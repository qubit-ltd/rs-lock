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
pub trait AsyncTimedMonitor: AsyncMonitor + AsyncTimeoutConditionWaiter {}

impl<M> AsyncTimedMonitor for M where M: AsyncMonitor + AsyncTimeoutConditionWaiter + ?Sized {}
