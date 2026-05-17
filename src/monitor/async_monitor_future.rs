/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Boxed future type used by asynchronous monitor traits.

use std::{
    future::Future,
    pin::Pin,
};

/// Sendable boxed future returned by asynchronous monitor operations.
pub type AsyncMonitorFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
