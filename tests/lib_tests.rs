// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for crate-root feature-gated public exports.

#[cfg(feature = "async-lock")]
use qubit_lock::AsyncDataLock;
#[cfg(feature = "async-lock")]
use qubit_lock::AsyncLock;
#[cfg(feature = "async-lock")]
use qubit_lock::AsyncReadLock;
#[cfg(feature = "async-lock")]
use qubit_lock::AsyncReadWriteLock;
#[cfg(feature = "async-lock")]
use qubit_lock::AsyncWriteLock;

/// Verifies that `async-lock` exposes lock capabilities without requiring the
/// Tokio monitor API.
#[cfg(feature = "async-lock")]
#[test]
fn test_async_lock_feature_exports_async_lock_capabilities() {
    fn accepts_async_lock<L: AsyncLock>() {}
    fn accepts_async_read_write_lock<L: AsyncReadWriteLock>() {}
    fn accepts_async_data_lock<L: AsyncDataLock<usize>>() {}

    accepts_async_lock::<tokio::sync::Mutex<usize>>();
    accepts_async_read_write_lock::<tokio::sync::RwLock<usize>>();
    accepts_async_data_lock::<tokio::sync::RwLock<usize>>();
    let _ = std::any::type_name::<
        AsyncReadLock<'static, tokio::sync::RwLock<usize>>,
    >();
    let _ = std::any::type_name::<
        AsyncWriteLock<'static, tokio::sync::RwLock<usize>>,
    >();
}
