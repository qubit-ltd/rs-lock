// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the asynchronous read-mode lock adapter.

use qubit_lock::AsyncLock;
use qubit_lock::AsyncReadWriteLock;
use tokio::sync::RwLock;
use tokio::test as tokio_test;

/// Verifies two asynchronous read adapters may hold guards concurrently.
#[tokio_test]
async fn test_async_read_lock_adapter_is_shared() {
    let lock = RwLock::new(());
    let read_lock = AsyncReadWriteLock::read_lock(&lock);
    let first = AsyncLock::lock(&read_lock).await;

    assert!(AsyncLock::try_lock(&read_lock).is_ok());
    drop(first);
}
