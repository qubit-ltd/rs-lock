// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the asynchronous write-mode lock adapter.

use qubit_lock::{
    AsyncLock,
    AsyncReadWriteLock,
    TryLockError,
};
use tokio::sync::RwLock;

/// Verifies the asynchronous write adapter is exclusive.
#[tokio::test]
async fn test_async_write_lock_adapter_is_exclusive() {
    let lock = RwLock::new(());
    let write_lock = AsyncReadWriteLock::write_lock(&lock);
    let guard = AsyncLock::lock(&write_lock).await;

    assert!(matches!(
        AsyncLock::try_lock(&write_lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(guard);
    assert!(AsyncLock::try_lock(&write_lock).is_ok());
}
