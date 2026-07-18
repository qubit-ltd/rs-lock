// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for asynchronous read-write lock capabilities.

use qubit_lock::AsyncReadWriteLock;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_async_read_write_lock_modes_return_native_guards() {
    let lock = RwLock::new(7);

    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 7);
    *AsyncReadWriteLock::write(&lock).await = 11;
    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 11);
}
