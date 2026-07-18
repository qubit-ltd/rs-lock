// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for asynchronous read-write lock capabilities.

use qubit_lock::{
    AsyncLock,
    AsyncReadWriteLock,
    TryLockError,
};
use tokio::sync::RwLock;

#[tokio::test]
async fn test_async_read_write_lock_modes_return_native_guards() {
    let lock = RwLock::new(7);

    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 7);
    *AsyncReadWriteLock::write(&lock).await = 11;
    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 11);
}

#[tokio::test]
async fn test_async_read_write_lock_read_adapter_is_shared() {
    let lock = RwLock::new(());
    let read_lock = AsyncReadWriteLock::read_lock(&lock);
    let first = AsyncLock::lock(&read_lock).await;

    assert!(AsyncLock::try_lock(&read_lock).is_ok());
    drop(first);
}

#[tokio::test]
async fn test_async_read_write_lock_write_adapter_is_exclusive() {
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
