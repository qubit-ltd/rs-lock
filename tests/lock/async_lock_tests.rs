// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for data-independent asynchronous RAII locks.

use std::sync::Arc;

use qubit_lock::{
    AsyncLock,
    TryLockError,
};
use tokio::sync::Mutex;

#[tokio::test]
async fn test_async_lock_tokio_mutex_releases_on_guard_drop() {
    let lock = Mutex::new(7);
    let guard = AsyncLock::lock(&lock).await;

    assert!(matches!(
        AsyncLock::try_lock(&lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(guard);
    assert!(AsyncLock::try_lock(&lock).is_ok());
}

#[tokio::test]
async fn test_async_lock_accepts_arc_forwarding() {
    let lock = Arc::new(Mutex::new(()));

    let guard = AsyncLock::lock(&lock).await;
    drop(guard);
    assert!(AsyncLock::try_lock(&lock).is_ok());
}

#[tokio::test]
async fn test_async_lock_cancelled_waiter_does_not_retain_lock() {
    let lock = Arc::new(Mutex::new(()));
    let guard = AsyncLock::lock(&lock).await;
    let waiting_lock = Arc::clone(&lock);
    let waiter = tokio::spawn(async move {
        let _guard = AsyncLock::lock(&waiting_lock).await;
    });

    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    drop(guard);

    assert!(AsyncLock::try_lock(&lock).is_ok());
}
