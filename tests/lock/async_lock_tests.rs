// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for data-independent asynchronous RAII locks.

use std::future::Future;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use qubit_lock::AsyncLock;
use qubit_lock::TryLockError;
use tokio::sync::Mutex;
use tokio::test as tokio_test;

#[tokio_test]
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

#[tokio_test]
async fn test_async_lock_accepts_arc_forwarding() {
    let lock = Arc::new(Mutex::new(()));

    let guard = AsyncLock::lock(&lock).await;
    drop(guard);
    assert!(AsyncLock::try_lock(&lock).is_ok());
}

#[tokio_test]
async fn test_async_lock_accepts_borrowed_forwarding() {
    let lock = Mutex::new(());
    let borrowed = &lock;

    let guard = AsyncLock::lock(&borrowed).await;
    drop(guard);
    assert!(AsyncLock::try_lock(&borrowed).is_ok());
}

/// Verifies dropping a waiter after registration does not retain the lock.
#[tokio_test]
async fn test_async_lock_cancelled_waiter_does_not_retain_lock() {
    let lock = Mutex::new(());
    let guard = AsyncLock::lock(&lock).await;
    let mut waiter = Box::pin(AsyncLock::lock(&lock));

    poll_fn(|context| {
        assert!(waiter.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiter);
    drop(guard);

    assert!(AsyncLock::try_lock(&lock).is_ok());
}
