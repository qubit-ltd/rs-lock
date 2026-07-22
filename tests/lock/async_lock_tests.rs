// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for data-independent asynchronous RAII locks.

use std::{
    future::{
        Future,
        poll_fn,
    },
    sync::Arc,
    task::Poll,
};

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
/// Verifies dropping a waiter after registration does not retain the lock.
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
