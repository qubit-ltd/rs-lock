// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for asynchronous read-write lock capabilities.

use std::sync::Arc;

use qubit_lock::AsyncReadWriteLock;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_async_read_write_lock_modes_return_native_guards() {
    let lock = RwLock::new(7);

    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 7);
    *AsyncReadWriteLock::write(&lock).await = 11;
    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 11);
}

#[tokio::test]
async fn test_async_read_write_lock_accepts_borrowed_forwarding() {
    let lock = RwLock::new(1);
    let borrowed = &lock;

    assert_eq!(*AsyncReadWriteLock::read(&borrowed).await, 1);
    *AsyncReadWriteLock::write(&borrowed).await = 2;
    assert_eq!(
        *AsyncReadWriteLock::try_read(&borrowed).expect("read should succeed"),
        2,
    );
    *AsyncReadWriteLock::try_write(&borrowed).expect("write should succeed") = 3;
}

#[tokio::test]
async fn test_async_read_write_lock_accepts_arc_forwarding() {
    let lock = Arc::new(RwLock::new(1));

    assert_eq!(*AsyncReadWriteLock::read(&lock).await, 1);
    *AsyncReadWriteLock::write(&lock).await = 2;
    assert_eq!(
        *AsyncReadWriteLock::try_read(&lock).expect("read should succeed"),
        2,
    );
    *AsyncReadWriteLock::try_write(&lock).expect("write should succeed") = 3;
}
