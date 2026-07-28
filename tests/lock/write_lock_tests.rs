// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the synchronous write-mode lock adapter.

use qubit_lock::{
    Lock,
    ReadWriteLock,
    TryLockError,
};
use std::sync::RwLock;

/// Verifies the write adapter provides exclusive acquisition.
#[test]
fn test_write_lock_adapter_is_exclusive() {
    let lock = RwLock::new(());
    let write_lock = ReadWriteLock::write_lock(&lock);
    let guard = Lock::lock(&write_lock);

    assert!(matches!(
        Lock::try_lock(&write_lock),
        Err(TryLockError::WouldBlock)
    ));
    drop(guard);
    assert!(Lock::try_lock(&write_lock).is_ok());
}
