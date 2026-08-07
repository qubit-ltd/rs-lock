// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the synchronous read-mode lock adapter.

use std::sync::RwLock;

use qubit_lock::Lock;
use qubit_lock::ReadWriteLock;

/// Verifies two read adapters may hold shared guards concurrently.
#[test]
fn test_read_lock_adapter_is_shared() {
    let lock = RwLock::new(());
    let read_lock = ReadWriteLock::read_lock(&lock);
    let first = Lock::lock(&read_lock);

    assert!(Lock::try_lock(&read_lock).is_ok());
    drop(first);
}
