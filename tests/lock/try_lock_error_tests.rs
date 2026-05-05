/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::error::Error;

use qubit_lock::lock::TryLockError;

/// Test try-lock error display text and error trait implementation.
#[test]
fn test_try_lock_error_display_and_error_trait() {
    assert_eq!(
        TryLockError::WouldBlock.to_string(),
        "lock acquisition would block",
    );
    assert_eq!(TryLockError::Poisoned.to_string(), "lock is poisoned");

    let error: &dyn Error = &TryLockError::WouldBlock;
    assert_eq!(error.to_string(), "lock acquisition would block");
}
