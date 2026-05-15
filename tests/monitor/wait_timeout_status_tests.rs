/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use qubit_lock::monitor::WaitTimeoutStatus;

/// Test wait-timeout status variants and value semantics.
#[test]
fn test_wait_timeout_status_variants_are_distinct() {
    assert_eq!(WaitTimeoutStatus::Woken, WaitTimeoutStatus::Woken);
    assert_eq!(WaitTimeoutStatus::TimedOut, WaitTimeoutStatus::TimedOut);
    assert_ne!(WaitTimeoutStatus::Woken, WaitTimeoutStatus::TimedOut);

    let copied = WaitTimeoutStatus::TimedOut;
    assert_eq!(copied, copied);
}
