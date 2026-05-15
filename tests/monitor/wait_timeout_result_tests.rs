/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use qubit_lock::monitor::WaitTimeoutResult;

/// Test wait-timeout result variants and value semantics.
#[test]
fn test_wait_timeout_result_variants_are_distinct() {
    assert_eq!(WaitTimeoutResult::Ready(42), WaitTimeoutResult::Ready(42));
    assert_ne!(WaitTimeoutResult::Ready(42), WaitTimeoutResult::Ready(7));
    assert_ne!(WaitTimeoutResult::Ready(42), WaitTimeoutResult::TimedOut);

    let copied = WaitTimeoutResult::Ready("ready");
    assert_eq!(copied, copied);
}
