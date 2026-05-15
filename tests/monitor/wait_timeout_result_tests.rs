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

fn double_i32(value: i32) -> i32 {
    value * 2
}

fn i32_to_string(value: i32) -> String {
    value.to_string()
}

/// Test wait-timeout result variants and value semantics.
#[test]
fn test_wait_timeout_result_variants_are_distinct() {
    assert_eq!(WaitTimeoutResult::Ready(42), WaitTimeoutResult::Ready(42));
    assert_ne!(WaitTimeoutResult::Ready(42), WaitTimeoutResult::Ready(7));
    assert_ne!(WaitTimeoutResult::Ready(42), WaitTimeoutResult::TimedOut);

    let copied = WaitTimeoutResult::Ready("ready");
    assert_eq!(copied, copied);
}

/// Test wait-timeout result helper methods for ready values.
#[test]
fn test_wait_timeout_result_ready_helpers() {
    let result = WaitTimeoutResult::Ready(21);

    assert!(result.is_ready());
    assert!(!result.is_timed_out());
    assert_eq!(result.into_option(), Some(21));

    let mapped = WaitTimeoutResult::Ready(21).map(double_i32);
    assert_eq!(mapped, WaitTimeoutResult::Ready(42));

    let mapped_to_string = WaitTimeoutResult::Ready(21).map(i32_to_string);
    assert_eq!(
        mapped_to_string,
        WaitTimeoutResult::Ready(String::from("21"))
    );
}

/// Test wait-timeout result helper methods for timeout values.
#[test]
fn test_wait_timeout_result_timed_out_helpers() {
    let result = WaitTimeoutResult::<i32>::TimedOut;

    assert!(!result.is_ready());
    assert!(result.is_timed_out());
    assert_eq!(result.into_option(), None);

    let mapped = WaitTimeoutResult::<i32>::TimedOut.map(double_i32);
    assert_eq!(mapped, WaitTimeoutResult::TimedOut);

    let mapped_to_string: WaitTimeoutResult<String> =
        WaitTimeoutResult::<i32>::TimedOut.map(i32_to_string);
    assert_eq!(mapped_to_string, WaitTimeoutResult::TimedOut);
}
