// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Monitor Tests
//!
//! Covers behavior corresponding to `src/monitor`.

/// Compares successful time results without requiring time errors to support
/// value equality.
macro_rules! assert_time_result_eq {
    ($actual:expr, Ok($expected:expr) $(,)?) => {{
        let expected = $expected;
        match $actual {
            Ok(actual) => assert_eq!(actual, expected),
            Err(error) => panic!("time result unexpectedly failed: {error}"),
        }
    }};
    (Ok($expected:expr), $actual:expr $(,)?) => {{
        let expected = $expected;
        match $actual {
            Ok(actual) => assert_eq!(expected, actual),
            Err(error) => panic!("time result unexpectedly failed: {error}"),
        }
    }};
}

#[cfg(feature = "parking-lot")]
mod arc_parking_lot_monitor_tests;
mod arc_std_monitor_tests;
#[cfg(feature = "async-monitor")]
mod arc_tokio_monitor_tests;
#[cfg(feature = "async-monitor")]
mod async_condition_waiter_tests;
#[cfg(feature = "async-monitor")]
mod async_monitor_tests;
#[cfg(feature = "async-monitor")]
mod async_timeout_condition_waiter_tests;
mod condition_waiter_tests;
mod failing_timer_tests;
mod internal;
mod mod_tests;
mod monitor_tests;
mod notifier_tests;
#[cfg(feature = "parking-lot")]
mod parking_lot_monitor_guard_tests;
#[cfg(feature = "parking-lot")]
mod parking_lot_monitor_tests;
#[cfg(feature = "async-monitor")]
mod shared_async_monitor_tests;
mod shared_monitor_tests;
mod std_monitor_guard_tests;
mod std_monitor_tests;
mod timeout_condition_waiter_tests;
#[cfg(feature = "async-monitor")]
mod tokio_monitor_tests;
mod wait_timeout_result_tests;
mod wait_timeout_status_tests;
