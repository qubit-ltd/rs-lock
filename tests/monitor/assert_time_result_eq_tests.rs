// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

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
