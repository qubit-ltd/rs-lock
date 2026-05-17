/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Monitor Tests
//!
//! Covers behavior corresponding to `src/monitor`.

mod arc_mock_monitor_tests;
mod arc_parking_lot_monitor_tests;
mod arc_std_monitor_tests;
#[cfg(feature = "async")]
mod arc_tokio_monitor_tests;
mod mock_monitor_tests;
mod monitor_trait_tests;
mod parking_lot_monitor_guard_tests;
mod parking_lot_monitor_tests;
mod std_monitor_guard_tests;
mod std_monitor_tests;
#[cfg(feature = "async")]
mod tokio_monitor_tests;
mod wait_timeout_result_tests;
mod wait_timeout_status_tests;
