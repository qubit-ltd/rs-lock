/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Monitor 相关测试
//!
//! 与 `src/monitor` 对应：`Monitor`、`MonitorGuard`、`ArcMonitor` 的行为测试。

mod arc_monitor_tests;
mod arc_std_monitor_tests;
mod monitor_guard_tests;
mod monitor_tests;
mod std_monitor_guard_tests;
mod std_monitor_tests;
mod wait_timeout_result_tests;
mod wait_timeout_status_tests;
