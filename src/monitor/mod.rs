/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Monitor Module
//!
//! Provides `Monitor`, `ArcMonitor`, and standard-library counterparts built
//! on mutex and condition-variable primitives.
//!

// The nested `monitor` module owns the `Monitor` type; the repeated name is an
// intentional module boundary.
#![allow(clippy::module_inception)]

mod arc_monitor;
mod arc_std_monitor;
mod monitor;
mod monitor_guard;
mod std_monitor;
mod std_monitor_guard;
mod wait_timeout_result;
mod wait_timeout_status;

pub use arc_monitor::ArcMonitor;
pub use arc_std_monitor::ArcStdMonitor;
pub use monitor::Monitor;
pub use monitor_guard::MonitorGuard;
pub use std_monitor::StdMonitor;
pub use std_monitor_guard::StdMonitorGuard;
pub use wait_timeout_result::WaitTimeoutResult;
pub use wait_timeout_status::WaitTimeoutStatus;
