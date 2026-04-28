/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Monitor 模块
//!
//! 在互斥与条件变量之上提供 `Monitor` / `ArcMonitor` 及 `MonitorGuard` 等原语。
//!
//! # Author
//!
//! Haixing Hu

// 子模块 `monitor` 对应类型 `Monitor`；`monitor/monitor.rs` 与父模块同名是刻意分层
#![allow(clippy::module_inception)]

mod arc_monitor;
mod monitor;
mod monitor_guard;
mod wait_timeout_result;
mod wait_timeout_status;

pub use arc_monitor::ArcMonitor;
pub use monitor::Monitor;
pub use monitor_guard::MonitorGuard;
pub use wait_timeout_result::WaitTimeoutResult;
pub use wait_timeout_status::WaitTimeoutStatus;
