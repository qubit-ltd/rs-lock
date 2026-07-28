// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Monitor Module
//!
//! Provides monitor capability traits and concrete monitor implementations
//! built on parking_lot, standard-library, and Tokio primitives.

// The nested `monitor` module owns the `Monitor` trait; the repeated name is
// intentional so each public trait can live in its matching source file.
#![allow(clippy::module_inception)]

#[cfg(feature = "async-monitor")]
mod async_condition_waiter;
#[cfg(feature = "async-monitor")]
mod async_monitor;
#[cfg(feature = "async-monitor")]
mod async_timed_monitor;
#[cfg(feature = "async-monitor")]
mod async_timeout_condition_waiter;
mod condition_waiter;
pub(crate) mod internal;
mod monitor;
mod notifier;
#[cfg(feature = "parking-lot")]
mod parking_lot_monitor;
#[cfg(feature = "parking-lot")]
mod parking_lot_monitor_guard;
#[cfg(feature = "async-monitor")]
mod shared_async_monitor;
mod shared_monitor;
mod std_monitor;
mod std_monitor_guard;
mod timed_monitor;
mod timeout_condition_waiter;
#[cfg(feature = "async-monitor")]
mod tokio_monitor;
mod wait_timeout_result;
mod wait_timeout_status;

#[cfg(feature = "async-monitor")]
pub use async_condition_waiter::AsyncConditionWaiter;
#[cfg(feature = "async-monitor")]
pub use async_monitor::AsyncMonitor;
#[cfg(feature = "async-monitor")]
pub use async_timed_monitor::AsyncTimedMonitor;
#[cfg(feature = "async-monitor")]
pub use async_timeout_condition_waiter::AsyncTimeoutConditionWaiter;
pub use condition_waiter::ConditionWaiter;
pub use monitor::Monitor;
pub use notifier::Notifier;
#[cfg(feature = "parking-lot")]
pub use parking_lot_monitor::ParkingLotMonitor;
#[cfg(feature = "parking-lot")]
pub use parking_lot_monitor_guard::ParkingLotMonitorGuard;
#[cfg(feature = "async-monitor")]
pub use shared_async_monitor::SharedAsyncMonitor;
pub use shared_monitor::SharedMonitor;
pub use std_monitor::StdMonitor;
pub use std_monitor_guard::StdMonitorGuard;
pub use timed_monitor::TimedMonitor;
pub use timeout_condition_waiter::TimeoutConditionWaiter;
#[cfg(feature = "async-monitor")]
pub use tokio_monitor::TokioMonitor;
pub use wait_timeout_result::WaitTimeoutResult;
pub use wait_timeout_status::WaitTimeoutStatus;
