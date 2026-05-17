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
//! Provides monitor capability traits and concrete monitor implementations
//! built on parking_lot, standard-library, Tokio, and mock primitives.
//!

// The nested `monitor` module owns the `Monitor` trait; the repeated name is
// intentional so each public trait can live in its matching source file.
#![allow(clippy::module_inception)]

mod arc_mock_monitor;
mod arc_parking_lot_monitor;
mod arc_std_monitor;
#[cfg(feature = "async")]
mod arc_tokio_monitor;
#[cfg(feature = "async")]
mod async_condition_waiter;
#[cfg(feature = "async")]
mod async_monitor;
#[cfg(feature = "async")]
mod async_monitor_future;
#[cfg(feature = "async")]
mod async_notification_waiter;
#[cfg(feature = "async")]
mod async_timeout_condition_waiter;
#[cfg(feature = "async")]
mod async_timeout_notification_waiter;
mod condition_waiter;
mod mock_monitor;
mod monitor;
mod notification_waiter;
mod notifier;
mod parking_lot_monitor;
mod parking_lot_monitor_guard;
#[cfg(feature = "async")]
mod shared_async_monitor;
mod shared_monitor;
mod std_monitor;
mod std_monitor_guard;
mod timeout_condition_waiter;
mod timeout_notification_waiter;
#[cfg(feature = "async")]
mod tokio_monitor;
mod wait_timeout_result;
mod wait_timeout_status;

pub use arc_mock_monitor::ArcMockMonitor;
pub use arc_parking_lot_monitor::ArcParkingLotMonitor;
pub use arc_std_monitor::ArcStdMonitor;
#[cfg(feature = "async")]
pub use arc_tokio_monitor::ArcTokioMonitor;
#[cfg(feature = "async")]
pub use async_condition_waiter::AsyncConditionWaiter;
#[cfg(feature = "async")]
pub use async_monitor::AsyncMonitor;
#[cfg(feature = "async")]
pub use async_monitor_future::AsyncMonitorFuture;
#[cfg(feature = "async")]
pub use async_notification_waiter::AsyncNotificationWaiter;
#[cfg(feature = "async")]
pub use async_timeout_condition_waiter::AsyncTimeoutConditionWaiter;
#[cfg(feature = "async")]
pub use async_timeout_notification_waiter::AsyncTimeoutNotificationWaiter;
pub use condition_waiter::ConditionWaiter;
pub use mock_monitor::MockMonitor;
pub use monitor::Monitor;
pub use notification_waiter::NotificationWaiter;
pub use notifier::Notifier;
pub use parking_lot_monitor::ParkingLotMonitor;
pub use parking_lot_monitor_guard::ParkingLotMonitorGuard;
#[cfg(feature = "async")]
pub use shared_async_monitor::SharedAsyncMonitor;
pub use shared_monitor::SharedMonitor;
pub use std_monitor::StdMonitor;
pub use std_monitor_guard::StdMonitorGuard;
pub use timeout_condition_waiter::TimeoutConditionWaiter;
pub use timeout_notification_waiter::TimeoutNotificationWaiter;
#[cfg(feature = "async")]
pub use tokio_monitor::TokioMonitor;
pub use wait_timeout_result::WaitTimeoutResult;
pub use wait_timeout_status::WaitTimeoutStatus;
