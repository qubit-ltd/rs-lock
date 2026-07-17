// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Lock
//!
//! Lock utilities for the Qubit Rust libraries.
//!
//! The crate provides:
//!
//! - Synchronous lock wrappers with `Arc` integrated internally.
//! - Optional asynchronous Tokio-based lock wrappers behind the `async`
//!   feature.
//! - Blocking parking_lot and standard-library monitor implementations.
//! - Tokio monitors behind the optional `async` feature.
//!
//! Public API items are re-exported from the crate root. The internal
//! `lock` and `monitor` modules are implementation details and are not public
//! import paths.
//!
//! ```compile_fail
//! use qubit_lock::lock::Lock;
//! ```
//!
//! ```compile_fail
//! use qubit_lock::monitor::Monitor;
//! ```
//!
//! Notification-only waiting traits are intentionally not part of the public
//! API.
//!
//! ```compile_fail
//! use qubit_lock::NotificationWaiter;
//! ```
//!
//! ```compile_fail
//! use qubit_lock::TimeoutNotificationWaiter;
//! ```
//!
//! ```compile_fail
//! use qubit_lock::AsyncNotificationWaiter;
//! ```
//!
//! ```compile_fail
//! use qubit_lock::AsyncTimeoutNotificationWaiter;
//! ```
//!
//! The implementation-specific boxed async monitor future alias is not part
//! of the public API.
//!
//! ```compile_fail
//! use qubit_lock::AsyncMonitorFuture;
//! ```
//!
//! Concrete monitors and Arc wrappers likewise expose only predicate-based
//! waiting.
//!
//! ```compile_fail
//! use qubit_lock::StdMonitor;
//!
//! let monitor = StdMonitor::new(false);
//! monitor.wait();
//! ```
//!
//! ```compile_fail
//! use std::time::Duration;
//!
//! use qubit_lock::ArcStdMonitor;
//!
//! let monitor = ArcStdMonitor::new(false);
//! let _ = monitor.wait_for(Duration::ZERO);
//! ```
//!
//! ```compile_fail
//! use qubit_lock::ArcTokioMonitor;
//!
//! async fn wait_for_notification(monitor: &ArcTokioMonitor<bool>) {
//!     monitor.wait_async().await;
//! }
//! ```
//!
//! ```compile_fail
//! use std::time::Duration;
//!
//! use qubit_lock::ArcTokioMonitor;
//!
//! async fn wait_for_notification(monitor: &ArcTokioMonitor<bool>) {
//!     let _ = monitor.wait_for_async(Duration::ZERO).await;
//! }
//! ```

mod lock;
mod monitor;
#[cfg(feature = "async")]
pub use lock::{
    ArcAsyncMutex,
    ArcAsyncRwLock,
    AsyncLock,
};
pub use lock::{
    ArcMutex,
    ArcRwLock,
    ArcStdMutex,
    ArcStdRwLock,
    Lock,
    TryLockError,
};
pub use monitor::{
    ArcParkingLotMonitor,
    ArcStdMonitor,
    ConditionWaiter,
    Monitor,
    Notifier,
    ParkingLotMonitor,
    ParkingLotMonitorGuard,
    SharedMonitor,
    StdMonitor,
    StdMonitorGuard,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
#[cfg(feature = "async")]
pub use monitor::{
    ArcTokioMonitor,
    AsyncConditionWaiter,
    AsyncMonitor,
    AsyncTimeoutConditionWaiter,
    SharedAsyncMonitor,
    TokioMonitor,
};
