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
//! - Data-independent synchronous RAII lock capabilities.
//! - Closure-based protected-data access capabilities.
//! - Optional asynchronous Tokio lock capabilities behind the `async-lock`
//!   feature.
//! - Standard-library monitors behind the optional `monitor` feature.
//! - parking_lot locks behind `parking-lot`, with parking_lot monitors
//!   available when `monitor` is also enabled.
//! - Tokio monitors behind the optional `async-monitor` feature.
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
//! Read-write locks intentionally expose explicit read and write modes rather
//! than guessing which mode [`Lock`] should acquire.
//!
//! ```compile_fail
//! use std::sync::RwLock;
//!
//! use qubit_lock::Lock;
//!
//! let lock = RwLock::new(());
//! let _guard = Lock::lock(&lock);
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
#[cfg(feature = "monitor")]
mod monitor;
#[cfg(feature = "async-lock")]
pub use lock::{
    AsyncDataLock,
    AsyncLock,
    AsyncReadLock,
    AsyncReadWriteLock,
    AsyncWriteLock,
};
pub use lock::{
    DataLock,
    Lock,
    ReadLock,
    ReadWriteLock,
    TryLockError,
    WriteLock,
};
#[cfg(all(feature = "monitor", feature = "parking-lot"))]
pub use monitor::{
    ArcParkingLotMonitor,
    ParkingLotMonitor,
    ParkingLotMonitorGuard,
};
#[cfg(feature = "monitor")]
pub use monitor::{
    ArcStdMonitor,
    ConditionWaiter,
    Monitor,
    Notifier,
    SharedMonitor,
    StdMonitor,
    StdMonitorGuard,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
#[cfg(feature = "async-monitor")]
pub use monitor::{
    ArcTokioMonitor,
    AsyncConditionWaiter,
    AsyncMonitor,
    AsyncTimeoutConditionWaiter,
    SharedAsyncMonitor,
    TokioMonitor,
};
