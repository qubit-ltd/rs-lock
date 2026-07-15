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
//! - Tokio monitors behind `async`, and deterministic mock monitors behind
//!   `mock`. Enable both features for asynchronous mock waits.
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
//! Notification-only waiting is intentionally not part of the public API.
//!
//! ```compile_fail
//! use qubit_lock::{
//!     AsyncNotificationWaiter,
//!     AsyncTimeoutNotificationWaiter,
//!     NotificationWaiter,
//!     TimeoutNotificationWaiter,
//! };
//! ```
//!
//! Concrete monitors likewise expose only predicate-based waiting.
//!
//! ```compile_fail
//! use qubit_lock::StdMonitor;
//!
//! let monitor = StdMonitor::new(false);
//! monitor.wait();
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
#[cfg(feature = "mock")]
pub use monitor::{
    ArcMockMonitor,
    MockMonitor,
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
    AsyncMonitorFuture,
    AsyncTimeoutConditionWaiter,
    SharedAsyncMonitor,
    TokioMonitor,
};
