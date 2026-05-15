/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Qubit Lock
//!
//! Lock utilities for the Qubit Rust libraries.
//!
//! The crate provides:
//!
//! - Synchronous lock wrappers with `Arc` integrated internally.
//! - Optional asynchronous Tokio-based lock wrappers behind the `async` feature.
//! - Monitor-style coordination built on `parking_lot` and standard-library
//!   `Mutex` plus `Condvar` pairs.
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
    ArcMonitor,
    ArcStdMonitor,
    Monitor,
    MonitorGuard,
    StdMonitor,
    StdMonitorGuard,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
