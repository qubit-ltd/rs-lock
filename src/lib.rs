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

pub mod lock;
pub mod monitor;
#[cfg(feature = "async")]
pub use lock::{
    ArcAsyncMutex,
    ArcAsyncRwLock,
    AsyncLock,
};
pub use lock::{
    ArcMonitor,
    ArcMutex,
    ArcRwLock,
    ArcStdMonitor,
    ArcStdMutex,
    Lock,
    Monitor,
    MonitorGuard,
    StdMonitor,
    StdMonitorGuard,
    TryLockError,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
