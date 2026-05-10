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
//! - Asynchronous Tokio-based lock wrappers.
//! - Monitor-style coordination built on `parking_lot` and standard-library
//!   `Mutex` plus `Condvar` pairs.
//!

pub mod lock;
pub mod monitor;
pub use lock::{
    ArcAsyncMutex,
    ArcAsyncRwLock,
    ArcMonitor,
    ArcMutex,
    ArcRwLock,
    ArcStdMonitor,
    ArcStdMutex,
    AsyncLock,
    Lock,
    Monitor,
    MonitorGuard,
    StdMonitor,
    StdMonitorGuard,
    TryLockError,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
