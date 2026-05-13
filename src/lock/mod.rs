/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Lock Module
//!
//! Provides synchronous lock abstractions, monitor re-exports, and
//! feature-gated asynchronous lock abstractions. This module offers unified
//! interfaces for different types of locks through traits, making it easier to
//! write generic code that works with multiple lock types.
//!

// 子模块 `lock` 存放同步锁 trait `Lock`，与父模块同名是刻意分层；避免 clippy::module_inception 误报
#![allow(clippy::module_inception)]

// Trait definitions
#[cfg(feature = "async")]
mod async_lock;
mod lock;
mod try_lock_error;

// Implementations
#[cfg(feature = "async")]
mod arc_async_mutex;
#[cfg(feature = "async")]
mod arc_async_rw_lock;
mod arc_mutex;
mod arc_rw_lock;
mod arc_std_mutex;

// Re-export traits
// Re-export implementations
pub use crate::monitor::{
    ArcMonitor,
    ArcStdMonitor,
    Monitor,
    MonitorGuard,
    StdMonitor,
    StdMonitorGuard,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
#[cfg(feature = "async")]
pub use arc_async_mutex::ArcAsyncMutex;
#[cfg(feature = "async")]
pub use arc_async_rw_lock::ArcAsyncRwLock;
pub use arc_mutex::ArcMutex;
pub use arc_rw_lock::ArcRwLock;
pub use arc_std_mutex::ArcStdMutex;
#[cfg(feature = "async")]
pub use async_lock::AsyncLock;
pub use lock::Lock;
pub use try_lock_error::TryLockError;
