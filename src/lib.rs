/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
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
//! - Monitor-style coordination built on `Mutex` plus `Condvar`.
//! - A reusable double-checked locking executor.
//!
//! # Author
//!
//! Haixing Hu

pub mod double_checked;
pub mod lock;

pub use double_checked::{
    DoubleCheckedLockExecutor,
    ExecutionContext,
    ExecutionLogger,
    ExecutionResult,
    ExecutorBuilder,
    ExecutorError,
    ExecutorLockBuilder,
    ExecutorReadyBuilder,
};
pub use lock::{
    ArcAsyncMutex,
    ArcAsyncRwLock,
    ArcMonitor,
    ArcMutex,
    ArcRwLock,
    AsyncLock,
    Lock,
    Monitor,
    MonitorGuard,
    TryLockError,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
