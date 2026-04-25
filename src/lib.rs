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
//!
//! # Author
//!
//! Haixing Hu

pub mod lock;
mod monitor;
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
