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
//! Public API items are re-exported from the crate root; the internal `lock`
//! and `monitor` modules are implementation details. Start with [`Lock`] for
//! synchronous lock acquisition. Use [`ExclusiveLock`] when generic code
//! needs mutual exclusion:
//!
//! ```rust
//! use qubit_lock::Lock;
//!
//! let lock = std::sync::Mutex::new(0);
//! let mut value = Lock::lock(&lock);
//! *value += 1;
//! assert_eq!(*value, 1);
//! ```
//!
//! Read-write locks expose explicit read and write modes through
//! [`ReadWriteLock`]:
//!
//! ```rust
//! use qubit_lock::{Lock, ReadWriteLock};
//!
//! let lock = std::sync::RwLock::new(0);
//! let read = lock.read_lock();
//! assert_eq!(*Lock::lock(&read), 0);
//! let write = lock.write_lock();
//! let mut value = Lock::lock(&write);
//! *value = 1;
//! drop(value);
//! assert_eq!(*Lock::lock(&read), 1);
//! ```
//!
//! Monitors provide closure-based access to protected data. `Monitor` and
//! `AsyncMonitor` combine state access, notification, and untimed predicate
//! waits. `TimedMonitor` and `AsyncTimedMonitor` additionally support
//! timeout-based waits:
//!
//! ```rust
//! # #[cfg(feature = "monitor")]
//! # {
//! use qubit_lock::StdMonitor;
//!
//! let monitor = StdMonitor::new(0);
//! monitor.with_write(|value| *value = 1);
//! assert_eq!(monitor.with_read(|value| *value), 1);
//! # }
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
    ExclusiveLock,
    Lock,
    ReadLock,
    ReadWriteLock,
    TryLockError,
    WriteLock,
};
#[cfg(all(feature = "loom-model", loom))]
#[doc(hidden)]
pub mod test_util;
#[cfg(feature = "async-monitor")]
pub use monitor::{
    AsyncConditionWaiter,
    AsyncMonitor,
    AsyncTimedMonitor,
    AsyncTimeoutConditionWaiter,
    SharedAsyncMonitor,
    TokioMonitor,
};
#[cfg(feature = "monitor")]
pub use monitor::{
    ConditionWaiter,
    Monitor,
    Notifier,
    SharedMonitor,
    StdMonitor,
    StdMonitorGuard,
    TimedMonitor,
    TimeoutConditionWaiter,
    WaitTimeoutResult,
    WaitTimeoutStatus,
};
#[cfg(all(feature = "monitor", feature = "parking-lot"))]
pub use monitor::{
    ParkingLotMonitor,
    ParkingLotMonitorGuard,
};
