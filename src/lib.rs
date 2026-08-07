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
pub use lock::AsyncDataLock;
#[cfg(feature = "async-lock")]
pub use lock::AsyncLock;
#[cfg(feature = "async-lock")]
pub use lock::AsyncReadLock;
#[cfg(feature = "async-lock")]
pub use lock::AsyncReadWriteLock;
#[cfg(feature = "async-lock")]
pub use lock::AsyncWriteLock;
pub use lock::DataLock;
pub use lock::ExclusiveLock;
pub use lock::Lock;
pub use lock::ReadLock;
pub use lock::ReadWriteLock;
pub use lock::TryLockError;
pub use lock::WriteLock;
#[cfg(all(feature = "loom-model", loom))]
#[doc(hidden)]
pub mod test_util;
#[cfg(feature = "async-monitor")]
pub use monitor::AsyncConditionWaiter;
#[cfg(feature = "async-monitor")]
pub use monitor::AsyncMonitor;
#[cfg(feature = "async-monitor")]
pub use monitor::AsyncTimedMonitor;
#[cfg(feature = "async-monitor")]
pub use monitor::AsyncTimeoutConditionWaiter;
#[cfg(feature = "monitor")]
pub use monitor::ConditionWaiter;
#[cfg(feature = "monitor")]
pub use monitor::Monitor;
#[cfg(feature = "monitor")]
pub use monitor::Notifier;
#[cfg(all(feature = "monitor", feature = "parking-lot"))]
pub use monitor::ParkingLotMonitor;
#[cfg(all(feature = "monitor", feature = "parking-lot"))]
pub use monitor::ParkingLotMonitorGuard;
#[cfg(feature = "async-monitor")]
pub use monitor::SharedAsyncMonitor;
#[cfg(feature = "monitor")]
pub use monitor::SharedMonitor;
#[cfg(feature = "monitor")]
pub use monitor::StdMonitor;
#[cfg(feature = "monitor")]
pub use monitor::StdMonitorGuard;
#[cfg(feature = "monitor")]
pub use monitor::TimedMonitor;
#[cfg(feature = "monitor")]
pub use monitor::TimeoutConditionWaiter;
#[cfg(feature = "async-monitor")]
pub use monitor::TokioMonitor;
#[cfg(feature = "monitor")]
pub use monitor::WaitTimeoutResult;
#[cfg(feature = "monitor")]
pub use monitor::WaitTimeoutStatus;
