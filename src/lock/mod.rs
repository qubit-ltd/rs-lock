// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Lock Module
//!
//! Provides synchronous lock abstractions and feature-gated asynchronous lock
//! abstractions. This module offers unified interfaces for different types of
//! locks through traits, making it easier to write generic code that works with
//! multiple lock types.

// The nested `lock` module owns the `Lock` trait; the repeated name is an
// intentional module boundary.
#![allow(clippy::module_inception)]

#[cfg(feature = "async-lock")]
mod async_data_lock;
#[cfg(feature = "async-lock")]
mod async_lock;
#[cfg(feature = "async-lock")]
mod async_read_lock;
#[cfg(feature = "async-lock")]
mod async_read_write_lock;
#[cfg(feature = "async-lock")]
mod async_write_lock;
mod data_lock;
mod lock;
mod read_lock;
mod read_write_lock;
mod try_lock_error;
mod write_lock;

#[cfg(feature = "async-lock")]
pub use async_data_lock::AsyncDataLock;
#[cfg(feature = "async-lock")]
pub use async_lock::AsyncLock;
#[cfg(feature = "async-lock")]
pub use async_read_lock::AsyncReadLock;
#[cfg(feature = "async-lock")]
pub use async_read_write_lock::AsyncReadWriteLock;
#[cfg(feature = "async-lock")]
pub use async_write_lock::AsyncWriteLock;
pub use data_lock::DataLock;
pub use lock::Lock;
pub use read_lock::ReadLock;
pub use read_write_lock::ReadWriteLock;
pub use try_lock_error::TryLockError;
pub use write_lock::WriteLock;
