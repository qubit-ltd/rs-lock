/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Try Lock Error
//!
//! Error type for non-blocking lock acquisition on synchronous locks.
//!
//! # Author
//!
//! Haixing Hu

/// Non-blocking lock acquisition error.
///
/// This error type is used by `try_read` and `try_write` to distinguish
/// lock contention from poisoned lock states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryLockError {
    /// The lock is currently held by another thread.
    WouldBlock,
    /// The lock is poisoned due to a panic while the lock was held.
    Poisoned,
}
