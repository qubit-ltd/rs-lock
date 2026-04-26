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
//! Error type for non-blocking lock acquisition.
//!
//! # Author
//!
//! Haixing Hu

use std::fmt;

/// Non-blocking lock acquisition error.
///
/// This error type is used by `try_read` and `try_write` to distinguish
/// immediate lock contention from poisoned lock states. The
/// [`Self::Poisoned`] variant is returned only by lock implementations that
/// support poisoning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryLockError {
    /// The lock could not be acquired immediately because another guard is active.
    WouldBlock,
    /// The lock implementation reports a poisoned state.
    Poisoned,
}

impl fmt::Display for TryLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WouldBlock => f.write_str("lock acquisition would block"),
            Self::Poisoned => f.write_str("lock is poisoned"),
        }
    }
}

impl std::error::Error for TryLockError {}
