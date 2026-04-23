/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Executor Error
//!
//! Provides executor error types for the double-checked lock executor.
//!
//! # Author
//!
//! Haixing Hu

use std::error::Error;
use std::fmt;

/// Executor error types
///
/// Defines various error conditions that can occur during executor
/// operation, including task execution errors, prepare failures, prepare
/// commit failures, and prepare rollback failures.
///
/// # Type Parameters
///
/// * `E` - The original error type from task execution
///
/// # Examples
///
/// ```rust
/// use qubit_lock::double_checked::ExecutorError;
///
/// let error: ExecutorError<String> =
///     ExecutorError::TaskFailed("task failed".to_string());
/// println!("Error: {}", error);
///
/// let error_with_msg: ExecutorError<String> =
///     ExecutorError::PrepareFailed("Service is not running".to_string());
/// println!("Error: {}", error_with_msg);
/// ```
///
/// # Author
///
/// Haixing Hu
///
#[derive(Debug)]
pub enum ExecutorError<E>
where
    E: fmt::Display,
{
    /// Task execution failed with original error
    TaskFailed(E),

    /// Preparation action failed
    PrepareFailed(String),

    /// Commit action for a successfully completed prepare action failed.
    PrepareCommitFailed(String),

    /// Rollback action for a successfully completed prepare action failed.
    PrepareRollbackFailed {
        /// The original error that triggered the rollback
        original: String,
        /// The error that occurred during prepare rollback
        rollback: String,
    },

    /// Lock poisoned error
    LockPoisoned(String),
}

impl<E> fmt::Display for ExecutorError<E>
where
    E: fmt::Display,
{
    /// Formats this executor error for user-facing diagnostics.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the human-readable error text.
    ///
    /// # Returns
    ///
    /// [`fmt::Result`] from writing the formatted error text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::TaskFailed(e) => {
                write!(f, "Task execution failed: {}", e)
            }
            ExecutorError::PrepareFailed(msg) => {
                write!(f, "Preparation action failed: {}", msg)
            }
            ExecutorError::PrepareCommitFailed(msg) => {
                write!(f, "Prepare commit action failed: {}", msg)
            }
            ExecutorError::PrepareRollbackFailed { original, rollback } => {
                write!(
                    f,
                    "Prepare rollback failed: original error = {}, rollback error = {}",
                    original, rollback
                )
            }
            ExecutorError::LockPoisoned(msg) => {
                write!(f, "Lock poisoned: {}", msg)
            }
        }
    }
}

impl<E> Error for ExecutorError<E> where E: fmt::Display + fmt::Debug {}
