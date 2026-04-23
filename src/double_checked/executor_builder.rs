/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Initial builder for [`super::DoubleCheckedLockExecutor`].
//!
//! # Author
//!
//! Haixing Hu

use std::marker::PhantomData;

use super::{
    ExecutionLogger,
    executor_lock_builder::ExecutorLockBuilder,
};
use crate::lock::Lock;

/// Initial builder for [`super::DoubleCheckedLockExecutor`].
///
/// This state has no lock yet. Call [`Self::on`] to attach the lock.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug, Default, Clone)]
pub struct ExecutorBuilder {
    /// Logger carried forward to later builder states.
    logger: ExecutionLogger,
}

impl ExecutorBuilder {
    /// Configures logging when the double-checked condition is not met.
    #[inline]
    pub fn log_unmet_condition(mut self, level: log::Level, message: impl Into<String>) -> Self {
        self.logger.set_unmet_condition(Some(level), message);
        self
    }

    /// Configures logging when the prepare action fails.
    #[inline]
    pub fn log_prepare_failure(
        mut self,
        level: log::Level,
        message_prefix: impl Into<String>,
    ) -> Self {
        self.logger.set_prepare_failure(Some(level), message_prefix);
        self
    }

    /// Configures logging when the prepare commit action fails.
    #[inline]
    pub fn log_prepare_commit_failure(
        mut self,
        level: log::Level,
        message_prefix: impl Into<String>,
    ) -> Self {
        self.logger
            .set_prepare_commit_failure(Some(level), message_prefix);
        self
    }

    /// Configures logging when the prepare rollback action fails.
    #[inline]
    pub fn log_prepare_rollback_failure(
        mut self,
        level: log::Level,
        message_prefix: impl Into<String>,
    ) -> Self {
        self.logger
            .set_prepare_rollback_failure(Some(level), message_prefix);
        self
    }

    /// Attaches the lock protected by this executor.
    ///
    /// # Parameters
    ///
    /// * `lock` - The lock handle. Arc-based lock wrappers can be cloned and
    ///   stored here for reusable execution.
    ///
    /// # Returns
    ///
    /// The builder state that can configure the required tester.
    #[inline]
    pub fn on<L, T>(self, lock: L) -> ExecutorLockBuilder<L, T>
    where
        L: Lock<T>,
    {
        ExecutorLockBuilder {
            lock,
            logger: self.logger,
            _phantom: PhantomData,
        }
    }
}
