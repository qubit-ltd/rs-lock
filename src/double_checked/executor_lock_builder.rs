/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Builder state after a lock has been attached for
//! [`super::DoubleCheckedLockExecutor`].
//!
//! # Author
//!
//! Haixing Hu

use std::marker::PhantomData;

use qubit_function::Tester;

use super::{
    ExecutionLogger,
    executor_ready_builder::ExecutorReadyBuilder,
};
use crate::lock::Lock;

/// Builder state after a lock has been attached.
///
/// Call [`Self::when`] to configure the required condition tester.
///
/// # Type Parameters
///
/// * `L` - The lock type implementing [`Lock<T>`].
/// * `T` - The data type protected by the lock.
///
/// # Author
///
/// Haixing Hu
#[derive(Clone)]
pub struct ExecutorLockBuilder<L, T> {
    /// The lock to store in the executor.
    pub(in crate::double_checked) lock: L,

    /// Logger carried forward to the ready builder state.
    pub(in crate::double_checked) logger: ExecutionLogger,

    /// Carries the protected data type.
    pub(in crate::double_checked) _phantom: PhantomData<fn() -> T>,
}

impl<L, T> ExecutorLockBuilder<L, T>
where
    L: Lock<T>,
{
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

    /// Configures the required double-checked condition.
    ///
    /// The tester is executed outside and inside the lock. State read by the
    /// outside check must be safe to access without this executor's lock.
    ///
    /// # Parameters
    ///
    /// * `tester` - The reusable condition tester.
    ///
    /// # Returns
    ///
    /// The builder state that can configure prepare callbacks and build the
    /// executor.
    #[inline]
    pub fn when<Tst>(self, tester: Tst) -> ExecutorReadyBuilder<L, T>
    where
        Tst: Tester + Send + Sync + 'static,
    {
        ExecutorReadyBuilder {
            lock: self.lock,
            tester: tester.into_arc(),
            logger: self.logger,
            prepare_action: None,
            rollback_prepare_action: None,
            commit_prepare_action: None,
            _phantom: PhantomData,
        }
    }
}
