/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Ready builder for [`super::DoubleCheckedLockExecutor`] (tester set, optional
//! prepare hooks).
//!
//! # Author
//!
//! Haixing Hu

use std::{
    fmt::Display,
    marker::PhantomData,
};

use qubit_function::{
    ArcRunnable,
    ArcTester,
    Runnable,
};

use super::{
    ExecutionLogger,
    double_checked_lock_executor::DoubleCheckedLockExecutor,
};
use crate::lock::Lock;

/// Builder state after the required condition tester has been configured.
///
/// This state can configure prepare lifecycle callbacks and build the final
/// [`DoubleCheckedLockExecutor`].
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
pub struct ExecutorReadyBuilder<L, T> {
    /// The lock to store in the executor.
    pub(in crate::double_checked) lock: L,

    /// Required condition tester.
    pub(in crate::double_checked) tester: ArcTester,

    /// Logger used by the executor.
    pub(in crate::double_checked) logger: ExecutionLogger,

    /// Optional action executed after the first check and before locking.
    pub(in crate::double_checked) prepare_action: Option<ArcRunnable<String>>,

    /// Optional action executed when prepare must be rolled back.
    pub(in crate::double_checked) rollback_prepare_action: Option<ArcRunnable<String>>,

    /// Optional action executed when prepare should be committed.
    pub(in crate::double_checked) commit_prepare_action: Option<ArcRunnable<String>>,

    /// Carries the protected data type.
    pub(in crate::double_checked) _phantom: PhantomData<fn() -> T>,
}

impl<L, T> ExecutorReadyBuilder<L, T>
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

    /// Sets the prepare action.
    ///
    /// The action runs after the first condition check succeeds and before the
    /// lock is acquired. If it succeeds, the executor will later run either
    /// rollback or commit according to the final task result.
    ///
    /// Errors returned by this action are converted to [`String`] and reported
    /// by execution methods as [`super::ExecutionResult::Failed`].
    ///
    /// # Parameters
    ///
    /// * `prepare_action` - The fallible action to run before locking.
    ///
    /// # Returns
    ///
    /// This builder with prepare configured.
    #[inline]
    pub fn prepare<Rn, E>(mut self, prepare_action: Rn) -> Self
    where
        Rn: Runnable<E> + Send + 'static,
        E: Display + Send + 'static,
    {
        let mut action = prepare_action;
        self.prepare_action = Some(ArcRunnable::new(move || {
            action.run().map_err(|error| error.to_string())
        }));
        self
    }

    /// Sets the rollback action for a successfully completed prepare action.
    ///
    /// Errors returned by this action are converted to [`String`] and replace
    /// the original execution result with a prepare-rollback failure.
    ///
    /// # Parameters
    ///
    /// * `rollback_prepare_action` - The action to run if the second condition
    ///   check or task execution fails after prepare succeeds.
    ///
    /// # Returns
    ///
    /// This builder with prepare rollback configured.
    #[inline]
    pub fn rollback_prepare<Rn, E>(mut self, rollback_prepare_action: Rn) -> Self
    where
        Rn: Runnable<E> + Send + 'static,
        E: Display + Send + 'static,
    {
        let mut action = rollback_prepare_action;
        self.rollback_prepare_action = Some(ArcRunnable::new(move || {
            action.run().map_err(|error| error.to_string())
        }));
        self
    }

    /// Sets the commit action for a successfully completed prepare action.
    ///
    /// Errors returned by this action are converted to [`String`] and replace
    /// an otherwise successful execution result with a prepare-commit failure.
    ///
    /// # Parameters
    ///
    /// * `commit_prepare_action` - The action to run if the task succeeds after
    ///   prepare succeeds.
    ///
    /// # Returns
    ///
    /// This builder with prepare commit configured.
    #[inline]
    pub fn commit_prepare<Rn, E>(mut self, commit_prepare_action: Rn) -> Self
    where
        Rn: Runnable<E> + Send + 'static,
        E: Display + Send + 'static,
    {
        let mut action = commit_prepare_action;
        self.commit_prepare_action = Some(ArcRunnable::new(move || {
            action.run().map_err(|error| error.to_string())
        }));
        self
    }

    /// Builds the reusable executor.
    ///
    /// # Returns
    ///
    /// A [`DoubleCheckedLockExecutor`] containing the configured lock, tester,
    /// execution logger, and prepare lifecycle callbacks.
    #[inline]
    pub fn build(self) -> DoubleCheckedLockExecutor<L, T> {
        DoubleCheckedLockExecutor::new(self)
    }
}
