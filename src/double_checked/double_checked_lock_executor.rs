/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Double-Checked Lock Executor
//!
//! Provides a reusable executor for double-checked locking workflows.
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
    Callable,
    CallableWith,
    Runnable,
    RunnableWith,
    Tester,
};

use super::{
    ExecutionContext,
    ExecutionLogger,
    ExecutionResult,
    executor_builder::ExecutorBuilder,
    executor_ready_builder::ExecutorReadyBuilder,
};
use crate::lock::Lock;

/// Reusable double-checked lock executor.
///
/// The executor owns the lock handle, condition tester, execution logger, and
/// optional prepare lifecycle callbacks. Each execution performs:
///
/// 1. A first condition check outside the lock.
/// 2. Optional prepare action.
/// 3. Lock acquisition.
/// 4. A second condition check inside the lock.
/// 5. The submitted task.
/// 6. Optional prepare commit or rollback after the lock is released.
///
/// The tester is intentionally run both outside and inside the lock. Any state
/// read by the first check must therefore use atomics or another synchronization
/// mechanism that is safe without this executor's lock.
///
/// # Type Parameters
///
/// * `L` - The lock type implementing [`Lock<T>`].
/// * `T` - The data type protected by the lock.
///
/// # Examples
///
/// Use [`DoubleCheckedLockExecutor::builder`] to attach a lock (for example
/// [`crate::ArcMutex`]), set a [`Tester`](qubit_function::Tester) with
/// [`ExecutorLockBuilder::when`], then call [`Self::call`], [`Self::execute`],
/// [`Self::call_with`], or [`Self::execute_with`] on the built executor.
///
/// ```rust
/// use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
///
/// use qubit_lock::{ArcMutex, DoubleCheckedLockExecutor, Lock};
/// use qubit_lock::double_checked::ExecutionResult;
///
/// fn main() {
///     let data = ArcMutex::new(10);
///     let skip = Arc::new(AtomicBool::new(false));
///
///     let executor = DoubleCheckedLockExecutor::builder()
///         .on(data.clone())
///         .when({
///             let skip = skip.clone();
///             move || !skip.load(Ordering::Acquire)
///         })
///         .build();
///
///     let updated = executor
///         .call_with(|value: &mut i32| {
///             *value += 5;
///             Ok::<i32, std::io::Error>(*value)
///         })
///         .get_result();
///
///     assert!(matches!(updated, ExecutionResult::Success(15)));
///     assert_eq!(data.read(|value| *value), 15);
///
///     skip.store(true, Ordering::Release);
///     let skipped = executor
///         .call_with(|value: &mut i32| {
///             *value += 1;
///             Ok::<i32, std::io::Error>(*value)
///         })
///         .get_result();
///
///     assert!(matches!(skipped, ExecutionResult::ConditionNotMet));
///     assert_eq!(data.read(|value| *value), 15);
/// }
/// ```
///
/// # Author
///
/// Haixing Hu
#[derive(Clone)]
pub struct DoubleCheckedLockExecutor<L = (), T = ()> {
    /// The lock protecting the target data.
    lock: L,

    /// Condition checked before and after acquiring the lock.
    tester: ArcTester,

    /// Logger for unmet conditions and prepare lifecycle failures.
    logger: ExecutionLogger,

    /// Optional action executed after the first check and before locking.
    prepare_action: Option<ArcRunnable<String>>,

    /// Optional action executed when prepare must be rolled back.
    rollback_prepare_action: Option<ArcRunnable<String>>,

    /// Optional action executed when prepare should be committed.
    commit_prepare_action: Option<ArcRunnable<String>>,

    /// Carries the protected data type.
    _phantom: PhantomData<fn() -> T>,
}

impl DoubleCheckedLockExecutor<(), ()> {
    /// Creates a builder for a reusable double-checked lock executor.
    ///
    /// # Returns
    ///
    /// A builder in the initial state. Attach a lock with
    /// [`ExecutorBuilder::on`], then configure a tester with
    /// [`ExecutorLockBuilder::when`].
    #[inline]
    pub fn builder() -> ExecutorBuilder {
        ExecutorBuilder::default()
    }
}

impl<L, T> DoubleCheckedLockExecutor<L, T>
where
    L: Lock<T>,
{
    /// Assembles an executor from the ready builder state.
    ///
    /// # Parameters
    ///
    /// * `builder` - Ready builder carrying the lock, tester, logger, and
    ///   prepare lifecycle callbacks.
    ///
    /// # Returns
    ///
    /// A reusable executor containing the supplied builder state.
    #[inline]
    pub fn new(builder: ExecutorReadyBuilder<L, T>) -> Self {
        Self {
            lock: builder.lock,
            tester: builder.tester,
            logger: builder.logger,
            prepare_action: builder.prepare_action,
            rollback_prepare_action: builder.rollback_prepare_action,
            commit_prepare_action: builder.commit_prepare_action,
            _phantom: builder._phantom,
        }
    }

    /// Executes a zero-argument callable while holding the write lock.
    ///
    /// Use [`Self::call_with`] when the task needs direct mutable access to the
    /// protected data.
    ///
    /// # Parameters
    ///
    /// * `task` - The callable task to execute after both condition checks pass.
    ///
    /// # Returns
    ///
    /// An [`ExecutionContext`] containing success, unmet-condition, or failure
    /// information.
    #[inline]
    pub fn call<C, R, E>(&self, task: C) -> ExecutionContext<R, E>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Display + Send + 'static,
    {
        let mut task = task;
        let result = self.execute_with_write_lock(move |_data| task.call());
        ExecutionContext::new(result)
    }

    /// Executes a zero-argument runnable while holding the write lock.
    ///
    /// # Parameters
    ///
    /// * `task` - The runnable task to execute after both condition checks pass.
    ///
    /// # Returns
    ///
    /// An [`ExecutionContext`] containing success, unmet-condition, or failure
    /// information.
    #[inline]
    pub fn execute<Rn, E>(&self, task: Rn) -> ExecutionContext<(), E>
    where
        Rn: Runnable<E> + Send + 'static,
        E: Display + Send + 'static,
    {
        let mut task = task;
        let result = self.execute_with_write_lock(move |_data| task.run());
        ExecutionContext::new(result)
    }

    /// Executes a callable with mutable access to the protected data.
    ///
    /// # Parameters
    ///
    /// * `task` - The callable receiving `&mut T` after both condition checks
    ///   pass.
    ///
    /// # Returns
    ///
    /// An [`ExecutionContext`] containing success, unmet-condition, or failure
    /// information.
    #[inline]
    pub fn call_with<C, R, E>(&self, task: C) -> ExecutionContext<R, E>
    where
        C: CallableWith<T, R, E> + Send + 'static,
        R: Send + 'static,
        E: Display + Send + 'static,
    {
        let mut task = task;
        let result = self.execute_with_write_lock(move |data| task.call_with(data));
        ExecutionContext::new(result)
    }

    /// Executes a runnable with mutable access to the protected data.
    ///
    /// # Parameters
    ///
    /// * `task` - The runnable receiving `&mut T` after both condition checks
    ///   pass.
    ///
    /// # Returns
    ///
    /// An [`ExecutionContext`] containing success, unmet-condition, or failure
    /// information.
    #[inline]
    pub fn execute_with<Rn, E>(&self, task: Rn) -> ExecutionContext<(), E>
    where
        Rn: RunnableWith<T, E> + Send + 'static,
        E: Display + Send + 'static,
    {
        let mut task = task;
        let result = self.execute_with_write_lock(move |data| task.run_with(data));
        ExecutionContext::new(result)
    }

    /// Runs the configured double-checked sequence under a write lock.
    ///
    /// # Parameters
    ///
    /// * `task` - The task to run with mutable access after both condition
    ///   checks pass.
    ///
    /// # Returns
    ///
    /// The final execution result, including prepare finalization.
    ///
    /// # Errors
    ///
    /// Task errors are captured as [`ExecutionResult::Failed`] with
    /// [`super::ExecutorError::TaskFailed`]. Prepare, commit, and rollback
    /// failures are also captured in the returned [`ExecutionResult`] rather
    /// than returned as a separate `Result`.
    fn execute_with_write_lock<R, E, F>(&self, task: F) -> ExecutionResult<R, E>
    where
        E: Display + Send + 'static,
        F: FnOnce(&mut T) -> Result<R, E>,
    {
        if !self.tester.test() {
            self.log_unmet_condition();
            return ExecutionResult::unmet();
        }

        let prepare_completed = match self.run_prepare_action() {
            Ok(completed) => completed,
            Err(error) => return ExecutionResult::prepare_failed(error),
        };

        let result = self.lock.write(|data| {
            if !self.tester.test() {
                self.log_unmet_condition();
                return ExecutionResult::unmet();
            }
            match task(data) {
                Ok(value) => ExecutionResult::success(value),
                Err(error) => ExecutionResult::task_failed(error),
            }
        });

        if prepare_completed {
            self.finalize_prepare(result)
        } else {
            result
        }
    }

    /// Executes the optional prepare action.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if prepare exists and succeeds, `Ok(false)` if no prepare
    /// action is configured, or `Err(message)` if prepare fails.
    ///
    /// # Errors
    ///
    /// Returns `Err(message)` when the configured prepare action returns an
    /// error. The message is already converted to [`String`].
    fn run_prepare_action(&self) -> Result<bool, String> {
        let Some(mut prepare_action) = self.prepare_action.clone() else {
            return Ok(false);
        };
        if let Err(error) = prepare_action.run() {
            self.logger.log_prepare_failed(&error);
            return Err(error);
        }
        Ok(true)
    }

    /// Commits or rolls back a successfully completed prepare action.
    ///
    /// This method runs after the write lock has been released.
    ///
    /// # Parameters
    ///
    /// * `result` - Result produced by the condition check and task execution.
    ///
    /// # Returns
    ///
    /// `result` unchanged when no finalization action fails. Returns a failed
    /// result when prepare commit or prepare rollback fails.
    fn finalize_prepare<R, E>(&self, mut result: ExecutionResult<R, E>) -> ExecutionResult<R, E>
    where
        E: Display + Send + 'static,
    {
        if result.is_success() {
            if let Some(mut commit_prepare_action) = self.commit_prepare_action.clone()
                && let Err(error) = commit_prepare_action.run()
            {
                self.logger.log_prepare_commit_failed(&error);
                result = ExecutionResult::prepare_commit_failed(error);
            }
            return result;
        }

        let original = if let ExecutionResult::Failed(error) = &result {
            error.to_string()
        } else {
            "Condition not met".to_string()
        };

        if let Some(mut rollback_prepare_action) = self.rollback_prepare_action.clone()
            && let Err(error) = rollback_prepare_action.run()
        {
            self.logger.log_prepare_rollback_failed(&error);
            result = ExecutionResult::prepare_rollback_failed(original, error);
        }
        result
    }

    /// Logs that the double-checked condition was not met.
    fn log_unmet_condition(&self) {
        self.logger.log_unmet_condition();
    }
}
