/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Execution Logger
//!
//! Logging configuration and helpers for the double-checked lock executor.
//!
//! # Author
//!
//! Haixing Hu

use std::fmt;

/// Logger for double-checked execution (condition unmet, prepare failures,
/// prepare commit failures, and prepare rollback failures).
///
/// Each event has its own optional [`log::Level`] and message. `None` means
/// that event does not emit logs. For prepare-style events the message is a
/// prefix formatted as `"{prefix}: {error}"`.
///
/// [`ExecutionLogger::default`] matches the previous `Option` logger unset
/// behavior: condition-unmet is silent (`None`); prepare lifecycle lines use
/// [`log::Level::Error`] with English default prefixes.
///
/// # Author
///
/// Haixing Hu
#[derive(Debug, Clone)]
pub struct ExecutionLogger {
    /// Log level for the condition-unmet message; `None` skips it.
    pub unmet_condition_level: Option<log::Level>,

    /// Message logged when the execution condition is not met.
    pub unmet_condition_message: String,

    /// Log level for prepare-action failure lines; `None` skips them.
    pub prepare_failed_level: Option<log::Level>,

    /// Prefix for prepare-failure lines, formatted as `"{prefix}: {error}"`.
    pub prepare_failed_message: String,

    /// Log level for prepare-commit failure lines; `None` skips them.
    pub prepare_commit_failed_level: Option<log::Level>,

    /// Prefix for prepare-commit failure lines, formatted as `"{prefix}: {error}"`.
    pub prepare_commit_failed_message: String,

    /// Log level for prepare-rollback failure lines; `None` skips them.
    pub prepare_rollback_failed_level: Option<log::Level>,

    /// Prefix for prepare-rollback failure lines, formatted as
    /// `"{prefix}: {error}"`.
    pub prepare_rollback_failed_message: String,
}

impl Default for ExecutionLogger {
    /// Returns the logger configuration used when the executor builder does not
    /// apply any logging overrides.
    ///
    /// Condition-unmet logging is disabled ([`ExecutionLogger::unmet_condition_level`]
    /// is [`None`]). Prepare lifecycle failures log at [`log::Level::Error`] with
    /// short English default prefixes (see the field defaults on [`ExecutionLogger`]).
    ///
    /// # Returns
    ///
    /// A new [`ExecutionLogger`] with the values described above.
    #[inline]
    fn default() -> Self {
        Self {
            unmet_condition_level: None,
            unmet_condition_message: String::new(),
            prepare_failed_level: Some(log::Level::Error),
            prepare_failed_message: "Prepare action failed".to_string(),
            prepare_commit_failed_level: Some(log::Level::Error),
            prepare_commit_failed_message: "Prepare commit action failed".to_string(),
            prepare_rollback_failed_level: Some(log::Level::Error),
            prepare_rollback_failed_message: "Prepare rollback action failed".to_string(),
        }
    }
}

impl ExecutionLogger {
    /// Updates logging for the case where the double-checked condition is not met
    /// (the tester returns `false` before or after taking the lock).
    ///
    /// When [`Self::unmet_condition_level`] is [`None`], [`Self::log_unmet_condition`]
    /// becomes a no-op. The `message` is still stored and used if the level is set
    /// to [`Some`] later.
    ///
    /// # Parameters
    ///
    /// * `level` - Optional severity for the line written through the `log` crate,
    ///   or [`None`] to disable this event.
    /// * `message` - Full line text (not a prefix); passed to [`log::log!`] as the
    ///   format argument when logging runs.
    #[inline]
    pub fn set_unmet_condition(&mut self, level: Option<log::Level>, message: impl Into<String>) {
        self.unmet_condition_level = level;
        self.unmet_condition_message = message.into();
    }

    /// Updates logging for a failed optional prepare action (before the lock is taken).
    ///
    /// When [`Self::prepare_failed_level`] is [`None`], [`Self::log_prepare_failed`]
    /// becomes a no-op.
    ///
    /// # Parameters
    ///
    /// * `level` - Optional severity for the diagnostic line, or [`None`] to disable.
    /// * `message_prefix` - Text placed before the error; the emitted line has the
    ///   form `"{prefix}: {error}"`.
    #[inline]
    pub fn set_prepare_failure(
        &mut self,
        level: Option<log::Level>,
        message_prefix: impl Into<String>,
    ) {
        self.prepare_failed_level = level;
        self.prepare_failed_message = message_prefix.into();
    }

    /// Updates logging for a failed prepare commit action (after a successful task
    /// when prepare had completed).
    ///
    /// When [`Self::prepare_commit_failed_level`] is [`None`],
    /// [`Self::log_prepare_commit_failed`] becomes a no-op.
    ///
    /// # Parameters
    ///
    /// * `level` - Optional severity for the diagnostic line, or [`None`] to disable.
    /// * `message_prefix` - Text placed before the error; the emitted line has the
    ///   form `"{prefix}: {error}"`.
    #[inline]
    pub fn set_prepare_commit_failure(
        &mut self,
        level: Option<log::Level>,
        message_prefix: impl Into<String>,
    ) {
        self.prepare_commit_failed_level = level;
        self.prepare_commit_failed_message = message_prefix.into();
    }

    /// Updates logging for a failed prepare rollback action (after a failed second
    /// check or task when prepare had completed).
    ///
    /// When [`Self::prepare_rollback_failed_level`] is [`None`],
    /// [`Self::log_prepare_rollback_failed`] becomes a no-op.
    ///
    /// # Parameters
    ///
    /// * `level` - Optional severity for the diagnostic line, or [`None`] to disable.
    /// * `message_prefix` - Text placed before the error; the emitted line has the
    ///   form `"{prefix}: {error}"`.
    #[inline]
    pub fn set_prepare_rollback_failure(
        &mut self,
        level: Option<log::Level>,
        message_prefix: impl Into<String>,
    ) {
        self.prepare_rollback_failed_level = level;
        self.prepare_rollback_failed_message = message_prefix.into();
    }

    /// Emits the condition-unmet log line if enabled.
    ///
    /// Does nothing when [`Self::unmet_condition_level`] is [`None`]. Otherwise
    /// writes [`Self::unmet_condition_message`] through the `log` facade at the
    /// configured level, subject to the crate-wide maximum log level (for example
    /// set via [`log::set_max_level`] or compile-time filters).
    #[inline]
    pub fn log_unmet_condition(&self) {
        let Some(level) = self.unmet_condition_level else {
            return;
        };
        log::log!(level, "{}", self.unmet_condition_message);
    }

    /// Emits a diagnostic line when the prepare action fails.
    ///
    /// Does nothing when [`Self::prepare_failed_level`] is [`None`]. Otherwise
    /// logs `"{prefix}: {err}"` at the configured level via the `log` facade,
    /// where `prefix` is [`Self::prepare_failed_message`], subject to the
    /// crate-wide maximum log level.
    ///
    /// # Type Parameters
    ///
    /// * `E` - Displayable error or message value appended after the prefix.
    ///
    /// # Parameters
    ///
    /// * `err` - Failure to record next to the configured prefix.
    #[inline]
    pub fn log_prepare_failed<E: fmt::Display>(&self, err: E) {
        let Some(level) = self.prepare_failed_level else {
            return;
        };
        log::log!(level, "{}: {}", self.prepare_failed_message, err);
    }

    /// Emits a diagnostic line when the prepare commit action fails.
    ///
    /// Does nothing when [`Self::prepare_commit_failed_level`] is [`None`].
    /// Otherwise logs `"{prefix}: {err}"` at the configured level, where `prefix`
    /// is [`Self::prepare_commit_failed_message`], subject to the crate-wide
    /// maximum log level.
    ///
    /// # Type Parameters
    ///
    /// * `E` - Displayable error or message value appended after the prefix.
    ///
    /// # Parameters
    ///
    /// * `err` - Commit failure to record next to the configured prefix.
    #[inline]
    pub fn log_prepare_commit_failed<E: fmt::Display>(&self, err: E) {
        let Some(level) = self.prepare_commit_failed_level else {
            return;
        };
        log::log!(level, "{}: {}", self.prepare_commit_failed_message, err);
    }

    /// Emits a diagnostic line when the prepare rollback action fails.
    ///
    /// Does nothing when [`Self::prepare_rollback_failed_level`] is [`None`].
    /// Otherwise logs `"{prefix}: {err}"` at the configured level, where `prefix`
    /// is [`Self::prepare_rollback_failed_message`], subject to the crate-wide
    /// maximum log level.
    ///
    /// # Type Parameters
    ///
    /// * `E` - Displayable error or message value appended after the prefix.
    ///
    /// # Parameters
    ///
    /// * `err` - Rollback failure to record next to the configured prefix.
    #[inline]
    pub fn log_prepare_rollback_failed<E: fmt::Display>(&self, err: E) {
        let Some(level) = self.prepare_rollback_failed_level else {
            return;
        };
        log::log!(level, "{}: {}", self.prepare_rollback_failed_message, err);
    }
}
