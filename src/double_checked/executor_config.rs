/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Executor Configuration
//!
//! Provides executor configuration for the double-checked lock executor.
//!
//! # Author
//!
//! Haixing Hu

/// Executor configuration
///
/// Configures various execution options for the double-checked lock
/// executor, including performance metrics and error handling.
///
/// # Examples
///
/// ```rust
/// use qubit_lock::double_checked::ExecutorConfig;
///
/// let config = ExecutorConfig::default();
/// assert!(!config.enable_metrics);
/// assert!(!config.disable_backtrace);
/// ```
///
/// # Author
///
/// Haixing Hu
///
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Whether to enable performance metrics collection
    pub enable_metrics: bool,

    /// Whether to disable error backtrace for performance
    pub disable_backtrace: bool,
}

impl Default for ExecutorConfig {
    /// Creates a default executor configuration
    ///
    /// # Returns
    ///
    /// A default configuration with metrics disabled and backtrace enabled.
    #[inline]
    fn default() -> Self {
        Self {
            enable_metrics: false,
            disable_backtrace: false,
        }
    }
}
