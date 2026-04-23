/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
#[cfg(test)]
mod tests {
    use qubit_lock::double_checked::{
        ExecutionLogger,
        ExecutorConfig,
    };

    mod test_execution_logger {
        use super::*;

        #[test]
        fn test_execution_logger_creation() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(Some(log::Level::Info), "Test message");

            assert_eq!(logger.unmet_condition_level, Some(log::Level::Info));
            assert_eq!(logger.unmet_condition_message, "Test message");
            assert_eq!(logger.prepare_failed_message, "Prepare action failed");
            assert_eq!(
                logger.prepare_commit_failed_message,
                "Prepare commit action failed"
            );
            assert_eq!(
                logger.prepare_rollback_failed_message,
                "Prepare rollback action failed"
            );
        }

        #[test]
        fn test_execution_logger_debug() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(Some(log::Level::Warn), "Warning message");

            let debug_str = format!("{:?}", logger);
            assert!(debug_str.contains("ExecutionLogger"));
            assert!(debug_str.contains("Warn"));
            assert!(debug_str.contains("Warning message"));
        }

        #[test]
        fn test_execution_logger_clone() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(Some(log::Level::Error), "Error occurred");

            let cloned = logger.clone();
            assert_eq!(cloned.unmet_condition_level, logger.unmet_condition_level);
            assert_eq!(
                cloned.unmet_condition_message,
                logger.unmet_condition_message
            );
        }

        #[test]
        fn test_execution_logger_with_empty_message() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(Some(log::Level::Debug), "");

            assert_eq!(logger.unmet_condition_level, Some(log::Level::Debug));
            assert!(logger.unmet_condition_message.is_empty());
        }

        #[test]
        fn test_execution_logger_with_unicode_message() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(Some(log::Level::Info), "测试消息 🚀");

            assert_eq!(logger.unmet_condition_message, "测试消息 🚀");
        }

        #[test]
        fn test_disabled_execution_logger_skips_all_log_methods() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(None, "");
            logger.set_prepare_failure(None, "");
            logger.set_prepare_commit_failure(None, "");
            logger.set_prepare_rollback_failure(None, "");

            logger.log_unmet_condition();
            logger.log_prepare_failed("prepare");
            logger.log_prepare_commit_failed("commit");
            logger.log_prepare_rollback_failed("rollback");
        }

        #[test]
        fn test_enabled_execution_logger_logs_all_methods() {
            let mut logger = ExecutionLogger::default();
            logger.set_unmet_condition(Some(log::Level::Info), "enabled");

            logger.log_unmet_condition();
            logger.log_prepare_failed("prepare");
            logger.log_prepare_commit_failed("commit");
            logger.log_prepare_rollback_failed("rollback");
        }
    }

    mod test_executor_config {
        use super::*;

        #[test]
        fn test_executor_config_default() {
            let config = ExecutorConfig::default();

            assert!(!config.enable_metrics);
            assert!(!config.disable_backtrace);
        }

        #[test]
        fn test_executor_config_creation() {
            let config = ExecutorConfig {
                enable_metrics: true,
                disable_backtrace: true,
            };

            assert!(config.enable_metrics);
            assert!(config.disable_backtrace);
        }

        #[test]
        fn test_executor_config_debug() {
            let config = ExecutorConfig {
                enable_metrics: true,
                disable_backtrace: false,
            };

            let debug_str = format!("{:?}", config);
            assert!(debug_str.contains("ExecutorConfig"));
            assert!(debug_str.contains("enable_metrics: true"));
            assert!(debug_str.contains("disable_backtrace: false"));
        }

        #[test]
        fn test_executor_config_clone() {
            let config = ExecutorConfig {
                enable_metrics: false,
                disable_backtrace: true,
            };

            let cloned = config.clone();
            assert_eq!(cloned.enable_metrics, config.enable_metrics);
            assert_eq!(cloned.disable_backtrace, config.disable_backtrace);
        }
    }
}
