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
    use qubit_lock::{
        ArcMutex,
        DoubleCheckedLockExecutor,
        double_checked::ExecutionResult,
    };

    mod test_executor_builder {
        use super::*;

        #[test]
        fn test_logger_can_be_configured_in_each_builder_state() {
            let data = ArcMutex::new(1);
            let executor = DoubleCheckedLockExecutor::builder()
                .log_unmet_condition(log::Level::Info, "initial")
                .on(data)
                .log_unmet_condition(log::Level::Debug, "locked")
                .when(|| true)
                .log_unmet_condition(log::Level::Warn, "ready")
                .build();

            let result = executor
                .call_with(|value: &mut i32| Ok::<i32, std::io::Error>(*value))
                .get_result();

            assert!(matches!(result, ExecutionResult::Success(1)));
        }

        #[test]
        fn test_prepare_logger_methods_on_initial_builder_are_chainable() {
            let data = ArcMutex::new(1);
            let executor = DoubleCheckedLockExecutor::builder()
                .log_prepare_failure(log::Level::Warn, "prepare failed")
                .log_prepare_commit_failure(log::Level::Error, "prepare commit failed")
                .log_prepare_rollback_failure(log::Level::Info, "prepare rollback failed")
                .on(data)
                .when(|| true)
                .build();

            let result = executor
                .call_with(|value: &mut i32| Ok::<i32, std::io::Error>(*value))
                .get_result();

            assert!(matches!(result, ExecutionResult::Success(1)));
        }
    }
}
