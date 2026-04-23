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
    use std::{
        io,
        sync::{
            Arc,
            atomic::{
                AtomicBool,
                AtomicUsize,
                Ordering,
            },
        },
    };

    use qubit_lock::{
        ArcMutex,
        DoubleCheckedLockExecutor,
        double_checked::{
            ExecutionResult,
            ExecutorError,
        },
        lock::Lock,
    };

    mod test_executor_lock_builder {
        use super::*;

        fn increment_and_return_task(value: &mut i32) -> Result<i32, io::Error> {
            *value += 1;
            Ok(*value)
        }

        #[test]
        fn test_second_condition_failure_after_prepare_rolls_back() {
            let data = ArcMutex::new(10);
            let checks = Arc::new(AtomicUsize::new(0));
            let rolled_back = Arc::new(AtomicBool::new(false));
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data.clone())
                .when({
                    let checks = checks.clone();
                    move || checks.fetch_add(1, Ordering::AcqRel) == 0
                })
                .log_unmet_condition(log::Level::Info, "condition not met")
                .prepare(|| Ok::<(), io::Error>(()))
                .rollback_prepare({
                    let rolled_back = rolled_back.clone();
                    move || {
                        rolled_back.store(true, Ordering::Release);
                        Ok::<(), io::Error>(())
                    }
                })
                .build();

            let result = executor
                .call_with(increment_and_return_task as fn(&mut i32) -> Result<i32, io::Error>)
                .get_result();

            assert!(matches!(result, ExecutionResult::ConditionNotMet));
            assert!(rolled_back.load(Ordering::Acquire));
            assert_eq!(data.read(|value| *value), 10);
            assert_eq!(checks.load(Ordering::Acquire), 2);
        }

        #[test]
        fn test_prepare_rollback_failure_with_logger_replaces_condition_result() {
            let data = ArcMutex::new(10);
            let checks = Arc::new(AtomicUsize::new(0));
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data)
                .when({
                    let checks = checks.clone();
                    move || checks.fetch_add(1, Ordering::AcqRel) == 0
                })
                .log_unmet_condition(log::Level::Error, "condition not met")
                .prepare(|| Ok::<(), io::Error>(()))
                .rollback_prepare(|| Err::<(), _>(io::Error::other("rollback failed")))
                .build();

            let result = executor
                .call_with(|value: &mut i32| Ok::<i32, io::Error>(*value))
                .get_result();

            assert!(matches!(
                result,
                ExecutionResult::Failed(ExecutorError::PrepareRollbackFailed {
                    original,
                    rollback,
                }) if original == "Condition not met" && rollback == "rollback failed"
            ));
        }
    }
}
