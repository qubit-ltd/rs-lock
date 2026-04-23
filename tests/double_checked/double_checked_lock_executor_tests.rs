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
            ExecutionContext,
            ExecutionResult,
        },
        lock::Lock,
    };

    mod test_double_checked_lock_executor {
        use super::*;

        fn increment_unit_task(value: &mut i32) -> Result<(), io::Error> {
            *value += 1;
            Ok(())
        }

        #[test]
        fn test_call_with_executes_reusable_task_with_mutable_data() {
            let data = ArcMutex::new(10);
            let active = Arc::new(AtomicBool::new(false));
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data.clone())
                .when({
                    let active = active.clone();
                    move || !active.load(Ordering::Acquire)
                })
                .build();

            let first = executor
                .call_with(|value: &mut i32| {
                    *value += 5;
                    Ok::<i32, io::Error>(*value)
                })
                .get_result();
            let second = executor
                .call_with(|value: &mut i32| {
                    *value += 7;
                    Ok::<i32, io::Error>(*value)
                })
                .get_result();

            assert!(matches!(first, ExecutionResult::Success(15)));
            assert!(matches!(second, ExecutionResult::Success(22)));
            assert_eq!(data.read(|value| *value), 22);
        }

        #[test]
        fn test_execute_with_reports_condition_not_met_without_lock_mutation() {
            let data = ArcMutex::new(10);
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data.clone())
                .when(|| false)
                .build();

            let result = executor
                .execute_with(increment_unit_task as fn(&mut i32) -> Result<(), io::Error>)
                .get_result();

            assert!(matches!(result, ExecutionResult::ConditionNotMet));
            assert_eq!(data.read(|value| *value), 10);
        }

        #[test]
        fn test_call_and_execute_run_zero_argument_tasks_as_executor_api() {
            let data = ArcMutex::new(0);
            let counter = Arc::new(AtomicUsize::new(0));
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data.clone())
                .when(|| true)
                .build();

            let value = executor
                .call(|| Ok::<i32, io::Error>(42))
                .get_result()
                .unwrap();
            let executed = executor
                .execute({
                    let counter = counter.clone();
                    move || {
                        counter.fetch_add(1, Ordering::AcqRel);
                        Ok::<(), io::Error>(())
                    }
                })
                .get_result();

            assert_eq!(value, 42);
            assert!(matches!(executed, ExecutionResult::Success(())));
            assert_eq!(counter.load(Ordering::Acquire), 1);
        }

        #[test]
        fn test_inherent_call_returns_execution_context() {
            let data = ArcMutex::new(0);
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data)
                .when(|| true)
                .build();

            let context: ExecutionContext<i32, io::Error> =
                executor.call(|| Ok::<i32, io::Error>(7));

            assert!(matches!(context.get_result(), ExecutionResult::Success(7)));
        }

        #[test]
        fn test_execution_context_peek_success_and_finish() {
            let data = ArcMutex::new(0);
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data.clone())
                .when(|| true)
                .build();

            let context = executor.call(|| Ok::<i32, io::Error>(3));
            assert!(context.is_success());
            assert!(matches!(context.peek_result(), ExecutionResult::Success(3)));
            assert!(matches!(context.get_result(), ExecutionResult::Success(3)));

            let finished = executor.execute(|| Ok::<(), io::Error>(())).finish();
            assert!(finished);

            let skipped = DoubleCheckedLockExecutor::builder()
                .on(data)
                .when(|| false)
                .build()
                .execute(|| Ok::<(), io::Error>(()))
                .finish();
            assert!(!skipped);
        }

        #[test]
        fn test_configured_logger_logs_condition_not_met() {
            let data = ArcMutex::new(10);
            let executor = DoubleCheckedLockExecutor::builder()
                .on(data.clone())
                .when(|| false)
                .log_unmet_condition(log::Level::Info, "condition not met")
                .build();

            let result = executor
                .execute_with(increment_unit_task as fn(&mut i32) -> Result<(), io::Error>)
                .get_result();

            assert!(matches!(result, ExecutionResult::ConditionNotMet));
            assert_eq!(data.read(|value| *value), 10);
        }
    }
}
