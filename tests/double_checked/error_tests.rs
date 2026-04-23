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
    use std::io;

    use qubit_lock::double_checked::ExecutorError;

    mod test_executor_error {
        use super::*;

        #[test]
        fn test_executor_error_task_failed_display() {
            let error = ExecutorError::<String>::TaskFailed("Task failed".to_string());
            let display = format!("{}", error);
            assert_eq!(display, "Task execution failed: Task failed");
        }

        #[test]
        fn test_executor_error_prepare_failed_display() {
            let error = ExecutorError::<String>::PrepareFailed("Prepare failed".to_string());
            let display = format!("{}", error);
            assert_eq!(display, "Preparation action failed: Prepare failed");
        }

        #[test]
        fn test_executor_error_prepare_commit_failed_display() {
            let error = ExecutorError::<String>::PrepareCommitFailed("Commit failed".to_string());
            let display = format!("{}", error);
            assert_eq!(display, "Prepare commit action failed: Commit failed");
        }

        #[test]
        fn test_executor_error_prepare_rollback_failed_display() {
            let error = ExecutorError::<String>::PrepareRollbackFailed {
                original: "Original error".to_string(),
                rollback: "Rollback error".to_string(),
            };
            let display = format!("{}", error);
            assert_eq!(
                display,
                "Prepare rollback failed: original error = Original error, rollback error = Rollback error"
            );
        }

        #[test]
        fn test_executor_error_lock_poisoned_display() {
            let error = ExecutorError::<String>::LockPoisoned("Lock poisoned".to_string());
            let display = format!("{}", error);
            assert_eq!(display, "Lock poisoned: Lock poisoned");
        }

        #[test]
        fn test_executor_error_debug() {
            let error = ExecutorError::<String>::TaskFailed("Debug test".to_string());
            let debug_str = format!("{:?}", error);
            assert!(debug_str.contains("TaskFailed"));
            assert!(debug_str.contains("Debug test"));
        }

        #[test]
        fn test_executor_error_with_io_error() {
            let io_error = io::Error::other("IO error");
            let error = ExecutorError::<io::Error>::TaskFailed(io_error);
            let display = format!("{}", error);
            assert!(display.contains("Task execution failed"));
        }

        #[test]
        fn test_executor_error_is_error_trait() {
            let error = ExecutorError::<String>::TaskFailed("Test".to_string());
            // This will compile if it implements Error trait
            let _error_trait: &dyn std::error::Error = &error;
        }
    }
}
