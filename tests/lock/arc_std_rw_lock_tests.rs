/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
// qubit-style: allow explicit-imports
//! # ArcStdRwLock Tests
//!
//! Tests for the ArcStdRwLock implementation.

use std::{
    sync::{
        Arc,
        mpsc,
    },
    thread,
    time::Duration,
};

use qubit_lock::{
    ArcStdRwLock,
    Lock,
    TryLockError,
};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod arc_std_rw_lock_tests {
    use super::*;

    fn read_i32(value: &i32) -> i32 {
        *value
    }

    fn increment_i32(value: &mut i32) -> i32 {
        *value += 1;
        *value
    }

    fn poison_lock(lock: Arc<ArcStdRwLock<i32>>) {
        let poisoned = lock.clone();
        let handle = thread::spawn(move || {
            poisoned.write(|value| {
                *value += 1;
                panic!("intentional panic to poison the lock");
            });
        });
        let _ = handle.join();
    }

    #[test]
    fn test_arc_std_rw_lock_new_from_default_and_clone() {
        let rw_lock = ArcStdRwLock::new(41);
        let cloned = rw_lock.clone();

        assert_eq!(rw_lock.read(read_i32), 41);
        assert_eq!(cloned.write(increment_i32), 42);
        assert_eq!(rw_lock.read(read_i32), 42);

        let from_value = ArcStdRwLock::from(String::from("ready"));
        assert_eq!(from_value.read(|value| value.clone()), "ready");

        let default_value = ArcStdRwLock::<Vec<i32>>::default();
        assert!(default_value.read(|items| items.is_empty()));
    }

    #[test]
    fn test_arc_std_rw_lock_deref_and_as_ref_expose_rw_lock_api() {
        let rw_lock = ArcStdRwLock::new(1);

        {
            let guard = (*rw_lock).read().expect("rw lock should not be poisoned");
            assert_eq!(*guard, 1);
        }

        {
            let mut guard = rw_lock
                .as_ref()
                .write()
                .expect("rw lock should not be poisoned");
            *guard += 1;
        }

        assert_eq!(rw_lock.read(read_i32), 2);
    }

    #[test]
    fn test_arc_std_rw_lock_try_methods_success_and_contention() {
        let rw_lock = Arc::new(ArcStdRwLock::new(0));

        assert_eq!(rw_lock.try_read(read_i32), Ok(0));
        assert_eq!(rw_lock.try_write(increment_i32), Ok(1));

        let (write_locked_tx, write_locked_rx) = mpsc::channel();
        let (write_release_tx, write_release_rx) = mpsc::channel();
        let write_lock = rw_lock.clone();
        let write_holder = thread::spawn(move || {
            write_lock.write(|_| {
                write_locked_tx
                    .send(())
                    .expect("test should observe held write lock");
                write_release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held write lock");
            });
        });

        write_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("write lock should be held within timeout");
        assert_eq!(rw_lock.try_read(read_i32), Err(TryLockError::WouldBlock));
        assert_eq!(
            rw_lock.try_write(increment_i32),
            Err(TryLockError::WouldBlock),
        );
        write_release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        write_holder.join().expect("holder should not panic");

        let (read_locked_tx, read_locked_rx) = mpsc::channel();
        let (read_release_tx, read_release_rx) = mpsc::channel();
        let read_lock = rw_lock.clone();
        let read_holder = thread::spawn(move || {
            read_lock.read(|_| {
                read_locked_tx
                    .send(())
                    .expect("test should observe held read lock");
                read_release_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("test should release held read lock");
            });
        });

        read_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("read lock should be held within timeout");
        assert_eq!(
            rw_lock.try_write(increment_i32),
            Err(TryLockError::WouldBlock),
        );
        assert_eq!(rw_lock.try_read(read_i32), Ok(1));
        read_release_tx
            .send(())
            .expect("holder thread should still be waiting for release");
        read_holder.join().expect("holder should not panic");
    }

    #[test]
    fn test_arc_std_rw_lock_try_methods_return_poisoned() {
        let rw_lock = Arc::new(ArcStdRwLock::new(0));

        poison_lock(rw_lock.clone());

        assert_eq!(rw_lock.try_read(read_i32), Err(TryLockError::Poisoned));
        assert_eq!(
            rw_lock.try_write(increment_i32),
            Err(TryLockError::Poisoned),
        );
    }

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_arc_std_rw_lock_read_panics_on_poisoned() {
        let rw_lock = Arc::new(ArcStdRwLock::new(0));

        poison_lock(rw_lock.clone());
        rw_lock.read(read_i32);
    }

    #[test]
    #[should_panic(expected = "PoisonError")]
    fn test_arc_std_rw_lock_write_panics_on_poisoned() {
        let rw_lock = Arc::new(ArcStdRwLock::new(0));

        poison_lock(rw_lock.clone());
        rw_lock.write(increment_i32);
    }
}
