// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`TokioMonitor`](qubit_lock::TokioMonitor).

use std::{
    future::Future,
    sync::{
        Arc,
        mpsc,
    },
    task::Poll,
    thread,
    time::Duration,
};

use qubit_lock::{
    AsyncConditionWaiter,
    AsyncTimeoutConditionWaiter,
    Notifier,
    TokioMonitor,
    WaitTimeoutResult,
};

/// Returns a future after proving at compile time that it is sendable.
fn assert_send<F: Future + Send>(future: F) -> F {
    future
}

/// Verifies that all Tokio condition-wait trait methods return `Send` futures.
#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_condition_wait_futures_are_send() {
    let monitor = TokioMonitor::new(true);

    assert!(
        assert_send(
            <TokioMonitor<bool> as AsyncConditionWaiter>::wait_until_async(
                &monitor,
                |ready| *ready,
                |ready| *ready,
            ),
        )
        .await,
    );
    assert!(
        assert_send(
            <TokioMonitor<bool> as AsyncConditionWaiter>::wait_while_async(
                &monitor,
                |ready| !*ready,
                |ready| *ready,
            ),
        )
        .await,
    );
    assert_eq!(
        assert_send(
            <TokioMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
                &monitor,
                Duration::ZERO,
                |ready| *ready,
                |ready| *ready,
            ),
        )
        .await,
        WaitTimeoutResult::Ready(true),
    );
    assert_eq!(
        assert_send(
            <TokioMonitor<bool> as AsyncTimeoutConditionWaiter>::wait_while_for_async(
                &monitor,
                Duration::ZERO,
                |ready| !*ready,
                |ready| *ready,
            ),
        )
        .await,
        WaitTimeoutResult::Ready(true),
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_helpers_and_conversions_delegate_to_state() {
    let monitor = TokioMonitor::from(vec![1]);

    monitor.with_write_async(|items| items.push(2)).await;
    assert_eq!(
        monitor.with_read_async(|items| items.clone()).await,
        vec![1, 2],
    );

    let one_result = monitor
        .with_write_notify_one_async(|items| {
            items.push(3);
            items.len()
        })
        .await;
    assert_eq!(one_result, 3);

    let all_result = monitor
        .with_write_notify_all_async(|items| {
            items.push(4);
            items.len()
        })
        .await;
    assert_eq!(all_result, 4);

    monitor.notify_one();
    monitor.notify_all();

    let default_monitor = TokioMonitor::<Vec<i32>>::default();
    assert!(
        default_monitor
            .with_read_async(|items| items.is_empty())
            .await
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_traits_delegate_to_monitor_methods() {
    let monitor = TokioMonitor::new(vec![1, 2]);

    <TokioMonitor<Vec<i32>> as Notifier>::notify_one(&monitor);
    <TokioMonitor<Vec<i32>> as Notifier>::notify_all(&monitor);

    monitor.with_write_async(|items| items.clear()).await;
    let condition_wait =
        <TokioMonitor<Vec<i32>> as AsyncConditionWaiter>::wait_while_async(
            &monitor,
            |items| items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(condition_wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut condition_wait)
            .await
            .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(2))
        .await;
    assert_eq!(condition_wait.await, 2);

    let timeout_condition_wait =
        <TokioMonitor<Vec<i32>> as AsyncTimeoutConditionWaiter>::wait_until_for_async(
            &monitor,
            Duration::from_secs(1),
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        );
    tokio::pin!(timeout_condition_wait);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            &mut timeout_condition_wait
        )
        .await
        .is_err()
    );
    monitor
        .with_write_notify_one_async(|items| items.push(1))
        .await;
    assert_eq!(timeout_condition_wait.await, WaitTimeoutResult::Ready(1),);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_uses_condition_wait_budget() {
    let monitor = TokioMonitor::new(false);
    let wait = monitor.wait_while_for_async(
        Duration::from_millis(5),
        |ready| !*ready,
        |_| 7,
    );

    tokio::time::advance(Duration::from_millis(10)).await;

    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
        "an unpolled condition wait should retain its full budget",
    );

    tokio::time::advance(Duration::from_millis(4)).await;
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(wait.await, WaitTimeoutResult::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_excludes_initial_lock_contention_from_timeout()
{
    let monitor = Arc::new(TokioMonitor::new(false));
    let holder_monitor = Arc::clone(&monitor);
    let (holding_tx, holding_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let holder = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("holder runtime should build");
        runtime.block_on(holder_monitor.with_write_async(|_| {
            holding_tx
                .send(())
                .expect("test should observe the held state lock");
            release_rx
                .recv()
                .expect("holder should receive release permission");
        }));
    });
    holding_rx
        .recv()
        .expect("holder should acquire the state lock");

    let wait = monitor.wait_while_for_async(
        Duration::from_millis(5),
        |ready| !*ready,
        |_| (),
    );
    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );
    tokio::time::advance(Duration::from_millis(10)).await;

    release_tx
        .send(())
        .expect("holder should receive release permission");
    holder.join().expect("holder thread should finish");
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
        "initial state-lock contention should not consume the wait budget",
    );

    tokio::time::advance(Duration::from_millis(5)).await;
    assert_eq!(wait.await, WaitTimeoutResult::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_reuses_fixed_timeout_deadline()
{
    let monitor = TokioMonitor::new(false);
    let wait = monitor.wait_while_for_async(
        Duration::from_millis(10),
        |ready| !*ready,
        |_| (),
    );
    tokio::pin!(wait);
    assert!(
        std::future::poll_fn(|context| {
            Poll::Ready(wait.as_mut().poll(context))
        })
        .await
        .is_pending(),
    );

    for _ in 0..2 {
        tokio::time::advance(Duration::from_millis(4)).await;
        monitor.notify_one();
        assert!(
            std::future::poll_fn(|context| {
                Poll::Ready(wait.as_mut().poll(context))
            })
            .await
            .is_pending(),
        );
    }

    tokio::time::advance(Duration::from_millis(2)).await;
    assert_eq!(wait.await, WaitTimeoutResult::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_zero_timeout_checks_predicate_once()
{
    let checks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let predicate_checks = Arc::clone(&checks);
    let monitor = TokioMonitor::new(false);

    let result = monitor
        .wait_while_for_async(
            Duration::ZERO,
            move |ready| {
                predicate_checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                !*ready
            },
            |_| (),
        )
        .await;

    assert_eq!(result, WaitTimeoutResult::TimedOut);
    assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_until_runs_action_after_notify() {
    let monitor = Arc::new(TokioMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_until_async(
                |ready| *ready,
                |ready| {
                    *ready = false;
                    7
                },
            )
            .await
    });

    tokio::task::yield_now().await;
    monitor
        .with_write_notify_one_async(|ready| *ready = true)
        .await;

    assert_eq!(waiter.await.expect("waiter task should finish"), 7);
    assert!(!monitor.with_read_async(|ready| *ready).await);
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_returns_ready_after_notify() {
    let monitor = Arc::new(TokioMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_while_for_async(
                Duration::from_secs(1),
                |ready| !*ready,
                |ready| {
                    *ready = false;
                    9
                },
            )
            .await
    });

    tokio::task::yield_now().await;
    monitor.with_write_async(|ready| *ready = true).await;
    monitor.notify_one();

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(9),
    );
}

#[tokio::test(start_paused = true)]
async fn test_tokio_monitor_async_wait_while_for_rechecks_state_after_timeout()
{
    let monitor = Arc::new(TokioMonitor::new(false));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_while_for_async(
                Duration::from_millis(20),
                |ready| !*ready,
                |ready| {
                    *ready = false;
                    9
                },
            )
            .await
    });

    tokio::time::advance(Duration::from_millis(5)).await;
    monitor.with_write_async(|ready| *ready = true).await;

    assert_eq!(
        waiter.await.expect("waiter task should finish"),
        WaitTimeoutResult::Ready(9),
    );
}
