// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`AsyncMonitor`](qubit_lock::AsyncMonitor).

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use qubit_lock::AsyncConditionWaiter;
use qubit_lock::AsyncMonitor;
use qubit_lock::TokioMonitor;

/// Exercises state access and untimed waiting through the aggregate capability.
async fn use_async_monitor<M>(monitor: &M)
where
    M: AsyncMonitor<State = bool>,
{
    AsyncMonitor::with_write_async(monitor, |ready| *ready = true).await;
    assert!(AsyncMonitor::with_read_async(monitor, |ready| *ready).await);
    AsyncMonitor::with_write_notify_one_async(monitor, |ready| {
        *ready = false;
    })
    .await;
    AsyncMonitor::with_write_notify_all_async(monitor, |ready| {
        *ready = true;
    })
    .await;
    assert_eq!(monitor.wait_until_async(|ready| *ready, |_| 7).await, 7,);
}

#[derive(Default)]
struct NotificationWakeCounter(AtomicUsize);

impl Wake for NotificationWakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_notification<F: Future>(future: Pin<&mut F>, counter: &Arc<NotificationWakeCounter>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(counter));
    future.poll(&mut Context::from_waker(&waker))
}

/// Verifies a Tokio handle satisfies [`AsyncMonitor`].
#[tokio::test(start_paused = true)]
async fn test_async_monitor_trait_accepts_tokio_monitor() {
    use_async_monitor(&TokioMonitor::current(false)).await;
}

/// Verifies [`Arc`] forwards the aggregate async monitor capability.
#[tokio::test(start_paused = true)]
async fn test_async_monitor_trait_accepts_arc_forwarding() {
    let monitor = Arc::new(TokioMonitor::current(false));
    use_async_monitor(&monitor).await;
}

#[tokio::test(start_paused = true)]
async fn test_async_monitor_default_notify_one_wakes_one_registered_waiter() {
    let monitor = TokioMonitor::current(false);
    let first_wakes = Arc::new(NotificationWakeCounter::default());
    let second_wakes = Arc::new(NotificationWakeCounter::default());
    let mut first = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
    let mut second = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
    assert!(poll_notification(first.as_mut(), &first_wakes).is_pending());
    assert!(poll_notification(second.as_mut(), &second_wakes).is_pending());

    AsyncMonitor::with_write_notify_one_async(&monitor, |ready| *ready = true).await;
    let first_selected = first_wakes.0.load(Ordering::SeqCst) > 0;
    let second_selected = second_wakes.0.load(Ordering::SeqCst) > 0;
    assert_ne!(
        first_selected, second_selected,
        "notify_one must select exactly one waiter"
    );
    if first_selected {
        assert_eq!(poll_notification(first.as_mut(), &first_wakes), Poll::Ready(true));
        assert!(poll_notification(second.as_mut(), &second_wakes).is_pending());
    } else {
        assert_eq!(poll_notification(second.as_mut(), &second_wakes), Poll::Ready(true));
        assert!(poll_notification(first.as_mut(), &first_wakes).is_pending());
    }

    AsyncMonitor::with_write_notify_all_async(&monitor, |_| ()).await;
    if first_selected {
        assert_eq!(poll_notification(second.as_mut(), &second_wakes), Poll::Ready(true));
    } else {
        assert_eq!(poll_notification(first.as_mut(), &first_wakes), Poll::Ready(true));
    }
}

#[tokio::test(start_paused = true)]
async fn test_async_monitor_default_notify_all_wakes_all_registered_waiters() {
    let monitor = Arc::new(TokioMonitor::current(false));
    let first_wakes = Arc::new(NotificationWakeCounter::default());
    let second_wakes = Arc::new(NotificationWakeCounter::default());
    let mut first = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
    let mut second = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
    assert!(poll_notification(first.as_mut(), &first_wakes).is_pending());
    assert!(poll_notification(second.as_mut(), &second_wakes).is_pending());

    AsyncMonitor::with_write_notify_all_async(&monitor, |ready| *ready = true).await;
    assert!(first_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(second_wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(poll_notification(first.as_mut(), &first_wakes), Poll::Ready(true));
    assert_eq!(poll_notification(second.as_mut(), &second_wakes), Poll::Ready(true));
}

#[tokio::test(start_paused = true)]
async fn test_async_monitor_default_notify_skips_notification_on_panic() {
    for broadcast in [false, true] {
        let monitor = TokioMonitor::current(false);
        let first_wakes = Arc::new(NotificationWakeCounter::default());
        let second_wakes = Arc::new(NotificationWakeCounter::default());
        let mut first = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
        let mut second = Box::pin(monitor.wait_until_async(|ready| *ready, |ready| *ready));
        assert!(poll_notification(first.as_mut(), &first_wakes).is_pending());
        assert!(poll_notification(second.as_mut(), &second_wakes).is_pending());
        let operation_wakes = Arc::new(NotificationWakeCounter::default());
        let mutate = |ready: &mut bool| {
            *ready = true;
            panic!("mutation failed");
        };
        let panic_result = if broadcast {
            let mut operation = Box::pin(AsyncMonitor::with_write_notify_all_async::<(), _>(&monitor, mutate));
            catch_unwind(AssertUnwindSafe(|| {
                let _ = poll_notification(operation.as_mut(), &operation_wakes);
            }))
        } else {
            let mut operation = Box::pin(AsyncMonitor::with_write_notify_one_async::<(), _>(&monitor, mutate));
            catch_unwind(AssertUnwindSafe(|| {
                let _ = poll_notification(operation.as_mut(), &operation_wakes);
            }))
        };
        assert!(panic_result.is_err(), "mutation panic must propagate");
        assert!(AsyncMonitor::with_read_async(&monitor, |ready| *ready).await);
        assert_eq!(first_wakes.0.load(Ordering::SeqCst), 0);
        assert_eq!(second_wakes.0.load(Ordering::SeqCst), 0);
        assert!(poll_notification(first.as_mut(), &first_wakes).is_pending());
        assert!(poll_notification(second.as_mut(), &second_wakes).is_pending());
        AsyncMonitor::with_write_notify_all_async(&monitor, |_| ()).await;
        assert_eq!(poll_notification(first.as_mut(), &first_wakes), Poll::Ready(true));
        assert_eq!(poll_notification(second.as_mut(), &second_wakes), Poll::Ready(true));
    }
}
