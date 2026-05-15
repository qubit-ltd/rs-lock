/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::time::Duration;

use qubit_lock::monitor::{
    ArcMonitor,
    WaitTimeoutResult,
};
use qubit_lock::{
    ArcRwLock,
    ArcStdRwLock,
    Lock,
};

fn main() {
    println!("Demonstrating rs-lock wrapper boundaries...");

    let cache = ArcRwLock::from(Vec::<String>::new());
    cache.write(|items| items.push(String::from("ready")));
    assert_eq!(cache.read(|items| items.len()), 1);

    let std_state = ArcStdRwLock::new(String::from("std semantics"));
    assert_eq!(
        std_state.read(|value| value.clone()),
        String::from("std semantics"),
    );

    let monitor = ArcMonitor::new(Vec::<i32>::new());
    let result = monitor.wait_timeout_while(
        Duration::from_millis(1),
        |items| items.is_empty(),
        |items| items.pop(),
    );
    assert!(result.is_timed_out());

    monitor.write(|items| items.push(7));
    monitor.notify_one();
    let result = monitor.wait_timeout_until(
        Duration::from_millis(1),
        |items| !items.is_empty(),
        |items| items.pop(),
    );
    assert_eq!(result, WaitTimeoutResult::Ready(Some(7)));
    assert_eq!(
        result.map(|item| item.unwrap_or_default()).into_option(),
        Some(7)
    );

    println!("All wrapper boundary examples passed.");
}
