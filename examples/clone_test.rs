// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;
use std::sync::RwLock;

use qubit_lock::{
    ArcParkingLotMonitor,
    DataLock,
    WaitTimeoutResult,
};

fn main() {
    println!("Demonstrating rs-lock wrapper boundaries...");

    let cache = parking_lot::RwLock::new(Vec::<String>::new());
    cache.with_write(|items| items.push(String::from("ready")));
    assert_eq!(cache.with_read(|items| items.len()), 1);

    let std_state = RwLock::new(String::from("std semantics"));
    assert_eq!(
        std_state.with_read(|value| value.clone()),
        String::from("std semantics"),
    );

    let monitor = ArcParkingLotMonitor::new(Vec::<i32>::new());
    let result = monitor.wait_while_for(
        Duration::from_millis(1),
        |items| items.is_empty(),
        |items| items.pop(),
    );
    assert!(
        result
            .expect("standard Timer should register")
            .is_timed_out()
    );

    monitor.with_write(|items| items.push(7));
    monitor.notify_one();
    let result = monitor.wait_until_for(
        Duration::from_millis(1),
        |items| !items.is_empty(),
        |items| items.pop(),
    );
    let result = result.expect("standard Timer should register");
    assert_eq!(result, WaitTimeoutResult::Ready(Some(7)));
    assert_eq!(
        result.map(|item| item.unwrap_or_default()).into_option(),
        Some(7)
    );

    println!("All wrapper boundary examples passed.");
}
