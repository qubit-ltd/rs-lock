/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Double-Checked Lock Executor Demo
//!
//! Demonstrates the usage of a reusable double-checked lock executor.
//!
//! # Author
//!
//! Haixing Hu

use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        Ordering,
    },
};

use qubit_lock::{
    DoubleCheckedLockExecutor,
    lock::{
        ArcMutex,
        Lock,
    },
};

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
enum ServiceError {
    #[error("Service is not running")]
    NotRunning,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create shared state
    let running = Arc::new(AtomicBool::new(false));
    let data = ArcMutex::new(42);

    println!(
        "Initial state: running = {}",
        running.load(Ordering::Acquire)
    );
    println!("Initial data: {}", data.read(|d| *d));

    let executor = DoubleCheckedLockExecutor::builder()
        .on(data.clone())
        .when({
            let running = running.clone();
            move || running.load(Ordering::Acquire)
        })
        .build();

    // Try to execute when service is not running (should fail)
    let result = executor
        .call_with(|value: &mut i32| {
            *value += 1;
            Ok::<_, ServiceError>(*value)
        })
        .get_result();

    if result.is_success() {
        println!("Unexpected success: {}", result.unwrap());
    } else {
        println!("Expected failure: Condition not met.");
    }

    // Start the service
    running.store(true, Ordering::Release);
    println!(
        "Service started: running = {}",
        running.load(Ordering::Acquire)
    );

    // Now execute should succeed
    let result = executor
        .call_with(|value: &mut i32| {
            *value += 1;
            Ok::<_, ServiceError>(*value)
        })
        .get_result();

    if result.is_success() {
        println!("Success: new value = {}", result.unwrap());
    } else {
        println!("Unexpected failure: {:?}", result);
    }

    // Verify the data was updated
    println!("Final data: {}", data.read(|d| *d));

    // Stop the service
    running.store(false, Ordering::Release);
    println!(
        "Service stopped: running = {}",
        running.load(Ordering::Acquire)
    );

    // Try to execute when service is stopped (should fail)
    let result = executor
        .call_with(|value: &mut i32| {
            *value += 1;
            Ok::<_, ServiceError>(*value)
        })
        .get_result();

    if result.is_success() {
        println!("Unexpected success: {}", result.unwrap());
    } else {
        println!("Expected failure: Condition not met.");
    }

    Ok(())
}
