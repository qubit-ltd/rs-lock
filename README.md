# Qubit Lock

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-lock.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-lock)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Lock-focused utilities for the Qubit Rust libraries. The crate provides synchronous and asynchronous lock wrappers, monitor-style coordination, and a reusable double-checked locking executor.

## Features

- `ArcMutex`, `ArcRwLock`, `ArcStdMutex`: synchronous lock wrappers with `Arc` built in.
- `ArcAsyncMutex`, `ArcAsyncRwLock`: Tokio-based asynchronous lock wrappers.
- `Monitor`, `ArcMonitor`, `MonitorGuard`: condition-based state coordination.
- `DoubleCheckedLockExecutor`: reusable test-outside-lock / re-test-inside-lock workflow.
- Closure-based APIs that keep lock acquisition and release scoped to one call.

## Installation

```toml
[dependencies]
qubit-lock = "0.1.0"
```

## Quick Start

### Synchronous lock

```rust
use qubit_lock::ArcMutex;

fn main() {
    let counter = ArcMutex::new(0);
    counter.write(|value| *value += 1);
    assert_eq!(counter.read(|value| *value), 1);
}
```

### Monitor

```rust
use qubit_lock::lock::ArcMonitor;

fn main() {
    let monitor = ArcMonitor::new(vec![1, 2, 3]);
    let length = monitor.read(|items| items.len());
    assert_eq!(length, 3);
}
```

### Double-checked locking

```rust
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use qubit_lock::{ArcMutex, DoubleCheckedLockExecutor};

fn main() {
    let data = ArcMutex::new(10);
    let enabled = Arc::new(AtomicBool::new(true));

    let executor = DoubleCheckedLockExecutor::builder()
        .on(data.clone())
        .when({
            let enabled = enabled.clone();
            move || enabled.load(Ordering::Acquire)
        })
        .build();

    let result = executor
        .call_with(|value: &mut i32| {
            *value += 5;
            Ok::<i32, std::io::Error>(*value)
        })
        .get_result()
        .unwrap();

    assert_eq!(result, 15);
}
```

## Project Layout

- `src/lock`: lock traits, wrappers, and monitor primitives.
- `src/double_checked`: reusable double-checked locking executor and builders.
- `tests/lock`: lock and monitor behavior tests.
- `tests/double_checked`: double-checked locking behavior tests.
- `tests/docs`: README and doctext consistency tests.

## Quality Checks

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## License

Apache-2.0
