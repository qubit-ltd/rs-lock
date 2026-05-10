# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-lock/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-lock?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Lock-focused utilities for the Qubit Rust libraries. The crate provides synchronous and asynchronous lock wrappers plus monitor-style coordination.

## Features

- `ArcMutex`, `ArcRwLock`, `ArcStdMutex`: synchronous lock wrappers with `Arc` built in.
- `ArcAsyncMutex`, `ArcAsyncRwLock`: Tokio-based asynchronous lock wrappers.
- `Monitor`, `ArcMonitor`, `MonitorGuard`: parking_lot-based condition coordination.
- `StdMonitor`, `ArcStdMonitor`, `StdMonitorGuard`: std-based condition coordination.
- Closure-based APIs that keep lock acquisition and release scoped to one call.
- `Arc*` wrappers implement `Deref` and `AsRef`, so the native guard-based
  APIs of the wrapped primitive remain available when needed.

## Installation

```toml
[dependencies]
qubit-lock = "0.4.0"
```

The async wrappers use Tokio synchronization primitives. If your application
creates a Tokio runtime, enable the appropriate Tokio runtime features in your
own `Cargo.toml`, such as `rt` or `rt-multi-thread`.

## Quick Start

### Synchronous lock

```rust
use qubit_lock::{ArcMutex, Lock};

fn main() {
    let counter = ArcMutex::new(0);
    counter.write(|value| *value += 1);
    assert_eq!(counter.read(|value| *value), 1);
}
```

### Native lock APIs

`Arc*` wrappers can still use the native lock APIs of their wrapped
primitives through `Deref` or `AsRef`.

```rust
use qubit_lock::{ArcMutex, Lock};

fn main() {
    let counter = ArcMutex::new(0);

    {
        let mut guard = counter.lock();
        *guard += 1;
    }

    counter.write(|value| *value += 1);
    assert_eq!(counter.read(|value| *value), 2);
}
```

For `ArcRwLock` and `ArcAsyncRwLock`, the closure-based `read` and `write`
methods have the same names as the native guard-based methods. When `Lock` or
`AsyncLock` is in scope, use `lock.as_ref().read()` or explicit dereferencing
such as `(*lock).read()` to call the native guard API.

### Monitor

```rust
use qubit_lock::monitor::ArcMonitor;

fn main() {
    let monitor = ArcMonitor::new(vec![1, 2, 3]);
    let length = monitor.read(|items| items.len());
    assert_eq!(length, 3);
}
```

## Project Layout

- `src/lock`: lock traits and lock wrappers.
- `src/monitor`: parking_lot and std monitor primitives.
- `tests/lock`: lock behavior tests.
- `tests/monitor`: monitor behavior tests.
- `tests/docs`: README and doctext consistency tests.

## Quality Checks

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## License

Apache-2.0
