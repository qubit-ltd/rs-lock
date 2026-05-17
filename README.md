# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Lock-focused utilities for the Qubit Rust libraries. The crate provides synchronous and asynchronous lock wrappers plus monitor-style coordination.

## Features

- `ArcMutex`, `ArcRwLock`: parking_lot-based synchronous lock wrappers with `Arc` built in.
- `ArcStdMutex`, `ArcStdRwLock`: standard-library lock wrappers for callers that need poison semantics.
- `ArcAsyncMutex`, `ArcAsyncRwLock`: Tokio-based asynchronous lock wrappers enabled by the default `async` feature.
- `ParkingLotMonitor`, `ArcParkingLotMonitor`, `ParkingLotMonitorGuard`: parking_lot-based condition coordination.
- `StdMonitor`, `ArcStdMonitor`, `StdMonitorGuard`: std-based condition coordination.
- `MockMonitor`, `ArcMockMonitor`: deterministic monitor testing with manually advanced timeout time.
- `TokioMonitor`, `ArcTokioMonitor`: async monitor coordination with Tokio.
- Closure-based APIs that keep lock acquisition and release scoped to one call.
- `Arc*` wrappers implement `Deref` and `AsRef`, so the native guard-based
  APIs of the wrapped primitive remain available when needed.

## Installation

```toml
[dependencies]
qubit-lock = "0.8"
```

The async wrappers use Tokio synchronization primitives and are enabled by
default. For sync-only users that want to avoid Tokio in the dependency graph:

```toml
[dependencies]
qubit-lock = { version = "0.8", default-features = false }
```

If your application creates a Tokio runtime, enable the appropriate Tokio
runtime features in your own `Cargo.toml`, such as `rt` or `rt-multi-thread`.
`AsyncLock` returns `Send` futures: `ArcAsyncMutex<T>` implements it for
`T: Send`, while `ArcAsyncRwLock<T>` implements it for `T: Send + Sync`.

## Migration from 0.7

Version `0.8` contains intentional breaking API cleanup:

- `Monitor` is now an aggregate trait for blocking monitor capabilities.
- The concrete parking_lot monitor is now `ParkingLotMonitor`; its cloneable
  handle is `ArcParkingLotMonitor`.
- Timeout condition methods are named `wait_until_for` and `wait_while_for`.
- `MockMonitor` and `ArcMockMonitor` provide manually advanced timeout time for
  deterministic tests.
- With the default `async` feature, `TokioMonitor` and `ArcTokioMonitor`
  provide async monitor operations.
- `qubit_lock::lock` and `qubit_lock::monitor` are no longer public modules.
  Import public types directly from the crate root.

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

### ParkingLotMonitor

```rust
use qubit_lock::ArcParkingLotMonitor;

fn main() {
    let monitor = ArcParkingLotMonitor::new(Vec::<i32>::new());
    let worker_monitor = monitor.clone();

    let worker = std::thread::spawn(move || {
        worker_monitor.wait_until(
            |items| !items.is_empty(),
            |items| items.pop().expect("item should be ready"),
        )
    });

    monitor.write_notify_one(|items| items.push(7));

    assert_eq!(worker.join().expect("worker should finish"), 7);
}
```

## Project Layout

- `src/lock`: lock traits and lock wrappers.
- `src/monitor`: monitor traits plus parking_lot, std, Tokio, and mock
  monitor implementations.
- `tests/lock`: lock behavior tests.
- `tests/monitor`: monitor behavior tests.
- `tests/docs`: README and doctest consistency tests.

## Quality Checks

From a repository checkout:

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## License

Apache-2.0
