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
- `Monitor`, `ArcMonitor`, `MonitorGuard`: parking_lot-based condition coordination.
- `StdMonitor`, `ArcStdMonitor`, `StdMonitorGuard`: std-based condition coordination.
- Closure-based APIs that keep lock acquisition and release scoped to one call.
- `Arc*` wrappers implement `Deref` and `AsRef`, so the native guard-based
  APIs of the wrapped primitive remain available when needed.

## Installation

```toml
[dependencies]
qubit-lock = "0.7"
```

The async wrappers use Tokio synchronization primitives and are enabled by
default. For sync-only users that want to avoid Tokio in the dependency graph:

```toml
[dependencies]
qubit-lock = { version = "0.7", default-features = false }
```

If your application creates a Tokio runtime, enable the appropriate Tokio
runtime features in your own `Cargo.toml`, such as `rt` or `rt-multi-thread`.
`AsyncLock` returns `Send` futures: `ArcAsyncMutex<T>` implements it for
`T: Send`, while `ArcAsyncRwLock<T>` implements it for `T: Send + Sync`.

## Migration from 0.6

Version `0.7` contains intentional breaking API cleanup:

- `ArcRwLock` now wraps `parking_lot::RwLock` and no longer uses poisoning.
  After a panic while holding the lock, future acquisitions continue normally
  and `try_read` / `try_write` do not return `TryLockError::Poisoned`.
- Use `ArcStdRwLock` when standard-library `std::sync::RwLock` poisoning
  semantics are required.
- `qubit_lock::lock` and `qubit_lock::monitor` are no longer public modules.
  Import public types directly from the crate root.
- Wrapper types now implement convenient `From<T>` and `Default`
  constructors where applicable.

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
use qubit_lock::ArcMonitor;

fn main() {
    let monitor = ArcMonitor::new(Vec::<i32>::new());
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
