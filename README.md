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
- `ArcAsyncMutex`, `ArcAsyncRwLock`: Tokio-based asynchronous lock wrappers
  behind the optional `async` feature.
- `ParkingLotMonitor`, `ArcParkingLotMonitor`, `ParkingLotMonitorGuard`: parking_lot-based condition coordination.
- `StdMonitor`, `ArcStdMonitor`, `StdMonitorGuard`: std-based condition coordination.
- `MockMonitor`, `ArcMockMonitor`: deterministic monitor testing behind the
  optional `mock` feature, driven by a shared
  `qubit_clock::ManualMonotonicClock`.
- `TokioMonitor`, `ArcTokioMonitor`: async monitor coordination behind the
  optional `async` feature.
- Closure-based APIs that keep lock acquisition and release scoped to one call.
- `Arc*` wrappers implement `Deref` and `AsRef`, so the native guard-based
  APIs of the wrapped primitive remain available when needed.

## Installation

```toml
[dependencies]
qubit-lock = "0.10"
```

The default feature set contains the synchronous locks and monitors only.
Enable asynchronous or deterministic-test support explicitly when needed:

```toml
[dependencies]
qubit-lock = { version = "0.10", features = ["async", "mock"] }
```

If your application creates a Tokio runtime, enable the appropriate Tokio
runtime features in your own `Cargo.toml`, such as `rt` or `rt-multi-thread`.
`AsyncLock` returns `Send` futures: `ArcAsyncMutex<T>` implements it for
`T: Send`, while `ArcAsyncRwLock<T>` implements it for `T: Send + Sync`.

### Deterministic monitor time

Enable the `mock` feature to use `MockMonitor` and `ArcMockMonitor`.
`MockMonitor::new` creates an independent `ManualMonotonicClock`. Use
`monotonic_clock()` to advance it explicitly. When several test components
must observe the same time domain, construct them from the same clock with
`MockMonitor::from_clock` or `ArcMockMonitor::from_clock`. Code that constructs
the shared clock directly must also declare `qubit-clock = "0.9"` as a direct
dependency:

```rust
use std::{sync::Arc, time::Duration};

use qubit_clock::ManualMonotonicClock;
use qubit_lock::ArcMockMonitor;

let clock = Arc::new(ManualMonotonicClock::new());
let monitor = ArcMockMonitor::from_clock(false, Arc::clone(&clock));

clock.advance(Duration::from_secs(10)).unwrap();
assert_eq!(monitor.elapsed(), Duration::from_secs(10));
```

Advancing the clock wakes blocking and asynchronous timeout waiters; no wall
clock delay is involved. Blocking tests can call
`wait_for_timeout_waiters(expected_count, real_timeout)` before advancing mock
time instead of guessing waiter registration with a real sleep.
`pending_timeout_waiters()` counts blocking and asynchronous timeout waits that
are ready to observe changes. An async wait starts contributing after its future
is first polled, and unregisters automatically when cancelled. Do not advance
the clock from inside a closure that currently holds the same monitor's state
lock.

## Migration from 0.9

Version `0.10` intentionally changes features and closure-method names:

- The default feature set is now synchronous only. Enable `async` explicitly
  for Tokio lock and monitor types.
- `MockMonitor`, `ArcMockMonitor`, and the `qubit-clock` dependency are behind
  the new `mock` feature. Enable both `async` and `mock` for async mock waits.
- Closure-scoped lock methods were renamed: `read` to `with_read`, `write` to
  `with_write`, `try_read` to `try_with_read`, and `try_write` to
  `try_with_write`.
- Synchronous monitor state helpers were renamed: `read` to `with_read`,
  `write` to `with_write`, `write_notify_one` to `with_write_notify_one`, and
  `write_notify_all` to `with_write_notify_all`.
- Tokio monitor state helpers now use the `_async` suffix consistently:
  `async_read` to `with_read_async`, `async_write` to `with_write_async`,
  `async_write_notify_one` to `with_write_notify_one_async`, and
  `async_write_notify_all` to `with_write_notify_all_async`.
- Notification-only waiting traits and concrete `wait`, `wait_for`,
  `wait_async`, and `wait_for_async` methods have been removed. Coordinate on
  protected state with predicate-based condition waits instead. Code that
  needs queued permits should use a semaphore or event primitive.

## Migration from 0.8

Version `0.9` contains intentional async monitor API renames:

- `AsyncConditionWaiter::async_wait_until` and `async_wait_while` are now
  `wait_until_async` and `wait_while_async`.
- `AsyncTimeoutConditionWaiter::async_wait_until_for` and
  `async_wait_while_for` are now `wait_until_for_async` and
  `wait_while_for_async`.
- Condition-wait traits provide default `wait_until*` implementations through
  their corresponding `wait_while*` methods.

## Migration from 0.7

Version `0.8` contains intentional breaking API cleanup:

- `Monitor` is now an aggregate trait for blocking monitor capabilities.
- The concrete parking_lot monitor is now `ParkingLotMonitor`; its cloneable
  handle is `ArcParkingLotMonitor`.
- Timeout condition methods are named `wait_until_for` and `wait_while_for`.
- `MockMonitor` and `ArcMockMonitor` use a `ManualMonotonicClock` for
  deterministic timeout tests.
- With the `async` feature, `TokioMonitor` and `ArcTokioMonitor` provide async
  monitor operations.
- `qubit_lock::lock` and `qubit_lock::monitor` are no longer public modules.
  Import public types directly from the crate root.

## Quick Start

### Synchronous lock

```rust
use qubit_lock::{ArcMutex, Lock};

fn main() {
    let counter = ArcMutex::new(0);
    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 1);
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

    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 2);
}
```

The `with_read` and `with_write` names distinguish closure-scoped access from
native guard acquisition. A read-write-lock wrapper can therefore acquire a
native guard directly with `lock.read()` or `lock.write()`; `lock.as_ref()`
remains available when the wrapped type should be explicit.

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

    monitor.with_write_notify_one(|items| items.push(7));

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

Copyright (c) 2025 - 2026. Haixing Hu.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

See [LICENSE](LICENSE) for the full license text.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Guidelines

- Follow Rust API Guidelines
- Keep comprehensive test coverage
- Document and provide examples for all public APIs
- Ensure all tests pass before submitting a PR

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

## Related Projects

More Qubit Rust libraries are published under the
[qubit-ltd](https://github.com/qubit-ltd) GitHub organization.

---

Repository: [https://github.com/qubit-ltd/rs-lock](https://github.com/qubit-ltd/rs-lock)
