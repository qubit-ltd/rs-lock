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

## Monitor semantics

Monitor notifications use memoryless condition-variable semantics.
`notify_one` selects at most one of the already registered waiters, while a
notification with no registered waiter has no future effect. A wakeup only
prompts another protected predicate check; it neither makes the predicate true
nor guarantees fairness.

A relative timeout is a condition-wait budget. Initial state-lock contention
and the initial predicate check are excluded. Once that check determines that
waiting is required, the monitor establishes one fixed deadline immediately
before the first condition-wait suspension and reuses it across wakeups. A zero
timeout still checks the predicate, and the final locked predicate check wins
over timeout.

Async monitor traits return `impl Future`; the returned future is lazy, so
construction and time before its first poll consume no timeout budget. A Tokio
time driver is needed only when a timed wait actually enters a nonzero timed
suspension, in which case the runtime must have the time driver enabled.
Dropping a pending future unregisters its active waiter, does not run the
action, and does not roll back protected-state changes made by other tasks. If
`notify_one` already selected that waiter, cancellation discards that selection
instead of transferring it to another or future waiter.

Arc-backed monitor wrappers keep explicit trait implementations for generic
code, while ordinary monitor method calls resolve through `Deref`. Their
`from_arc`, `as_arc`, and `into_arc` methods make the shared-ownership boundary
explicit.

### Choosing monitor capabilities

For ordinary application code, prefer a concrete implementation:
`ParkingLotMonitor` or `StdMonitor` for blocking coordination and
`TokioMonitor` for asynchronous coordination. Choose the corresponding
`Arc*Monitor` handle when ownership must be cloned or retained.

At generic API boundaries, use the narrowest capability that expresses the
operation: `Notifier` for signaling, `ConditionWaiter` or
`TimeoutConditionWaiter` for blocking predicate waits, and their
`AsyncConditionWaiter` or `AsyncTimeoutConditionWaiter` counterparts for
asynchronous waits. Use `Monitor` or `AsyncMonitor` only when the full
notification-and-wait contract is required; use `SharedMonitor` or
`SharedAsyncMonitor` when the generic API also retains a cloneable handle.
These waiter and aggregate traits are intended for static generic bounds, not
`dyn` trait-object interfaces.

Import public types directly from the crate root.

`MockMonitor` and `ArcMockMonitor` are deterministic test implementations for
capability-trait and predicate-wait behavior. They do not provide a mock guard
type and are not replacements for concrete guard-oriented monitor APIs.

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
is first polled, and unregisters automatically when cancelled. Code may also
advance the clock while holding that monitor's state lock; clock callbacks do
not reacquire the protected state.

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
