# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Lock-focused utilities for the Qubit Rust libraries. The crate provides
generic lock capabilities plus monitor-style coordination.

## Features

- `Lock`: data-independent exclusive locking with an RAII guard, implemented
  by `std::sync::Mutex<T>` and `parking_lot::Mutex<T>`.
- `ReadWriteLock`: data-independent shared/exclusive locking, implemented by
  `std::sync::RwLock<T>` and `parking_lot::RwLock<T>`.
- `DataLock<T>`: closure-based access to data protected by any supported mutex
  or read-write lock.
- `AsyncLock`, `AsyncReadWriteLock`, and `AsyncDataLock<T>`: equivalent Tokio
  lock capabilities behind the optional `async-lock` feature.
- `ParkingLotMonitor`, `ArcParkingLotMonitor`, `ParkingLotMonitorGuard`: parking_lot-based condition coordination.
- `StdMonitor`, `ArcStdMonitor`, `StdMonitorGuard`: std-based condition coordination.
- `TokioMonitor`, `ArcTokioMonitor`: async monitor coordination behind the
  optional `async-monitor` feature.
- Timer injection on every monitor for deterministic integration tests that
  execute the production wait algorithm.
- Implementations for borrowed and `Arc`-owned locks, without wrapper types.

## Installation

```toml
[dependencies]
qubit-lock = "0.11"
```

The default feature set enables `monitor` and `parking-lot`, preserving the
complete synchronous API. Lock-only users can avoid both optional dependencies:

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false }
```

Enable asynchronous locks without Tokio monitor deadlines when needed:

```toml
[dependencies]
qubit-lock = { version = "0.11", features = ["async-lock"] }
```

Enable Tokio monitor coordination, including timed waits, when needed:

```toml
[dependencies]
qubit-lock = { version = "0.11", features = ["async-monitor"] }
```

The legacy `async` compatibility alias enables `async-monitor`. If your
application creates a Tokio runtime, enable the appropriate Tokio runtime
features in your own `Cargo.toml`, such as `rt` or `rt-multi-thread`.
`AsyncLock` and `AsyncReadWriteLock` return `Send` futures. Tokio mutexes
implement the former when `T: Send`; Tokio read-write locks implement the
latter when `T: Send + Sync`.

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
construction and time before its first poll consume no timeout budget.
`TokioMonitor::current` and `ArcTokioMonitor::current` bind their default Timer
to the currently entered runtime; their `try_current` variants report a missing
runtime without panicking. The bound runtime must have its time driver enabled
when a nonzero timed wait actually suspends. `with_timer` remains the explicit
injection path and inherits the supplied Timer's driver requirements.
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

### Deterministic monitor time

Every concrete monitor exposes `with_timer`. Integration tests inject a
`ManualTimer` into the same `ParkingLotMonitor`, `StdMonitor`, or
`TokioMonitor` type used in production; there is no separate mock wait
algorithm. Code that constructs the manual clock declares
`qubit-clock = "0.9"` as a direct dependency:

```rust
use std::{sync::Arc, thread, time::Duration};

use qubit_clock::{ManualMonotonicClock, MonotonicClock};
use qubit_lock::{ParkingLotMonitor, WaitTimeoutResult};

let clock = ManualMonotonicClock::new_shared();
let monitor = Arc::new(ParkingLotMonitor::with_timer(false, clock.new_timer()));
let waiter_monitor = Arc::clone(&monitor);
let waiter = thread::spawn(move || {
    waiter_monitor.wait_until_for(
        Duration::from_secs(16),
        |ready| *ready,
        |_| (),
    )
});

let _ = clock.advance_to_next_deadline_after_waiters(
    1,
    Duration::from_secs(1),
);
assert_eq!(waiter.join().unwrap(), Ok(WaitTimeoutResult::TimedOut));
```

`ManualMonotonicClock` is the test control plane. Its waiter/deadline observer
APIs coordinate advancement without guessing registration with a real sleep.
The Monitor and Timer registrations are cancellation-safe, and multiple
components can share one manual clock domain.

Timed predicate methods return `Result<WaitTimeoutResult<_>, TimeError>` so
Timer registration failures remain distinct from a real timeout. Guard waits
use in-place `wait`, `wait_for`, and `wait_until` methods; a Timer error leaves
the guard held and usable.

## Quick Start

### Data-bound lock

```rust
use qubit_lock::DataLock;

fn main() {
    let counter = parking_lot::Mutex::new(0);
    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 1);
}
```

### Data-independent lock

Use `Lock` when the protected state lives elsewhere, for example in atomics.
The guard releases the lock automatically when it leaves scope.

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

use qubit_lock::Lock;

fn main() {
    let gate = std::sync::Mutex::new(());
    let counter = AtomicUsize::new(0);

    {
        let _guard = Lock::lock(&gate);
        counter.fetch_add(1, Ordering::Relaxed);
    }

    assert_eq!(counter.load(Ordering::Relaxed), 1);
}
```

`std::sync::Mutex<T>`, `std::sync::RwLock<T>`, `parking_lot::Mutex<T>`, and
`parking_lot::RwLock<T>` implement `DataLock<T>`. Read-write locks implement
`ReadWriteLock`; use `read_lock()` or `write_lock()` to adapt one side to the
exclusive `Lock` capability.

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
- `src/monitor`: monitor traits plus parking_lot, std, and Tokio monitor
  implementations.
- `tests/lock`: lock behavior tests.
- `tests/monitor`: monitor behavior tests.
- `tests/docs`: README and doctest consistency tests.

## Related Projects

More Qubit Rust libraries are published under the
[qubit-ltd](https://github.com/qubit-ltd) GitHub organization.

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-lock](https://github.com/qubit-ltd/rs-lock)
