# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Build reusable concurrent components around the operations they need, not
around a particular lock implementation. Choose the `std`, `parking_lot`, or
Tokio backend where you assemble the application. `qubit-lock` provides the
common interfaces for protected-data access and state-based waiting, and lets
you test timeout behavior with a controllable clock.

## Why this crate exists

A local `Mutex` in one function rarely needs another abstraction. The value
appears when a component must be reused or its concurrency policy must change:

- A reusable component should not need separate implementations for
  `std::sync::Mutex`, `std::sync::RwLock`, and `parking_lot` locks just because
  their acquisition APIs and guard types differ.
- A lock protects state, but it does not define the protocol for “wait until
  this predicate becomes true.” Correct code must coordinate state changes,
  predicate checks, waiter registration, and notifications.
- Tests that call real sleep functions are slow and race-prone. Timeout
  behavior is more useful when the production wait algorithm runs against a
  controllable clock.

`qubit-lock` supplies capability traits for the first problem. Its monitor
implementations handle the waiting protocol and support injected time for the
second and third.

Synchronous adapters support `std::sync::Mutex<T>`, `std::sync::RwLock<T>`,
`parking_lot::Mutex<T>`, and `parking_lot::RwLock<T>` when the corresponding
feature is enabled.

## When not to use this crate

Use the native lock directly when it is local to one implementation, the
backend will not vary, and no condition waiting or deterministic timeout test
is involved. Add this crate when a public or reusable boundary needs a
backend-neutral contract, or when coordinating waiters becomes part of the
domain behavior.

## See the lock abstraction pay off

The following domain functions only require read and write access to
`ServiceStats`. They work unchanged whether the caller uses a mutex in a test
or a read-write lock in a read-heavy service.

```rust
use qubit_lock::DataLock;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
struct ServiceStats {
    accepted: u64,
    rejected: u64,
}

enum Outcome {
    Accepted,
    Rejected,
}

fn record<L>(stats: &L, outcome: Outcome)
where
    L: DataLock<ServiceStats>,
{
    stats.with_write(|stats| match outcome {
        Outcome::Accepted => stats.accepted += 1,
        Outcome::Rejected => stats.rejected += 1,
    });
}

fn snapshot<L>(stats: &L) -> ServiceStats
where
    L: DataLock<ServiceStats>,
{
    stats.with_read(Clone::clone)
}

fn main() {
    let test_stats: std::sync::Mutex<ServiceStats> =
        std::sync::Mutex::new(ServiceStats::default());
    record(&test_stats, Outcome::Accepted);
    assert_eq!(snapshot(&test_stats).accepted, 1);

    let service_stats: std::sync::RwLock<ServiceStats> =
        std::sync::RwLock::new(ServiceStats::default());
    record(&service_stats, Outcome::Accepted);
    record(&service_stats, Outcome::Rejected);
    assert_eq!(
        snapshot(&service_stats),
        ServiceStats {
            accepted: 1,
            rejected: 1,
        },
    );
}
```

With the `parking-lot` feature enabled, the same functions also accept
`parking_lot::Mutex<ServiceStats>` and `parking_lot::RwLock<ServiceStats>`.
The caller chooses the locking and dependency policy; the component keeps one
business implementation.

| Without this abstraction | With `qubit-lock` |
| --- | --- |
| A component commits to one concrete lock type | A component declares `DataLock<T>` |
| Domain code branches between `lock`, `read`, and `write` | Domain code uses `with_read` and `with_write` |
| Guard and poisoning entry points leak into the component | The capability boundary owns backend acquisition details |
| Replacing a backend changes business code | The backend is selected by the caller |

For operations that need a guard rather than data-bound closures, use `Lock`.
When the algorithm requires true exclusive entry, require `ExclusiveLock`;
`Lock` alone can also represent a read-mode adapter. `ReadWriteLock` preserves
shared and exclusive modes and exposes `read_lock()` and `write_lock()`
adapters.

## When a lock is not enough

A work queue, readiness gate, or connection pool needs more than mutual
exclusion. A worker must wait until a condition on the shared state is true.
Producers must change that state and notify waiters without losing a wakeup.
Shutdown must wake every affected waiter, and timeout tests should not depend
on real sleeps.

`ParkingLotMonitor` and `StdMonitor` give blocking code the same interface for
state-based waiting. `TokioMonitor` is the asynchronous counterpart. The
[English user guide](doc/user_guide.md) builds a closable, bounded task queue
whose domain logic runs unchanged on `StdMonitor` and `ParkingLotMonitor`,
then tests the real timeout path with a manual clock.

The [complete runnable bounded-queue example](examples/bounded_queue.rs)
exercises the same blocking queue behavior with `ParkingLotMonitor`.

## Choose a capability

| Need | Start with |
| --- | --- |
| Read or mutate data stored in a lock | `DataLock<T>` |
| Abstract one guard acquisition mode | `Lock` |
| Require true exclusive acquisition | `ExclusiveLock` |
| Preserve explicit shared and exclusive modes | `ReadWriteLock` |
| Coordinate blocking predicate waits | `ParkingLotMonitor` or `StdMonitor` |
| Coordinate Tokio predicate waits | `TokioMonitor` |
| Express a reusable monitor dependency | The narrowest capability trait |
| Test deadlines without sleeping | `with_timer` and `ManualMonotonicClock` |

All public types are exported from and should be imported directly from the crate root.

## Installation and features

The default feature set is empty. Enable only the components used by the
program:

| Feature | What it enables |
| --- | --- |
| no optional features | Synchronous lock traits and `std` lock implementations |
| `parking-lot` | Implementations for `parking_lot` mutexes and read-write locks |
| `monitor` | Monitor traits, `StdMonitor`, timed waits, and timer injection |
| `async-lock` | Tokio lock traits and adapters |
| `async-monitor` | `async-lock`, monitor support, and `TokioMonitor` |
| default | no optional features |

Use only synchronous lock traits and standard-library implementations:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false }
```

Use `StdMonitor` without a `parking_lot` dependency:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor"] }
```

Use `ParkingLotMonitor`:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor", "parking-lot"] }
```

Enable Tokio locks without Tokio monitors:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["async-lock"] }
```

Enable Tokio monitors and timed waits:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["async-monitor"] }
```

Applications that create a Tokio runtime must enable their required runtime
features, such as `rt` or `rt-multi-thread`, in their own `Cargo.toml`.

## Condition-wait semantics

Monitor notifications are memoryless: `notify_one` selects at most one
already registered waiter, while a notification with no waiter has no future
effect. Store readiness in protected state; a wakeup only asks a waiter to
check its predicate again.

Timed monitor waits align with `std::sync::Condvar::wait_timeout_while`. A
relative timeout is a condition-wait budget: after acquiring the state lock
and before the first predicate check, the monitor samples one fixed deadline.
Initial lock acquisition is excluded, but predicate checks and waiting consume
the budget. A wait may return after the timeout while reacquiring the state
lock. Blocking `*_with_total_timeout` methods instead fix their deadline
before initial lock acquisition, so contention consumes the operation-wide
budget. Neither form is a hard return-time guarantee because lock
reacquisition and a ready action cannot be interrupted.

See the user guide for zero-timeout behavior, Timer registration and
completion errors, cancellation, external predicate state, and total-timeout
semantics.

## Project layout

- `src/lock`: lock traits and native lock adapters.
- `src/monitor`: monitor traits and parking_lot, std, and Tokio
  implementations.
- `doc`: English and Chinese user guides.
- `tests/lock`: lock behavior tests.
- `tests/monitor`: monitor behavior tests.
- `tests/docs`: public-document consistency tests.

## Related projects

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
