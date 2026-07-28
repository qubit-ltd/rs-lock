# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

## The problem

Rust applications often mix `std`, `parking_lot`, and Tokio locks. Their
concrete APIs differ, so reusable code becomes tied to a backend even when it
only needs to acquire a lock or access protected data.

Condition coordination adds another problem: a lock alone cannot express
"wait until this predicate becomes true." Correct condition-variable code must
keep state updates, predicate checks, waiter registration, and notification in
one protocol. Timeout tests then become slow and flaky when they depend on real
sleeps.

`qubit-lock` provides backend-independent lock capabilities, closure-based data
access, synchronous and asynchronous monitors, and injectable timers for
deterministic tests.

## Quick start

`DataLock` gives supported mutexes and read-write locks the same closure-based
read/write interface:

```rust
use qubit_lock::DataLock;

fn main() {
    let counter = std::sync::Mutex::new(0);
    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 1);
}
```

## Complete user guide

The [English user guide](doc/user_guide.md) explains the motivating
producer-consumer example, every public component, feature selection, monitor
semantics, timed waits, deterministic testing, and common mistakes.

The [Chinese user guide](doc/user_guide.zh_CN.md) covers the same material in
Chinese.

## Features

- `Lock`, `ExclusiveLock`, `ReadWriteLock`, and `DataLock<T>` provide common
  synchronous capabilities for `std::sync::Mutex<T>`,
  `std::sync::RwLock<T>`, `parking_lot::Mutex<T>`, and
  `parking_lot::RwLock<T>`.
- `AsyncLock`, `AsyncReadWriteLock`, and `AsyncDataLock<T>` provide matching
  Tokio capabilities behind `async-lock`.
- `ParkingLotMonitor` and `StdMonitor`, including standard
  `Arc<ParkingLotMonitor<T>>` and `Arc<StdMonitor<T>>` handles, provide
  blocking predicate coordination.
- `TokioMonitor` provides asynchronous coordination behind
  `async-monitor`.
- Every concrete monitor supports Timer injection for deterministic tests.

Import public types directly from the crate root.

## Installation

The default feature set is empty. Enable `ParkingLotMonitor` explicitly:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor", "parking-lot"] }
```

Use `StdMonitor` without a `parking_lot` dependency:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor"] }
```

Use only the synchronous lock traits and standard-library implementations:

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false }
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

If the application creates a Tokio runtime, enable its required runtime
features in the application's own `Cargo.toml`.

## Condition-wait semantics

Timed monitor waits align with `std::sync::Condvar::wait_timeout_while`.
Their timeout is a condition-wait budget: after acquiring the state lock and
before the first predicate check, the monitor samples one fixed deadline. The
initial lock acquisition is excluded, predicate checks consume the budget, and
the method may return after the timeout while reacquiring the state lock.
Blocking `*_with_total_timeout` methods instead fix their deadline before
initial lock acquisition, so contention consumes the operation-wide budget.
They are still not a hard return-time guarantee because mutex reacquisition
and the ready action cannot be interrupted. See the
[English user guide](doc/user_guide.md) or
[中文用户手册](doc/user_guide.zh_CN.md) for zero-timeout, error, cancellation,
and whole-call-deadline semantics.

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
