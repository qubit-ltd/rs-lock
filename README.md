# Qubit Lock

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-lock.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-lock)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-lock/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-lock?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Lock-focused utilities for the Qubit Rust libraries. The crate provides synchronous and asynchronous lock wrappers plus monitor-style coordination.

## Features

- `ArcMutex`, `ArcRwLock`, `ArcStdMutex`: synchronous lock wrappers with `Arc` built in.
- `ArcAsyncMutex`, `ArcAsyncRwLock`: Tokio-based asynchronous lock wrappers.
- `Monitor`, `ArcMonitor`, `MonitorGuard`: condition-based state coordination.
- Closure-based APIs that keep lock acquisition and release scoped to one call.

## Installation

```toml
[dependencies]
qubit-lock = "0.3.0"
```

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
- `src/monitor`: `Monitor` / `ArcMonitor` and related condition-variable primitives.
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
