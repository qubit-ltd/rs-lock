# Qubit Lock

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-lock.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-lock)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-lock/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-lock?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Doc](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的锁工具 crate。它提供同步锁、异步锁、基于条件变量的 monitor 协调能力，以及可复用的双重检查锁执行器。

## 特性

- `ArcMutex`、`ArcRwLock`、`ArcStdMutex`：内部已集成 `Arc` 的同步锁包装器。
- `ArcAsyncMutex`、`ArcAsyncRwLock`：基于 Tokio 的异步锁包装器。
- `Monitor`、`ArcMonitor`、`MonitorGuard`：基于条件变量的状态协调工具。
- `DoubleCheckedLockExecutor`：封装“锁外先检查、加锁后再检查”的复用流程。
- 基于闭包的访问接口，让加锁和释放始终局限在一次调用内部。

## 安装

```toml
[dependencies]
qubit-lock = "0.1.0"
```

## 快速开始

### 同步锁

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

### 双重检查锁

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

## 项目结构

- `src/lock`：锁 trait、锁包装器和 monitor 原语。
- `src/double_checked`：双重检查锁执行器及其 builder。
- `tests/lock`：锁与 monitor 行为测试。
- `tests/double_checked`：双重检查锁行为测试。
- `tests/docs`：README 与文档文本一致性测试。

## 质量检查

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## 许可证

Apache-2.0
