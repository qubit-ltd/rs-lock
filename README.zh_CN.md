# Qubit Lock

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-lock.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-lock)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-lock/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-lock?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Doc](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的锁工具 crate。它提供同步锁、异步锁与基于条件变量的 monitor 协调能力。

## 特性

- `ArcMutex`、`ArcRwLock`、`ArcStdMutex`：内部已集成 `Arc` 的同步锁包装器。
- `ArcAsyncMutex`、`ArcAsyncRwLock`：基于 Tokio 的异步锁包装器。
- `Monitor`、`ArcMonitor`、`MonitorGuard`：基于条件变量的状态协调工具。
- 基于闭包的访问接口，让加锁和释放始终局限在一次调用内部。

## 安装

```toml
[dependencies]
qubit-lock = "0.3.0"
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
use qubit_lock::monitor::ArcMonitor;

fn main() {
    let monitor = ArcMonitor::new(vec![1, 2, 3]);
    let length = monitor.read(|items| items.len());
    assert_eq!(length, 3);
}
```

## 项目结构

- `src/lock`：锁 trait 与锁包装器。
- `src/monitor`：基于条件变量的 `Monitor` / `ArcMonitor` 等原语。
- `tests/lock`：锁相关行为测试。
- `tests/monitor`：monitor 相关行为测试。
- `tests/docs`：README 与文档文本一致性测试。

## 质量检查

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## 许可证

Apache-2.0
