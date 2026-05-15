# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Doc](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的锁工具 crate。它提供同步锁、异步锁与基于条件变量的 monitor 协调能力。

## 特性

- `ArcMutex`、`ArcRwLock`：基于 parking_lot、内部已集成 `Arc` 的同步锁包装器。
- `ArcStdMutex`、`ArcStdRwLock`：基于标准库、保留 poison 语义的同步锁包装器。
- `ArcAsyncMutex`、`ArcAsyncRwLock`：默认 `async` 特性启用的 Tokio 异步锁包装器。
- `Monitor`、`ArcMonitor`、`MonitorGuard`：基于 parking_lot 的条件变量协调工具。
- `StdMonitor`、`ArcStdMonitor`、`StdMonitorGuard`：基于标准库的条件变量协调工具。
- 基于闭包的访问接口，让加锁和释放始终局限在一次调用内部。
- `Arc*` 包装器实现了 `Deref` 和 `AsRef`，需要时仍可使用底层同步原语的
  guard 风格原生接口。

## 安装

```toml
[dependencies]
qubit-lock = "0.7"
```

异步锁包装器使用 Tokio 同步原语，并默认启用。只需要同步锁与 Monitor、且希望依赖图中不包含 Tokio 的使用方，可以关闭默认特性：

```toml
[dependencies]
qubit-lock = { version = "0.7", default-features = false }
```

如果应用需要创建 Tokio runtime，请在应用自己的 `Cargo.toml` 中启用合适的 Tokio runtime 特性，例如 `rt` 或 `rt-multi-thread`。
`AsyncLock` 返回 `Send` future：`ArcAsyncMutex<T>` 在 `T: Send` 时实现它，
`ArcAsyncRwLock<T>` 在 `T: Send + Sync` 时实现它。

## 从 0.6 迁移

`0.7` 包含有意的破坏性 API 清理：

- `ArcRwLock` 现在包装 `parking_lot::RwLock`，不再使用 poison 语义。持锁
  panic 之后，后续加锁会继续正常工作，`try_read` / `try_write` 不再返回
  `TryLockError::Poisoned`。
- 如果需要标准库 `std::sync::RwLock` 的 poison 语义，请使用
  `ArcStdRwLock`。
- `qubit_lock::lock` 和 `qubit_lock::monitor` 不再作为公开模块暴露。
  请直接从 crate root 导入公开类型。
- 各包装类型在适用时新增了 `From<T>` 和 `Default` 便捷构造。

## 快速开始

### 同步锁

```rust
use qubit_lock::{ArcMutex, Lock};

fn main() {
    let counter = ArcMutex::new(0);
    counter.write(|value| *value += 1);
    assert_eq!(counter.read(|value| *value), 1);
}
```

### 原生锁接口

`Arc*` 包装器可以通过 `Deref` 或 `AsRef` 继续使用底层同步原语的原生锁接口。

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

对 `ArcRwLock` 和 `ArcAsyncRwLock`，闭包式 `read` / `write` 与底层
guard 风格方法同名。当 `Lock` 或 `AsyncLock` 在作用域中时，如果要调用底层
guard API，请使用 `lock.as_ref().read()`，或用 `(*lock).read()` 显式解引用。

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

## 项目结构

- `src/lock`：锁 trait 与锁包装器。
- `src/monitor`：基于 parking_lot 和标准库的 monitor 原语。
- `tests/lock`：锁相关行为测试。
- `tests/monitor`：monitor 相关行为测试。
- `tests/docs`：README 与文档文本一致性测试。

## 质量检查

在仓库 checkout 中执行：

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## 许可证

Apache-2.0
