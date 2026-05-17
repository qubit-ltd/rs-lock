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
- `ParkingLotMonitor`、`ArcParkingLotMonitor`、`ParkingLotMonitorGuard`：基于 parking_lot 的条件变量协调工具。
- `StdMonitor`、`ArcStdMonitor`、`StdMonitorGuard`：基于标准库的条件变量协调工具。
- `MockMonitor`、`ArcMockMonitor`：使用手动推进 timeout 时间的确定性测试 monitor。
- `TokioMonitor`、`ArcTokioMonitor`：基于 Tokio 的异步 monitor 协调工具。
- 基于闭包的访问接口，让加锁和释放始终局限在一次调用内部。
- `Arc*` 包装器实现了 `Deref` 和 `AsRef`，需要时仍可使用底层同步原语的
  guard 风格原生接口。

## 安装

```toml
[dependencies]
qubit-lock = "0.8"
```

异步锁包装器使用 Tokio 同步原语，并默认启用。只需要同步锁与 ParkingLotMonitor、且希望依赖图中不包含 Tokio 的使用方，可以关闭默认特性：

```toml
[dependencies]
qubit-lock = { version = "0.8", default-features = false }
```

如果应用需要创建 Tokio runtime，请在应用自己的 `Cargo.toml` 中启用合适的 Tokio runtime 特性，例如 `rt` 或 `rt-multi-thread`。
`AsyncLock` 返回 `Send` future：`ArcAsyncMutex<T>` 在 `T: Send` 时实现它，
`ArcAsyncRwLock<T>` 在 `T: Send + Sync` 时实现它。

## 从 0.7 迁移

`0.8` 包含有意的破坏性 API 清理：

- `Monitor` 现在是阻塞 monitor 能力的聚合 trait。
- 基于 parking_lot 的具体实现改为 `ParkingLotMonitor`，其可克隆共享句柄改为
  `ArcParkingLotMonitor`。
- 带超时的 condition wait 方法改名为 `wait_until_for` 和 `wait_while_for`。
- `MockMonitor` 和 `ArcMockMonitor` 提供手动推进的 timeout 时间，便于确定性测试。
- 默认 `async` 特性下，`TokioMonitor` 和 `ArcTokioMonitor` 提供异步 monitor 操作。
- `qubit_lock::lock` 和 `qubit_lock::monitor` 不再作为公开模块暴露。
  请直接从 crate root 导入公开类型。

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

    monitor.write_notify_one(|items| items.push(7));

    assert_eq!(worker.join().expect("worker should finish"), 7);
}
```

## 项目结构

- `src/lock`：锁 trait 与锁包装器。
- `src/monitor`：monitor traits，以及 parking_lot、标准库、Tokio 和 mock
  monitor 实现。
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

Copyright (c) 2025 - 2026. Haixing Hu.

根据 Apache 许可证 2.0 版（"许可证"）授权；
除非遵守许可证，否则您不得使用此文件。
您可以在以下位置获取许可证副本：

    http://www.apache.org/licenses/LICENSE-2.0

除非适用法律要求或书面同意，否则根据许可证分发的软件
按"原样"分发，不附带任何明示或暗示的担保或条件。
有关许可证下的特定语言管理权限和限制，请参阅许可证。

完整的许可证文本请参阅 [LICENSE](LICENSE)。

## 贡献

欢迎贡献！请随时提交 Pull Request。

### 开发指南

- 遵循 Rust API 指南
- 保持全面的测试覆盖
- 为所有公共 API 编写文档和示例
- 提交 PR 前确保所有测试通过

## 作者

**胡海星** - *Qubit Co. Ltd.*

## 相关项目

Qubit 旗下的更多 Rust 库发布在 GitHub 组织 [qubit-ltd](https://github.com/qubit-ltd)。

---

仓库地址：[https://github.com/qubit-ltd/rs-lock](https://github.com/qubit-ltd/rs-lock)
