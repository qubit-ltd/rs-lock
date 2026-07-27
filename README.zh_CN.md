# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

## 解决的问题

Rust 应用经常混用 `std`、`parking_lot` 和 Tokio 锁。它们的具体 API 不同，因此即使
可复用代码只需要获取锁或访问受保护数据，也容易被绑定到某个后端。

条件协调还会带来另一个问题：锁本身不能表达“等待某个 predicate 成立”。正确的
条件变量代码必须让状态更新、predicate 检查、waiter 注册和 notification 遵循同一个
协议。如果超时测试依赖真实 sleep，它还会变慢且不稳定。

`qubit-lock` 提供后端无关的锁能力、基于闭包的数据访问、同步与异步 monitor，以及
支持确定性测试的可注入 Timer。

## 快速开始

`DataLock` 为受支持的 mutex 和读写锁提供相同的闭包式读写接口：

```rust
use qubit_lock::DataLock;

fn main() {
    let counter = std::sync::Mutex::new(0);
    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 1);
}
```

## 完整用户手册

[中文用户手册](doc/user_guide.zh_CN.md)详细介绍生产者—消费者引导示例、所有公开
组件、Feature 选择、monitor 语义、计时等待、确定性测试和常见错误。

[英文用户手册](doc/user_guide.md)提供相同内容的英文版本。

## 特性

- `Lock`、`ExclusiveLock`、`ReadWriteLock` 和 `DataLock<T>` 为
  `std::sync::Mutex<T>`、`std::sync::RwLock<T>`、
  `parking_lot::Mutex<T>` 和 `parking_lot::RwLock<T>` 提供统一同步能力。
- `AsyncLock`、`AsyncReadWriteLock` 和 `AsyncDataLock<T>` 由
  `async-lock` 启用，并提供对应的 Tokio 能力。
- `ParkingLotMonitor`、`StdMonitor` 和对应的 `Arc*` 句柄提供阻塞式 predicate
  协调。
- `TokioMonitor` 和 `ArcTokioMonitor` 由 `async-monitor` 启用，并提供异步协调。
- 每个具体 monitor 都支持注入 Timer，以便进行确定性测试。

所有公开类型都直接从 crate root 导入。

## 安装

默认特性集启用 `monitor` 和 `parking-lot`：

```toml
[dependencies]
qubit-lock = "0.11"
```

只使用同步锁 trait 和标准库实现：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false }
```

启用 Tokio 锁但不启用 Tokio monitor：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false, features = ["async-lock"] }
```

启用 Tokio monitor 和计时等待：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false, features = ["async-monitor"] }
```

如果应用创建 Tokio runtime，应在应用自己的 `Cargo.toml` 中启用所需的 runtime
Feature。

## 条件等待语义

计时 monitor 等待与 `std::sync::Condvar::wait_timeout_while` 对齐。timeout 是条件
等待预算：取得状态锁后、首次 predicate 检查前，monitor 会采样一个固定 deadline。
初始获取锁不计入预算，predicate 检查会消耗预算，并且重新获取状态锁时可能在 timeout
后返回。零时长、错误、取消和整个调用 deadline 的语义请参阅
[英文用户手册](doc/user_guide.md) 或 [中文用户手册](doc/user_guide.zh_CN.md)。

## 项目结构

- `src/lock`：锁 trait 与原生锁 adapter。
- `src/monitor`：monitor trait，以及 parking_lot、标准库和 Tokio 实现。
- `doc`：中英文用户手册。
- `tests/lock`：锁行为测试。
- `tests/monitor`：monitor 行为测试。
- `tests/docs`：公开文档一致性测试。

## 相关项目

Qubit 旗下的更多 Rust 库发布在 GitHub 组织
[qubit-ltd](https://github.com/qubit-ltd)。

## 测试

```bash
# 使用默认 Feature 集运行测试
cargo test

# 使用项目声明的全部 Feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-lock](https://github.com/qubit-ltd/rs-lock)
