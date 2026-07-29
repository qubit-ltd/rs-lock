# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

让并发组件依赖它需要的同步能力，再在集成边界选择具体后端。`qubit-lock` 让原生锁、
predicate 协调和确定性 timeout 测试都能采用这种方式。

## 为什么需要这个 crate

一个只在单个函数内使用的 `Mutex` 通常不需要额外抽象。价值出现在组件需要复用、
或者并发策略需要变化时：

- 可复用组件不应仅因获取 API 和 guard 类型不同，就为
  `std::sync::Mutex`、`std::sync::RwLock` 和 `parking_lot` 锁分别实现一次。
- 锁能保护状态，却不能定义“等待直到 predicate 成立”的协议。正确实现必须协调状态
  更新、predicate 检查、waiter 注册和 notification。
- 调用真实 sleep 的测试既慢又容易竞争。让生产等待算法运行在可控时钟上，timeout
  行为才容易验证。

`qubit-lock` 用能力 trait 解决第一个问题，用 monitor 实现解决后两个问题。

同步 adapter 支持 `std::sync::Mutex<T>`、`std::sync::RwLock<T>`，以及启用对应
Feature 后的 `parking_lot::Mutex<T>` 和 `parking_lot::RwLock<T>`。

## 何时不需要这个 crate

如果锁只服务于一个局部实现、后端不会变化，也没有条件等待或确定性 timeout 测试，
直接使用原生锁通常更简单。当公共或可复用边界需要后端无关的契约，或者等待者协调
已经成为领域行为的一部分时，再使用这个 crate。

## 一眼看到锁抽象的价值

下面的领域函数只知道如何读取或更新 `ServiceStats`。它不知道调用方选择的是低竞争
测试中的 mutex，还是读多写多服务中的读写锁。

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

启用 `parking-lot` Feature 后，同一组函数还可直接接收
`parking_lot::Mutex<ServiceStats>` 和 `parking_lot::RwLock<ServiceStats>`。
调用方决定竞争和依赖策略；组件始终只有一份业务实现。

| 不使用这个抽象 | 使用 `qubit-lock` |
| --- | --- |
| 组件签名绑定一种具体锁类型 | 组件声明 `DataLock<T>` |
| 领域代码在 `lock`、`read` 和 `write` 之间分支 | 领域代码统一使用 `with_read` 和 `with_write` |
| guard 与 poisoning 入口泄漏进组件 | 能力边界处理后端获取细节 |
| 替换后端会修改业务代码 | 调用方在集成边界选择后端 |

需要 guard 而不是数据闭包时使用 `Lock`。泛型算法确实要求独占进入时使用
`ExclusiveLock`；单独的 `Lock` 也可能表示读模式 adapter。`ReadWriteLock` 保留
共享与独占模式，并提供 `read_lock()` 和 `write_lock()` adapter。

## 锁还不够时

任务队列、就绪门和连接池不只需要互斥：worker 必须等待状态 predicate，producer
必须在不丢失 notification 的情况下更新状态，shutdown 必须唤醒所有受影响 waiter，
timeout 测试也不能依赖 sleep。

`ParkingLotMonitor` 和 `StdMonitor` 为阻塞代码提供相同的 predicate 导向领域边界；
`TokioMonitor` 提供异步对应实现。[英文用户手册](doc/user_guide.md) 会构建一个可关闭
的有界任务队列：同一份领域逻辑可运行在 `StdMonitor` 与 `ParkingLotMonitor` 上，
并用手动时钟测试真实 timeout 路径。[中文用户手册](doc/user_guide.zh_CN.md)提供相同案例。

## 选择能力

| 需求 | 首选组件 |
| --- | --- |
| 读取或修改锁内数据 | `DataLock<T>` |
| 抽象一种 guard 获取模式 | `Lock` |
| 要求真正独占的获取 | `ExclusiveLock` |
| 保留显式共享与独占模式 | `ReadWriteLock` |
| 协调阻塞式 predicate wait | `ParkingLotMonitor` 或 `StdMonitor` |
| 协调 Tokio predicate wait | `TokioMonitor` |
| 表达可复用 monitor 依赖 | 满足操作的最小能力 trait |
| 不使用 sleep 测试 deadline | `with_timer` 和 `ManualMonotonicClock` |

所有公开类型都从 crate root 导出。请直接从 crate root 导入公开类型。

## 安装与 Feature

默认特性集为空。只启用程序实际使用的组件：

| Feature | 启用的能力 |
| --- | --- |
| 不启用可选 Feature | 同步锁 trait 和 `std` 锁实现 |
| `parking-lot` | `parking_lot` mutex 和读写锁实现 |
| `monitor` | Monitor trait、`StdMonitor`、计时等待和 Timer 注入 |
| `async-lock` | Tokio 锁 trait 和 adapter |
| `async-monitor` | `async-lock`、monitor 支持和 `TokioMonitor` |
| 默认配置 | 不启用可选 Feature |

只使用同步锁 trait 和标准库实现：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false }
```

只使用 `StdMonitor`，无需 `parking_lot` 依赖：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor"] }
```

使用 `ParkingLotMonitor`：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor", "parking-lot"] }
```

启用 Tokio 锁但不启用 Tokio monitor：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["async-lock"] }
```

启用 Tokio monitor 和计时等待：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["async-monitor"] }
```

如果应用创建 Tokio runtime，应在应用自己的 `Cargo.toml` 中启用所需 runtime
Feature，例如 `rt` 或 `rt-multi-thread`。

## 条件等待语义

Monitor notification 是无记忆的：`notify_one` 最多选择一个已注册 waiter；没有
waiter 时发送的 notification 对未来没有作用。就绪状态应存入受保护数据；wakeup
只会要求 waiter 再次检查 predicate。

计时 monitor 等待与 `std::sync::Condvar::wait_timeout_while` 对齐。相对 timeout
是条件等待预算：取得状态锁后、首次 predicate 检查前，monitor 会采样一个固定
deadline。初始获取锁不计入预算，但 predicate 检查和等待会消耗预算。重新获取状态锁时
可能在 timeout 后返回。同步 `*_with_total_timeout` 会在初始获取状态锁前固定
deadline，因此锁竞争会消耗整个操作预算。两者都不是严格返回时限，因为重新获取锁和
执行 ready action 都不能被中断。

零时长、Timer 注册与完成错误、取消、外部 predicate 状态和 total-timeout 语义请参阅
用户手册。

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
