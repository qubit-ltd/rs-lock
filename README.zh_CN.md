# Qubit Lock

[![Rust CI](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-lock/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-lock/coverage-badge.json)](https://qubit-ltd.github.io/rs-lock/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-lock.svg?color=blue)](https://crates.io/crates/qubit-lock)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

面向 Qubit Rust 库的锁工具 crate。它提供通用锁能力与基于条件变量的 monitor 协调能力。

## 特性

- `Lock`：与数据无关、返回 RAII guard 的独占锁能力，由
  `std::sync::Mutex<T>` 和 `parking_lot::Mutex<T>` 实现。
- `ReadWriteLock`：与数据无关的共享/独占锁能力，由
  `std::sync::RwLock<T>` 和 `parking_lot::RwLock<T>` 实现。
- `DataLock<T>`：以闭包访问受支持 mutex 或读写锁所保护的数据。
- `AsyncLock`、`AsyncReadWriteLock` 和 `AsyncDataLock<T>`：由可选
  `async-lock` 特性启用的对应 Tokio 锁能力。
- `ParkingLotMonitor`、`ArcParkingLotMonitor`、`ParkingLotMonitorGuard`：基于 parking_lot 的条件变量协调工具。
- `StdMonitor`、`ArcStdMonitor`、`StdMonitorGuard`：基于标准库的条件变量协调工具。
- `TokioMonitor`、`ArcTokioMonitor`：由可选 `async-monitor` 特性启用的
  Tokio 异步 monitor 协调工具。
- 所有 monitor 都支持注入 Timer，使集成测试直接运行生产等待算法。
- 直接支持借用和 `Arc` 持有的锁，无需额外包装类型。

## 安装

```toml
[dependencies]
qubit-lock = "0.11"
```

默认特性集启用 `monitor` 和 `parking-lot`，保留完整同步 API。只需要基础锁
trait 的用户可以关闭全部默认特性，从依赖图中移除这两个可选依赖：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false }
```

只需要异步锁、但不需要 Tokio monitor deadline 时显式启用：

```toml
[dependencies]
qubit-lock = { version = "0.11", features = ["async-lock"] }
```

需要 Tokio monitor 协调和计时等待时显式启用：

```toml
[dependencies]
qubit-lock = { version = "0.11", features = ["async-monitor"] }
```

历史 `async` 兼容别名会启用 `async-monitor`。如果应用需要创建 Tokio runtime，
请在应用自己的 `Cargo.toml` 中启用合适的 Tokio runtime 特性，例如 `rt` 或
`rt-multi-thread`。
`AsyncLock` 和 `AsyncReadWriteLock` 返回 `Send` future。Tokio mutex 在
`T: Send` 时实现前者；Tokio 读写锁在 `T: Send + Sync` 时实现后者。

## Monitor 语义

monitor notification 使用无记忆的条件变量语义。`notify_one` 最多选择一个已经注册的
waiter；没有已注册 waiter 时发出的 notification 对未来没有影响。唤醒只会触发下一次
受保护的 predicate 检查，既不会让 predicate 自动变为 true，也不保证公平性。

相对 timeout 是条件等待预算。初始状态锁竞争和初始 predicate 检查不计入预算。初始
检查确认必须等待后，monitor 会在首次条件等待挂起前立即建立同一个固定 deadline，并
在后续唤醒中复用。零 timeout 仍会检查 predicate，最后一次持锁 predicate 检查优先于
timeout。

异步 monitor trait 返回 `impl Future`；返回的 future 是惰性的，所以构造 future 和首次
poll 之前的时间不消耗 timeout 预算。默认 Tokio Timer 在非零计时等待真正挂起时要求
runtime 启用 time driver；注入其他 Timer 时由该 Timer 决定驱动要求。drop 一个 pending future 会注销其活跃 waiter，
不会执行 action，也不会回滚受保护状态的变化。如果 `notify_one` 已选择该 waiter，
取消会丢弃该次选择，不会转交给其他或未来 waiter。

基于 Arc 的 monitor 包装器保留了供泛型代码使用的显式 trait 实现；普通 monitor 方法
调用通过 `Deref` 解析。`from_arc`、`as_arc` 和 `into_arc` 明确表达共享所有权边界。

### 选择 monitor 能力

普通应用代码应优先选择具体实现：阻塞式协调使用 `ParkingLotMonitor` 或
`StdMonitor`，异步协调使用 `TokioMonitor`；需要克隆或长期持有共享所有权时，
选择对应的 `Arc*Monitor` 句柄。

在泛型 API 边界，使用能够表达操作的最小能力：仅发送通知时使用 `Notifier`，
阻塞式 predicate wait 使用 `ConditionWaiter` 或 `TimeoutConditionWaiter`，异步
wait 使用对应的 `AsyncConditionWaiter` 或 `AsyncTimeoutConditionWaiter`。只有在
确实需要完整的通知与等待契约时才使用 `Monitor` 或 `AsyncMonitor`；泛型 API
还要持有可克隆句柄时，使用 `SharedMonitor` 或 `SharedAsyncMonitor`。这些 waiter
和聚合 trait 用于静态泛型约束，不用于 `dyn` trait object 接口。

所有公开类型都直接从 crate root 导入。

### 确定性的 monitor 时间

每个具体 monitor 都提供 `with_timer`。集成测试把 `ManualTimer` 注入生产所用的
`ParkingLotMonitor`、`StdMonitor` 或 `TokioMonitor`，不再维护另一套 mock 等待算法。
构造手动 clock 的代码需要直接依赖 `qubit-clock = "0.9"`：

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

`ManualMonotonicClock` 是测试控制面。测试通过 waiter/deadline 观察接口协调推进，
无需用真实 sleep 猜测注册时机。Monitor 和 Timer 注册都支持安全取消；多个组件也可
共享同一个手动时间域。

带超时的 predicate API 返回 `Result<WaitTimeoutResult<_>, TimeError>`，Timer 注册错误
不会伪装成超时。Guard 使用原地更新的 `wait`、`wait_for` 和 `wait_until`；Timer 出错时
guard 仍然持有且可继续使用。

## 快速开始

### 绑定数据的锁

```rust
use qubit_lock::DataLock;

fn main() {
    let counter = parking_lot::Mutex::new(0);
    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 1);
}
```

### 与数据无关的锁

当受保护状态位于锁外部（例如 atomic）时使用 `Lock`。guard 离开作用域时自动解锁。

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

`std::sync::Mutex<T>`、`std::sync::RwLock<T>`、`parking_lot::Mutex<T>` 和
`parking_lot::RwLock<T>` 都实现 `DataLock<T>`。读写锁实现 `ReadWriteLock`；
可用 `read_lock()` 或 `write_lock()` 将其中一侧适配为独占 `Lock` 能力。

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

## 项目结构

- `src/lock`：锁 trait 与原生锁适配器。
- `src/monitor`：monitor traits，以及 parking_lot、标准库和 Tokio monitor 实现。
- `tests/lock`：锁相关行为测试。
- `tests/monitor`：monitor 相关行为测试。
- `tests/docs`：README 与文档文本一致性测试。

## 相关项目

Qubit 旗下的更多 Rust 库发布在 GitHub 组织 [qubit-ltd](https://github.com/qubit-ltd)。

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
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
