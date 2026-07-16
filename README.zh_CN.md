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
- `ArcAsyncMutex`、`ArcAsyncRwLock`：由可选 `async` 特性启用的 Tokio
  异步锁包装器。
- `ParkingLotMonitor`、`ArcParkingLotMonitor`、`ParkingLotMonitorGuard`：基于 parking_lot 的条件变量协调工具。
- `StdMonitor`、`ArcStdMonitor`、`StdMonitorGuard`：基于标准库的条件变量协调工具。
- `MockMonitor`、`ArcMockMonitor`：由可选 `mock` 特性启用、共享的
  `qubit_clock::ManualMonotonicClock` 驱动的确定性测试 monitor。
- `TokioMonitor`、`ArcTokioMonitor`：由可选 `async` 特性启用的 Tokio
  异步 monitor 协调工具。
- 基于闭包的访问接口，让加锁和释放始终局限在一次调用内部。
- `Arc*` 包装器实现了 `Deref` 和 `AsRef`，需要时仍可使用底层同步原语的
  guard 风格原生接口。

## 安装

```toml
[dependencies]
qubit-lock = "0.10"
```

默认特性集只包含同步锁与同步 monitor。需要异步能力或确定性测试能力时，显式启用对应特性：

```toml
[dependencies]
qubit-lock = { version = "0.10", features = ["async", "mock"] }
```

如果应用需要创建 Tokio runtime，请在应用自己的 `Cargo.toml` 中启用合适的 Tokio runtime 特性，例如 `rt` 或 `rt-multi-thread`。
`AsyncLock` 返回 `Send` future：`ArcAsyncMutex<T>` 在 `T: Send` 时实现它，
`ArcAsyncRwLock<T>` 在 `T: Send + Sync` 时实现它。

## Monitor 语义

monitor notification 使用无记忆的条件变量语义。`notify_one` 最多选择一个已经注册的
waiter；没有已注册 waiter 时发出的 notification 对未来没有影响。唤醒只会触发下一次
受保护的 predicate 检查，既不会让 predicate 自动变为 true，也不保证公平性。

相对 timeout 是条件等待预算。初始状态锁竞争和初始 predicate 检查不计入预算。初始
检查确认必须等待后，monitor 会在首次条件等待挂起前立即建立同一个固定 deadline，并
在后续唤醒中复用。零 timeout 仍会检查 predicate，最后一次持锁 predicate 检查优先于
timeout。

异步 monitor trait 返回 `impl Future`；返回的 future 是惰性的，所以构造 future 和首次
poll 之前的时间不消耗 timeout 预算。只有带 timeout 的 wait 实际进入非零计时挂起时，
Tokio runtime 才必须启用 time driver。drop 一个 pending future 会注销其活跃 waiter，
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

`MockMonitor` 和 `ArcMockMonitor` 是用于能力 trait 与 predicate wait 行为的
确定性测试实现。它们不提供 mock guard 类型，也不替代具有 guard 接口的具体
monitor 实现。

### 确定性的 monitor 时间

使用 `MockMonitor` 和 `ArcMockMonitor` 前需要启用 `mock` 特性。
`MockMonitor::new` 会创建一个独立的 `ManualMonotonicClock`，测试通过
`monotonic_clock()` 显式推进它。如果多个测试组件需要处于同一个时间域，使用同一个
clock 调用 `MockMonitor::from_clock` 或 `ArcMockMonitor::from_clock` 构造。
直接构造共享 clock 的代码还需要显式声明直接依赖 `qubit-clock = "0.9"`：

```rust
use std::{sync::Arc, time::Duration};

use qubit_clock::ManualMonotonicClock;
use qubit_lock::ArcMockMonitor;

let clock = Arc::new(ManualMonotonicClock::new());
let monitor = ArcMockMonitor::from_clock(false, Arc::clone(&clock));

clock.advance(Duration::from_secs(10)).unwrap();
assert_eq!(monitor.elapsed(), Duration::from_secs(10));
```

推进 clock 会唤醒阻塞和异步 timeout waiter，不会产生真实时间等待。阻塞测试可在推进
mock time 前调用 `wait_for_timeout_waiters(expected_count, real_timeout)`，不再用真实
sleep 猜测 waiter 是否已经注册。`pending_timeout_waiters()` 汇总已经能够观察变化的
同步和异步 timeout wait；异步 wait 的 future 首次被 poll 后才计数，被取消时会自动
注销。代码也可以在持有该 monitor 状态锁时推进 clock；clock callback 不会再次获取
受保护的状态。

## 快速开始

### 同步锁

```rust
use qubit_lock::{ArcMutex, Lock};

fn main() {
    let counter = ArcMutex::new(0);
    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 1);
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

    counter.with_write(|value| *value += 1);
    assert_eq!(counter.with_read(|value| *value), 2);
}
```

`with_read` 和 `with_write` 将闭包式访问与原生 guard 获取明确区分开。
因此，读写锁包装器可直接通过 `lock.read()` 或 `lock.write()` 获取原生 guard；
需要显式指定底层类型时仍可使用 `lock.as_ref()`。

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
