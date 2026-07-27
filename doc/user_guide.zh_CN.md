# Qubit Lock 用户手册

[English](user_guide.md)

`qubit-lock` 为同步锁、Tokio 锁和 monitor 风格的条件协调提供统一抽象。本手册先说明
这个 crate 解决的问题，再详细介绍每个公开组件的选择与用法。

## 1. 这个库解决什么问题

Rust 已经提供了优秀的锁实现，但应用和库代码仍会反复遇到三个问题：

1. `std`、`parking_lot` 和 Tokio 暴露的具体 API 不同。只需要“获取锁”或“读取受保护
   数据”的泛型代码，不应该被迫了解调用方选择了哪个后端。
2. 锁可以保护状态，却不能独立表达“休眠直到某个 predicate 成立”。如果状态更新、
   predicate 检查和 waiter 注册没有遵循同一个协议，手写的条件变量代码可能丢失通知。
3. 真实 sleep 会让超时测试变慢且不稳定。测试应运行生产环境的等待算法，同时能够
   确定性地控制时间。

`qubit-lock` 通过后端无关的锁 trait、基于闭包的数据访问、同步与异步 monitor 以及
可注入 Timer 解决这些问题。

### 引导示例：单任务工作队列

队列为空时 worker 必须休眠；队列非空后，它必须在持有同一把锁时取出任务。producer
必须更新队列并通知已注册 waiter，同时不能留下丢失通知的窗口：

```rust
use qubit_lock::ArcParkingLotMonitor;

fn main() {
    let queue = ArcParkingLotMonitor::new(Vec::<i32>::new());
    let worker_queue = queue.clone();

    let worker = std::thread::spawn(move || {
        worker_queue.wait_until(
            |items| !items.is_empty(),
            |items| items.remove(0),
        )
    });

    queue.with_write_notify_one(|items| items.push(7));

    assert_eq!(worker.join().expect("worker should finish"), 7);
}
```

`with_write_notify_one` 完成状态更新并通知的握手。`wait_until` 在 monitor 锁内检查
predicate，必要时注册 waiter，并在唤醒后重新检查。队列状态才是事实来源；
notification 只是再次检查状态的提示。

## 2. 安装与 Feature 选择

默认配置提供完整的同步 API：

```toml
[dependencies]
qubit-lock = "0.11"
```

根据所需组件选择 Feature：

| Feature | 启用的能力 |
| --- | --- |
| 不启用可选 Feature | 同步锁 trait 和 `std` 锁实现 |
| `parking-lot` | parking_lot mutex 和读写锁实现 |
| `monitor` | monitor trait、标准库 monitor、计时等待和 Timer 注入 |
| `async-lock` | Tokio 锁 trait 和适配器 |
| `async-monitor` | `async-lock`、monitor 支持和 Tokio monitor |
| 默认配置 | `monitor` 和 `parking-lot` |

只使用锁的用户可以避免所有可选依赖：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false }
```

只启用异步锁，不启用 Tokio monitor deadline：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false, features = ["async-lock"] }
```

启用 Tokio monitor 协调和计时等待：

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false, features = ["async-monitor"] }
```

如果应用创建 Tokio runtime，应在应用自己的 `Cargo.toml` 中启用所需的 runtime
Feature，例如 `rt` 或 `rt-multi-thread`。

所有 `qubit-lock` 公开类型都从 crate root 导入。

## 3. 同步锁组件

### `DataLock<T>`

当数据存储在锁中，并且操作可以用闭包表达时，使用 `DataLock<T>`：

- `with_read` 向闭包提供 `&T`。
- `with_write` 向闭包提供 `&mut T`。
- `try_with_read` 和 `try_with_write` 立即返回
  `Result<_, TryLockError>`。

`std::sync::Mutex<T>`、`std::sync::RwLock<T>` 以及启用 `parking-lot` 后对应的
parking_lot 类型都实现了该 trait。对于 mutex，读写操作获取的是同一把独占锁；
对于读写锁，`with_read` 允许多个 reader 并发。

```rust
use std::sync::RwLock;

use qubit_lock::DataLock;

fn main() {
    let values = RwLock::new(vec![1, 2, 3]);
    values.with_write(|items| items.push(4));

    let sum = values.with_read(|items| items.iter().sum::<i32>());
    assert_eq!(sum, 10);

    let length = values
        .try_with_read(|items| items.len())
        .expect("the lock should be available");
    assert_eq!(length, 4);
}
```

闭包应保持简短；锁会一直持有到闭包返回。标准库实现遇到 poisoned lock 时会在阻塞式
获取中 panic；`try_*` 方法则返回 `TryLockError::Poisoned`。
所有回调 panic 都会向上传播。对于标准库锁，它还可能使锁中毒；parking_lot 锁不会中毒。

### `Lock` 与 `ExclusiveLock`

当锁和受保护状态相互分离，或泛型代码只需要一种获取模式时，使用 `Lock`。`lock`
返回 RAII guard，`try_lock` 执行非阻塞尝试。

`Lock` 不承诺其获取模式一定排除所有其他 guard，因为读模式适配器也实现了它。
当泛型算法确实需要独占进入时，增加标记 trait `ExclusiveLock`。

```rust
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use qubit_lock::ExclusiveLock;

fn increment<L>(gate: &L, counter: &AtomicUsize)
where
    L: ExclusiveLock + ?Sized,
{
    let _guard = gate.lock();
    counter.fetch_add(1, Ordering::Relaxed);
}

fn main() {
    let gate = Mutex::new(());
    let counter = AtomicUsize::new(0);
    increment(&gate, &counter);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}
```

`std::sync::Mutex`、`parking_lot::Mutex` 和 write-mode adapter 实现
`ExclusiveLock`；read-mode adapter 刻意不实现它。

### `ReadWriteLock`、`ReadLock` 与 `WriteLock`

`ReadWriteLock` 表示显式区分共享和独占模式的后端：

- `read` 和 `write` 返回后端 guard。
- `try_read` 和 `try_write` 返回 `TryLockError`，不会阻塞。
- `read_lock()` 返回借用型 `ReadLock`。
- `write_lock()` 返回借用型 `WriteLock`。

这些 adapter 让基于 `Lock` 编写的 API 能够使用读写锁的一侧。`ReadLock` 实现
`Lock`；`WriteLock` 同时实现 `Lock` 和 `ExclusiveLock`。

```rust
use std::sync::RwLock;

use qubit_lock::{Lock, ReadWriteLock};

fn main() {
    let values = RwLock::new(vec![1, 2]);

    let read_mode = values.read_lock();
    assert_eq!(Lock::lock(&read_mode).len(), 2);

    let write_mode = values.write_lock();
    Lock::lock(&write_mode).push(3);

    assert_eq!(&*values.read().expect("lock should not be poisoned"), &[1, 2, 3]);
}
```

### `TryLockError`

所有非阻塞锁 API 都使用与后端无关的 `TryLockError`：

- `WouldBlock` 表示当前已有 guard 阻止锁获取。
- `Poisoned` 表示标准库锁曾因 panic 而中毒。

parking_lot 和 Tokio 锁没有 poisoning，因此它们只会报告竞争。

## 4. 异步锁组件

本节类型需要 `async-lock` Feature。

### `AsyncDataLock<T>`

`AsyncDataLock<T>` 是 `DataLock<T>` 的异步版本。`with_read` 和 `with_write`
不会阻塞 executor 线程；它们等待获取锁，然后在持有 guard 时运行同步闭包。
`try_*` 方法不会等待。

```rust
use qubit_lock::AsyncDataLock;

#[tokio::main]
async fn main() {
    let values = tokio::sync::RwLock::new(vec![1, 2, 3]);

    values.with_write(|items| items.push(4)).await;
    let sum = values.with_read(|items| items.iter().sum::<i32>()).await;

    assert_eq!(sum, 10);
}
```

不要在闭包中执行阻塞 I/O，也不能在闭包中 await 另一个 future。闭包本身是同步的，
锁会一直持有到闭包返回。
所有回调 panic 都会向上传播；Tokio 锁不会中毒。

### `AsyncLock`、`AsyncReadWriteLock`、`AsyncReadLock` 与 `AsyncWriteLock`

`AsyncLock` 提供异步 `lock` 和立即返回的 `try_lock`。
`AsyncReadWriteLock` 提供 `read`、`write`、`try_read`、`try_write`，以及
`read_lock()` 和 `write_lock()` adapter。`AsyncReadLock` 表示共享侧，
`AsyncWriteLock` 表示独占侧。

```rust
use qubit_lock::{AsyncLock, AsyncReadWriteLock};

#[tokio::main]
async fn main() {
    let values = tokio::sync::RwLock::new(vec![1, 2]);

    let write_mode = values.write_lock();
    AsyncLock::lock(&write_mode).await.push(3);

    let read_mode = values.read_lock();
    assert_eq!(AsyncLock::lock(&read_mode).await.len(), 3);
}
```

`AsyncLock` 和 `AsyncReadWriteLock` 返回 `Send` future。Tokio mutex 在
`T: Send` 时实现 `AsyncLock`；Tokio 读写锁在 `T: Send + Sync` 时实现
`AsyncReadWriteLock`。

## 5. Monitor 能力组件

monitor 持有受保护状态，并使用 notification 协调 predicate wait。应用代码通常应
选择具体 monitor；在泛型 API 边界使用能力 trait：

| 组件 | 能力 |
| --- | --- |
| `Notifier` | 只提供 `notify_one` 和 `notify_all` |
| `ConditionWaiter` | 同步 `wait_until`、`wait_until_ready` 和 `wait_while` |
| `TimeoutConditionWaiter` | 同步 `wait_until_for`、`wait_until_ready_for` 和 `wait_while_for` |
| `Monitor` | 状态访问、notification 和无时限同步等待 |
| `TimedMonitor` | `Monitor` 加同步计时等待 |
| `SharedMonitor` | 可克隆的同步共享 monitor 句柄 |
| `AsyncConditionWaiter` | 异步 `wait_until_async`、无 action 的 `wait_until_ready_async` 和 `wait_while_async` |
| `AsyncTimeoutConditionWaiter` | 异步 `wait_until_for_async`、无 action 的 `wait_until_ready_for_async` 和 `wait_while_for_async` |
| `AsyncMonitor` | 异步状态访问、notification 和无时限等待 |
| `AsyncTimedMonitor` | `AsyncMonitor` 加计时等待 |
| `SharedAsyncMonitor` | 可克隆的异步共享 monitor 句柄 |

例如，一个只更新状态并唤醒一个 waiter 的泛型 producer 只需要 `Monitor`，不需要
完整的计时 monitor 能力：

```rust
use qubit_lock::Monitor;

fn publish<M>(monitor: &M, value: i32)
where
    M: Monitor<State = Vec<i32>> + ?Sized,
{
    monitor.with_write_notify_one(|items| items.push(value));
}
```

这些 trait 使用返回位置的 `impl Future` 和泛型方法。它们用于静态泛型约束，不用于
`dyn` trait-object 接口。

## 6. 具体 Monitor 组件

### `ParkingLotMonitor<T>` 与 `ArcParkingLotMonitor<T>`

启用 `parking-lot` 和 `monitor` Feature 后，需要高效阻塞式协调时使用
`ParkingLotMonitor<T>`。需要在线程间克隆或长期持有句柄时，使用
`ArcParkingLotMonitor<T>`。

重要方法包括：

- `new` 和 `with_timer`：构造 monitor。
- `with_read` 和 `with_write`：访问状态。
- `with_write_notify_one` 和 `with_write_notify_all`：执行常规的状态更新与通知协议。
- `wait_until` / `wait_while`、无 action 的 `wait_until_ready` 及对应的 `_for` 计时版本。
- `lock`：需要显式控制 guard 时使用。

`Arc*` 包装器通过 `Deref` 访问内部 monitor。`from_arc`、`as_arc` 和 `into_arc`
显式表达所有权边界，不会再分配一层。

### `StdMonitor<T>` 与 `ArcStdMonitor<T>`

`StdMonitor<T>` 使用标准库原语，并提供相同的高层 API。当避免 parking_lot 依赖比
使用该后端更重要时选择它。`ArcStdMonitor<T>` 是其可克隆的共享句柄，同样提供
`from_arc`、`as_arc` 和 `into_arc`。

与标准库的 `Lock` 和 `DataLock` 适配器不同，`StdMonitor` 在 poisoning 后会恢复
并继续提供内部状态，而不是拒绝访问。线程持有状态锁时发生 panic，可能使状态只完成
部分修改。`is_poisoned` 用于检查是否发生过这种情况；普通的 `lock`、`with_read`、
`with_write` 和等待操作仍可使用，但不会自动清除 poisoning 标记。调用者应先检查状态，
必要时修复受保护的不变量，再调用 `clear_poison` 明确接受恢复后的状态。
`clear_poison` 只清除标记，不会验证或回滚状态；以后再次在持锁期间 panic，monitor
仍会重新进入 poisoned 状态。`ArcStdMonitor` 通过内部 monitor 同样公开这两个方法。

```rust
use qubit_lock::ArcStdMonitor;

fn main() {
    let monitor = ArcStdMonitor::new(false);
    let waiter_monitor = monitor.clone();

    let waiter = std::thread::spawn(move || {
        waiter_monitor.wait_until(|ready| *ready, |_| "ready")
    });

    monitor.with_write_notify_all(|ready| *ready = true);
    assert_eq!(waiter.join().expect("waiter should finish"), "ready");
}
```

### `ParkingLotMonitorGuard` 与 `StdMonitorGuard`

`ParkingLotMonitor::lock` 返回 `ParkingLotMonitorGuard`；
`StdMonitor::lock` 返回 `StdMonitorGuard`。两者都可解引用为状态，并支持：

- `wait`：释放状态锁、等待并重新获取。
- `wait_for` 和 `wait_until`：原地更新 guard。
- 消费 guard 的 `notify_one` 和 `notify_all`：先释放 guard 再发送通知。

优先使用 monitor 上的 predicate helper。当算法需要执行多次状态转换并显式控制锁时，
再使用 guard：

```rust
use qubit_lock::ParkingLotMonitor;

fn main() {
    let monitor = ParkingLotMonitor::new(Vec::<i32>::new());
    let mut guard = monitor.lock();
    guard.push(7);
    guard.notify_one();

    assert_eq!(monitor.with_read(|items| items.clone()), vec![7]);
}
```

### `TokioMonitor<T>` 与 `ArcTokioMonitor<T>`

这些类型需要 `async-monitor`。任务局部持有时使用 `TokioMonitor<T>`；需要可克隆
共享所有权时使用 `ArcTokioMonitor<T>`。

- `current` 为默认 Timer 捕获当前 Tokio runtime Handle。
- `try_current` 在缺少当前 runtime 时返回错误，而不是 panic。
- `with_timer` 注入显式 Timer。
- `with_read_async`、`with_write_async` 和组合式
  `with_write_notify_*_async` 方法访问状态。
- `wait_until_async` / `wait_while_async` 及对应的 `_for_async` 版本等待 predicate；
  `wait_until_ready_async` 和 `wait_until_ready_for_async` 提供无 action 的形式。

```rust
use qubit_lock::{ArcTokioMonitor, AsyncConditionWaiter};

#[tokio::main]
async fn main() {
    let monitor = ArcTokioMonitor::current(Vec::<i32>::new());
    let worker_monitor = monitor.clone();

    let worker = tokio::spawn(async move {
        worker_monitor
            .wait_until_async(
                |items| !items.is_empty(),
                |items| items.remove(0),
            )
            .await
    });

    monitor
        .with_write_notify_one_async(|items| items.push(7))
        .await;

    assert_eq!(worker.await.expect("worker should finish"), 7);
}
```

捕获的目标 runtime 必须保持存活、启用 time driver，并持续运行到计时等待完成。
计时 future 可以在另一个 runtime context 中 poll，但 Timer 仍属于捕获或注入的
runtime。

## 7. 等待、通知与超时语义

### Notification 是无记忆的

`notify_one` 最多选择一个已经注册的 waiter。没有 waiter 注册时发送的 notification
对未来没有影响。`notify_all` 影响当前已经注册的 waiter。两者都不会使 predicate
自动变为 true，也不保证公平性。

predicate 和回调都在持有 monitor 状态锁时执行；它们的 panic 会向上传播。
传给 `with_write_notify_*` 的回调如果 panic，monitor 不会发送 notification。

包括虚假唤醒在内的 wakeup 只会触发下一次持锁 predicate 检查。应把就绪状态存入
受保护数据，并让 predicate 检查它。

### 外部 predicate 状态也需要同一个握手

如果 predicate 读取 monitor 外的状态，例如 atomic，可能使其就绪的更新仍必须参与
monitor-lock handshake。仅靠 atomic ordering 无法阻止 notification 落在 predicate
检查和 waiter 注册之间：

```rust
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use qubit_lock::ArcStdMonitor;

fn main() {
    let ready = Arc::new(AtomicBool::new(false));
    let monitor = ArcStdMonitor::new(());
    let waiter_ready = Arc::clone(&ready);
    let waiter_monitor = monitor.clone();

    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until_ready(|_| {
            waiter_ready.load(Ordering::Acquire)
        });
    });

    monitor.with_write_notify_all(|_| {
        ready.store(true, Ordering::Release);
    });

    waiter.join().expect("waiter should finish");
}
```

异步协议相同；应使用组合式异步 helper，避免更新跨过 waiter 注册：

```rust
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use qubit_lock::{ArcTokioMonitor, AsyncConditionWaiter};

#[tokio::main]
async fn main() {
    let ready = Arc::new(AtomicBool::new(false));
    let monitor = ArcTokioMonitor::current(());
    let waiter_ready = Arc::clone(&ready);
    let waiter_monitor = monitor.clone();

    let waiter = tokio::spawn(async move {
        waiter_monitor
            .wait_until_ready_async(|_| {
                waiter_ready.load(Ordering::Acquire)
            })
            .await;
    });

    monitor
        .with_write_notify_all_async(|_| {
            ready.store(true, Ordering::Release);
        })
        .await;

    waiter.await.expect("waiter should finish");
}
```

### Timeout 预算

相对 timeout 是条件等待预算，与 `std::sync::Condvar::wait_timeout_while`
对齐。它不是整个方法调用的硬性 deadline：初始状态锁竞争不计入预算，predicate
就绪之后执行的 action 也不计入预算。

取得状态锁后、首次 predicate 检查前，monitor 会采样起始时间并推导出一个固定的
绝对 deadline。第一次和之后的每次 predicate 检查、waiter 注册以及全部等待时间都会
消耗这份预算；notification 与虚假唤醒都不会重新开始计时。和条件变量一样，重新获取
状态锁时可能在 timeout 后返回。

零时长 timeout 仍会执行初始 predicate 检查。到达 deadline 后，最后一次持锁
predicate 检查优先于成功的 Timer 完成。Timer 注册或完成错误优先于等待后的就绪结果，
并且不会执行 action。如果应用需要整个调用的 deadline，必须在业务操作入口自行采样
绝对 deadline，并将每次等待的剩余预算传入。

异步等待 future 是惰性的：首次 poll 前的时间不消耗预算，初始异步状态锁竞争也不
计入预算。drop pending future 会注销其 waiter，也不会执行 action。取消不会回滚受保护状态的变化，
包括其他任务已经完成的修改。如果 `notify_one` 已经选择该 waiter，
取消会丢弃这次选择，不会转交给其他或未来 waiter。

## 8. 等待结果与错误

基于 predicate 的计时等待返回：

```text
Result<WaitTimeoutResult<R>, qubit_clock::TimeError>
```

`WaitTimeoutResult::Ready(R)` 包含 action 结果。
`WaitTimeoutResult::TimedOut` 表示最终持锁检查后 predicate 仍为 false。

guard 级计时等待返回 `WaitTimeoutStatus`：

- `Woken` 表示等待在 deadline 前返回，原因可能是 notification 或虚假唤醒。
- `TimedOut` 表示到达 deadline。

使用 guard 的调用方在收到任一状态后都必须继续检查受保护状态。`TimeError` 表示
Timer 注册或完成失败，不是真实超时。发生该错误后，guard 仍处于持锁且可用状态。

`WaitTimeoutResult` 提供 `is_ready`、`is_timed_out`、`into_option` 和 `map`；
`WaitTimeoutStatus` 提供 `is_woken` 和 `is_timed_out`。

## 9. 测试中的确定性时间

每个具体 monitor 都提供 `with_timer`。测试可注入 `qubit-clock` 的 `ManualTimer`，
使生产环境使用的 monitor 类型和等待算法在没有真实 sleep 的情况下直接运行。

直接声明测试时钟依赖：

```toml
[dev-dependencies]
qubit-clock = { version = "0.10", features = ["test-util"] }
```

```rust
use std::{sync::Arc, thread, time::Duration};

use qubit_clock::{ManualMonotonicClock, MonotonicClock};
use qubit_lock::{ParkingLotMonitor, WaitTimeoutResult};

fn main() {
    let clock = ManualMonotonicClock::new_shared();
    let monitor = Arc::new(ParkingLotMonitor::with_timer(
        false,
        clock.new_timer(),
    ));
    let waiter_monitor = Arc::clone(&monitor);

    let waiter = thread::spawn(move || {
        waiter_monitor.wait_until_ready_for(
            Duration::from_secs(16),
            |ready| *ready,
        )
    });

    let _ = clock.advance_to_next_deadline_after_waiters(
        1,
        Duration::from_secs(1),
    );

    assert!(matches!(
        waiter.join().expect("waiter should finish"),
        Ok(WaitTimeoutResult::TimedOut),
    ));
}
```

clock 的 waiter 和 deadline 观察接口会在注册后协调推进时间，不需要用真实 sleep
猜测。Monitor 和 Timer 注册都支持安全取消，多个组件也可以共享同一个手动时间域。
Tokio monitor 使用相同的注入设计。

## 10. 组件选择与常见错误

### 选择指南

| 需求 | 首选组件 |
| --- | --- |
| 抽象一种获取模式 | `Lock` |
| 要求真正的独占获取 | `ExclusiveLock` |
| 读取或修改锁内数据 | `DataLock<T>` |
| 保留共享与独占模式 | `ReadWriteLock` |
| 使用 Tokio 锁 | 对应的 `Async*` 组件 |
| 协调阻塞式 predicate wait | `ParkingLotMonitor` 或 `StdMonitor` |
| 协调 Tokio predicate wait | `TokioMonitor` |
| 克隆 monitor 句柄 | 对应的 `Arc*Monitor` |
| 不使用 sleep 测试 deadline | `with_timer` 和 `ManualMonotonicClock` |
| 表达泛型 monitor 依赖 | 能满足操作的最小能力 trait |

### 常见错误

- 把 notification 当作已存储状态。应把就绪条件存入受保护状态。
- 在 monitor-lock handshake 外修改 predicate 状态。
- 能使用组合式 `with_write_notify_*` helper 时，却把更新和 raw `notify_*` 分开。
- 持锁执行耗时操作、阻塞 I/O 或无关回调。
- 在 monitor 闭包中重入同一个 monitor；这可能造成死锁。
- 假设 `notify_one` 公平，或假设取消会转交它选中的 notification。
- 使用 gated 组件时遗漏 `async-lock`、`monitor`、`parking-lot` 或
  `async-monitor`。
- `Notifier` 或 waiter trait 已经足够时，仍要求宽泛的聚合 trait。

精确方法签名和各后端的 trait 实现请参阅 crate 的
[API 文档](https://docs.rs/qubit-lock)。
