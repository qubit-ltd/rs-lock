# Qubit Lock 用户手册

[English](user_guide.md)

`qubit-lock` 为同步锁、Tokio 锁和 monitor 风格的条件协调提供统一抽象。本手册先说明
这个 crate 解决的问题，再详细介绍每个公开组件的选择与用法。

+

## 1. 先做选择

如果锁只服务于一个私有实现，没有 waiter、后端替换或确定性 timeout 测试需求，
直接使用原生锁。可复用边界则应使用 `qubit-lock`：

| 组件需要的能力 | 使用 |
| --- | --- |
| 读取或修改由多种原生锁持有的数据 | `DataLock<T>` |
| 一种 guard 获取模式 | `Lock` |
| 必须排除所有其他 guard 的获取模式 | `ExclusiveLock` |
| 共享与独占模式 | `ReadWriteLock` |
| 状态、predicate wait 与 notification | 具体 monitor 或最小 monitor 能力 trait |
| 不使用 sleep 的 timeout 测试 | 由 `with_timer` 构造的 monitor |

下面的案例先证明这些边界，之后再提供完整 API 参考。

## 2. 案例：可关闭的有界任务队列

任务队列中有两类 waiter：worker 等待队列非空或关闭；producer 等待队列未满或关闭。
两类就绪条件都属于同一份受保护状态。

```rust
use std::{
    collections::VecDeque,
    num::NonZeroUsize,
};

struct QueueState<T> {
    items: VecDeque<T>,
    capacity: NonZeroUsize,
    closed: bool,
}

impl<T> QueueState<T> {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            items: VecDeque::new(),
            capacity,
            closed: false,
        }
    }
}
```

不变量是明确的：`items.len() <= capacity.get()` 始终成立；`closed` 后拒绝新任务；
空且关闭的队列返回 `None`；加入、移除或关闭都可能让另一类 waiter 就绪。

### 一份领域实现，两个阻塞后端

领域函数依赖 `Monitor`、`ConditionWaiter` 和 `Notifier`，而不是某一种具体锁或
condition-variable guard。

```rust
use qubit_lock::Monitor;

fn push<M, T>(queue: &M, item: T) -> Result<(), T>
where
    M: Monitor<State = QueueState<T>> + ?Sized,
{
    let result = queue.wait_until(
        |state| state.closed || state.items.len() < state.capacity.get(),
        |state| {
            if state.closed {
                Err(item)
            } else {
                state.items.push_back(item);
                Ok(())
            }
        },
    );
    if result.is_ok() {
        queue.notify_all();
    }
    result
}

fn pop<M, T>(queue: &M) -> Option<T>
where
    M: Monitor<State = QueueState<T>> + ?Sized,
{
    let item = queue.wait_until(
        |state| state.closed || !state.items.is_empty(),
        |state| state.items.pop_front(),
    );
    if item.is_some() {
        queue.notify_all();
    }
    item
}

fn close<M, T>(queue: &M)
where
    M: Monitor<State = QueueState<T>> + ?Sized,
{
    queue.with_write_notify_all(|state| state.closed = true);
}
```

该队列有两类 predicate：“未满”和“非空”。`notify_one` 可能唤醒 predicate 仍为
false 的 waiter，而另一类已经可以继续的 waiter 仍在休眠。因此，本案例在可能改变
就绪条件的状态转换后使用 `notify_all`，让每个 waiter 在 monitor 锁内重新检查自己
的 predicate。这不表示 `notify_all` 永远更好：单一 predicate 的 waiter 集通常可以
优先使用 `notify_one`。

```rust
use std::{
    num::NonZeroUsize,
    sync::Arc,
};

use qubit_lock::{
    ParkingLotMonitor,
    StdMonitor,
};

fn exercise<M>(queue: &M)
where
    M: Monitor<State = QueueState<i32>> + ?Sized,
{
    assert_eq!(push(queue, 7), Ok(()));
    assert_eq!(pop(queue), Some(7));
    close(queue);
    assert_eq!(push(queue, 8), Err(8));
    assert_eq!(pop(queue), None);
}

fn main() {
    let capacity = NonZeroUsize::new(2).expect("capacity must be non-zero");

    let std_queue = Arc::new(StdMonitor::new(QueueState::new(capacity)));
    exercise(&std_queue);

    let parking_lot_queue = Arc::new(ParkingLotMonitor::new(QueueState::new(capacity)));
    exercise(&parking_lot_queue);
}
```

具体 monitor 在组装队列的位置选择。`exercise`、`push`、`pop` 和 `close` 都不需要
修改。原生 `Mutex`/`Condvar` 代码则需要让后端专用 guard 穿过每次等待、手动循环检查
predicate、决定 poisoning 策略，并自己维持状态更新与 waiter 注册的同一协议。

### 计时接收与确定性时间

计时接收的结果为：

```text
Result<WaitTimeoutResult<Option<T>>, qubit_clock::TimeError>
```

`Ready(Some(task))` 表示取得任务，`Ready(None)` 表示队列关闭且排空，`TimedOut` 表示
最终持锁 predicate 检查仍为 false，`Err(TimeError)` 表示 Timer 注册或完成失败，而
不是真实 timeout。

```rust
use std::time::Duration;

use qubit_lock::{
    TimedMonitor,
    WaitTimeoutResult,
};

fn pop_for<M, T>(
    queue: &M,
    timeout: Duration,
) -> Result<WaitTimeoutResult<Option<T>>, qubit_clock::TimeError>
where
    M: TimedMonitor<State = QueueState<T>> + ?Sized,
{
    queue.wait_until_for(
        timeout,
        |state| state.closed || !state.items.is_empty(),
        |state| state.items.pop_front(),
    )
}
```

每个具体 monitor 都提供 `with_timer`。测试向生产使用的 `ParkingLotMonitor` 注入
`ManualMonotonicClock`，并且只在 waiter 注册后推进时间：

```rust
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use qubit_clock::{
    ManualMonotonicClock,
    MonotonicClock,
};
use qubit_lock::{
    ParkingLotMonitor,
    WaitTimeoutResult,
};

fn main() {
    let clock = ManualMonotonicClock::new_shared();
    let capacity = NonZeroUsize::new(1).expect("capacity must be non-zero");
    let queue = Arc::new(ParkingLotMonitor::with_timer(
        QueueState::<i32>::new(capacity),
        clock.new_timer(),
    ));
    let waiting_queue = Arc::clone(&queue);

    let waiter = std::thread::spawn(move || {
        pop_for(&*waiting_queue, Duration::from_secs(16))
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

生产等待算法运行在注入的 Timer 上。它的 waiter 与 deadline 观察接口会在推进时间前
确认注册，因此不需要 `thread::sleep`。

### Tokio 保留状态机，而不是阻塞调用

Tokio 版本保留 `QueueState<T>`、两类 predicate、关闭规则和结果含义。它使用
`AsyncMonitor` 和 `AsyncTimedMonitor`，具体类型是 `TokioMonitor`。持锁期间的
async closure 仍然是同步 closure；不要在其中 await 或执行阻塞 I/O。

```rust
use std::{
    num::NonZeroUsize,
    sync::Arc,
};

use qubit_lock::{
    AsyncConditionWaiter,
    AsyncMonitor,
    TokioMonitor,
};

#[tokio::main]
async fn main() {
    let capacity = NonZeroUsize::new(1).expect("capacity must be non-zero");
    let queue = Arc::new(TokioMonitor::current(QueueState::new(capacity)));
    let worker_queue = Arc::clone(&queue);

    let worker = tokio::spawn(async move {
        worker_queue
            .wait_until_async(
                |state| state.closed || !state.items.is_empty(),
                |state| state.items.pop_front(),
            )
            .await
    });

    queue
        .with_write_notify_all_async(|state| state.items.push_back(7))
        .await;

    assert_eq!(worker.await.expect("worker should finish"), Some(7));
}
```

异步等待 future 是惰性的。Timer 属于捕获或注入的目标 runtime；该 runtime 必须存活并
启用 time driver，直到计时等待完成。drop pending future 会注销 waiter，不执行
action，也不会回滚其他 task 已经完成的状态修改。如果 `notify_one` 已选择该 waiter，
取消会丢弃这次 selection，而不会转交给另一个 waiter。

## 3. 为什么需要这些抽象

Rust 已经提供了优秀的锁实现，但应用和库代码仍会反复遇到三个问题：

1. `std`、`parking_lot` 和 Tokio 暴露的具体 API 不同。只需要“获取锁”或“读取受保护
   数据”的泛型代码，不应该被迫了解调用方选择了哪个后端。
2. 锁可以保护状态，却不能独立表达“休眠直到某个 predicate 成立”。如果状态更新、
   predicate 检查和 waiter 注册没有遵循同一个协议，手写的条件变量代码可能丢失通知。
3. 真实 sleep 会让超时测试变慢且不稳定。测试应运行生产环境的等待算法，同时能够
   确定性地控制时间。

队列案例把这三类边界具体化：锁 trait 避免后端泄漏，monitor 操作承载持锁 predicate
协议，Timer 注入则在没有独立 mock 算法的前提下执行生产 timeout 路径。

## 4. 安装与 Feature 选择

默认特性集为空；请显式启用程序使用的组件：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["monitor", "parking-lot"] }
```

根据所需组件选择 Feature：

| Feature | 启用的能力 |
| --- | --- |
| 不启用可选 Feature | 同步锁 trait 和 `std` 锁实现 |
| `parking-lot` | parking_lot mutex 和读写锁实现 |
| `monitor` | monitor trait、标准库 monitor、计时等待和 Timer 注入 |
| `async-lock` | Tokio 锁 trait 和适配器 |
| `async-monitor` | `async-lock`、monitor 支持和 Tokio monitor |
| 默认配置 | 不启用可选 Feature |

只使用锁的用户可以避免所有可选依赖：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false }
```

只启用异步锁，不启用 Tokio monitor deadline：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["async-lock"] }
```

启用 Tokio monitor 协调和计时等待：

```toml
[dependencies]
qubit-lock = { version = "0.12", default-features = false, features = ["async-monitor"] }
```

如果应用创建 Tokio runtime，应在应用自己的 `Cargo.toml` 中启用所需的 runtime
Feature，例如 `rt` 或 `rt-multi-thread`。

所有 `qubit-lock` 公开类型都从 crate root 导入。

## 5. 同步锁组件

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

## 6. 异步锁组件

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

## 7. Monitor 能力组件

monitor 持有受保护状态，并使用 notification 协调 predicate wait。应用代码通常应
选择具体 monitor；在泛型 API 边界使用能力 trait：

| 组件 | 能力 |
| --- | --- |
| `Notifier` | 只提供 `notify_one` 和 `notify_all` |
| `ConditionWaiter` | 同步 `wait_until`、`wait_until_ready` 和 `wait_while` |
| `TimeoutConditionWaiter` | 同步条件预算 `*_for`、绝对 deadline `*_with_deadline` 和整个操作预算 `*_with_total_timeout` 等待 |
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

## 8. 具体 Monitor 组件

### `ParkingLotMonitor<T>`

启用 `parking-lot` 和 `monitor` Feature 后，需要高效阻塞式协调时使用
`ParkingLotMonitor<T>`。需要在线程间克隆或长期持有句柄时，使用
`Arc<ParkingLotMonitor<T>>`。

重要方法包括：

- `new` 和 `with_timer`：构造 monitor。
- `with_read` 和 `with_write`：访问状态。
- `with_write_notify_one` 和 `with_write_notify_all`：执行常规的状态更新与通知协议。
- `wait_until` / `wait_while`、无 action 的 `wait_until_ready` 及对应的 `_for` 计时版本。
- `lock`：需要显式控制 guard 时使用。

直接使用标准库的 `Arc`；它的 deref coercion 保留 monitor API，无需 crate 专用包装器。

### `StdMonitor<T>`

`StdMonitor<T>` 使用标准库原语，并提供相同的高层 API。当避免 parking_lot 依赖比
使用该后端更重要时选择它。需要共享时使用 `Arc<StdMonitor<T>>`。

与标准库的 `Lock` 和 `DataLock` 适配器不同，`StdMonitor` 在 poisoning 后会恢复
并继续提供内部状态，而不是拒绝访问。线程持有状态锁时发生 panic，可能使状态只完成
部分修改。`is_poisoned` 用于检查是否发生过这种情况；普通的 `lock`、`with_read`、
`with_write` 和等待操作仍可使用，但不会自动清除 poisoning 标记。调用者应先检查状态，
必要时修复受保护的不变量，再调用 `clear_poison` 明确接受恢复后的状态。
`clear_poison` 只清除标记，不会验证或回滚状态；以后再次在持锁期间 panic，monitor
仍会重新进入 poisoned 状态。`Arc<StdMonitor<T>>` 通过 deref coercion 同样公开这两个方法。

```rust
use std::sync::Arc;

use qubit_lock::StdMonitor;

fn main() {
    let monitor = Arc::new(StdMonitor::new(false));
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

### `TokioMonitor<T>`

该类型需要 `async-monitor`。任务局部持有时使用 `TokioMonitor<T>`；需要可克隆
共享所有权时使用 `Arc<TokioMonitor<T>>`。

- `current` 为默认 Timer 捕获当前 Tokio runtime Handle。
- `try_current` 在缺少当前 runtime 时返回错误，而不是 panic。
- `with_timer` 注入显式 Timer。
- `with_read_async`、`with_write_async` 和组合式
  `with_write_notify_*_async` 方法访问状态。
- `wait_until_async` / `wait_while_async` 及对应的 `_for_async` 版本等待 predicate；
  `wait_until_ready_async` 和 `wait_until_ready_for_async` 提供无 action 的形式。

```rust
use std::sync::Arc;

use qubit_lock::{AsyncConditionWaiter, TokioMonitor};

#[tokio::main]
async fn main() {
    let monitor = Arc::new(TokioMonitor::current(Vec::<i32>::new()));
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

## 9. 等待、通知与超时语义

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

use qubit_lock::StdMonitor;

fn main() {
    let ready = Arc::new(AtomicBool::new(false));
    let monitor = Arc::new(StdMonitor::new(()));
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

use qubit_lock::{AsyncConditionWaiter, TokioMonitor};

#[tokio::main]
async fn main() {
    let ready = Arc::new(AtomicBool::new(false));
    let monitor = Arc::new(TokioMonitor::current(()));
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
并且不会执行 action。

对于阻塞代码，`wait_while_with_total_timeout`、
`wait_until_with_total_timeout` 和
`wait_until_ready_with_total_timeout` 会在初始获取状态锁之前固定绝对 deadline。
因此锁竞争、predicate 求值和等待会消耗同一个整个操作预算。
这些方法仍不是严格的返回时限：到达 deadline 无法中断 mutex 的获取或重新获取，
ready action 也没有执行时限。

异步等待 future 是惰性的：首次 poll 前的时间不消耗预算，初始异步状态锁竞争也不
计入预算。drop pending future 会注销其 waiter，也不会执行 action。取消不会回滚受保护状态的变化，
包括其他任务已经完成的修改。如果 `notify_one` 已经选择该 waiter，
取消会丢弃这次选择，不会转交给其他或未来 waiter。

## 10. 等待结果与错误

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

## 11. 测试中的确定性时间

每个具体 monitor 都提供 `with_timer`。测试可注入 `qubit-clock` 的 `ManualTimer`，
使生产环境使用的 monitor 类型和等待算法在没有真实 sleep 的情况下直接运行。

直接声明测试时钟依赖：

```toml
[dev-dependencies]
qubit-clock = { version = "0.11", features = ["test-util"] }
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

## 12. 组件选择与常见错误

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
| 克隆 monitor 句柄 | `Arc<ParkingLotMonitor<T>>`、`Arc<StdMonitor<T>>` 或 `Arc<TokioMonitor<T>>` |
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
