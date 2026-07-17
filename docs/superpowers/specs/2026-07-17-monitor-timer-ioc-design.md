# rs-lock Monitor Timer IOC 重构设计

## 背景

Monitor 的本质是“持有状态锁、登记条件 waiter、释放状态锁、在通知或 deadline
到达后重新获得状态锁并重检条件”。其中 deadline 是时间能力，不应由
`StdMonitor`、`ParkingLotMonitor`、`TokioMonitor` 各自绑定不同的原生超时
机制。

当前 `MockMonitor` 通过实现另一套等待算法来模拟时间。它可以辅助单元测试，
但不能证明真实 Monitor 在手动时间下的注册、通知、取消和边界竞争行为正确。
集成测试需要替换的是 Timer，而不是 Monitor。

本设计依赖 `rs-clock` 的 `Timer`、`TimerFuture` 和
`MonotonicClock::new_timer`，允许任意破坏性变更。

## 设计目标

- 生产和集成测试运行同一个具体 Monitor 实现。
- `StdMonitor`、`ParkingLotMonitor` 和 `TokioMonitor` 都通过 IOC 接收 Timer。
- 同步与异步 Monitor 遵循相同的 fixed-deadline、取消和边界判定语义。
- notification 与 Timer Waker 唤醒同一个 waiter，消除 lost wake-up 窗口。
- Timer 创建错误通过 Monitor API 返回，并且不丢失已持有的 guard。
- 删除 `MockMonitor` 及 `mock` feature。

## 非目标

- 不把 Monitor 改成 object-safe 服务接口；现有能力 trait 继续用于静态泛型。
- 不保证 waiter 公平性或通知顺序。
- 不提供 queued permit；notification 继续是 memoryless condition signal。
- 不把 Clock、Timer、Monitor 聚合成全局 IOC 容器。
- 不在 async runtime worker 上隐式执行阻塞等待。

## 依赖和 Feature

`qubit-clock` 从仅供 `mock` feature 使用的 optional dependency 改为
`rs-lock` 的必要依赖，因为所有 timed Monitor 都需要 `Timer` 和
`MonotonicInstant`。

- 删除 `mock` feature；
- 删除 `MockMonitor`、`ArcMockMonitor` 及其专用内部 registry/state；
- `async` feature 同时启用 Tokio 和 `qubit-clock/tokio`；
- 默认 feature 仍可只构建同步 Monitor，但同步 Monitor 也依赖基础
  `qubit-clock`。

## 构造与 IOC

每个真实 Monitor 提供两种构造入口：

```rust
impl<T> ParkingLotMonitor<T> {
    pub fn new(state: T) -> Self;

    pub fn with_timer(
        state: T,
        timer: Arc<dyn Timer>,
    ) -> Self;

    pub fn timer(&self) -> &dyn Timer;
}
```

`StdMonitor` 与 `TokioMonitor` 提供同形接口；Arc-backed wrapper 同样提供
`with_timer`，并继续包装对应的具体 Monitor。

- `ParkingLotMonitor::new` 和 `StdMonitor::new` 使用进程内共享的默认
  `StdTimer`；
- `TokioMonitor::new` 使用新建 `TokioMonotonicClock` 所创建的
  `TokioTimer`；
- `with_timer` 是生产 composition root 和集成测试共同使用的 IOC 入口；
- Monitor 不接收独立 Clock，所有 deadline 通过 `timer.clock()` 采样，避免
  Clock/Timer domain 配错；
- `timer()` 让默认构造的 Monitor 也能公开其 deadline domain，调用者可以先
  通过 `monitor.timer().clock().now()` 构造 guard `wait_until` 所需的绝对
  deadline。

生产代码保持简单：

```rust
let monitor = ParkingLotMonitor::new(State::default());
```

集成测试只替换 Timer：

```rust
let clock = ManualMonotonicClock::new_shared();
let timer = clock.new_timer();
let monitor = ParkingLotMonitor::with_timer(
    State::default(),
    timer,
);
```

## 公共等待 API

### Predicate wait

无超时的 `ConditionWaiter`/`AsyncConditionWaiter` API 保持现有结果类型。
所有 timed predicate wait 增加 Timer 创建错误：

```rust
fn wait_while_for<R, P, F>(
    &self,
    timeout: Duration,
    predicate: P,
    action: F,
) -> Result<WaitTimeoutResult<R>, TimeError>;

fn wait_until_for<R, P, F>(
    &self,
    timeout: Duration,
    predicate: P,
    action: F,
) -> Result<WaitTimeoutResult<R>, TimeError>;
```

异步对应方法返回：

```rust
impl Future<
    Output = Result<WaitTimeoutResult<R>, TimeError>,
> + Send + '_
```

`WaitTimeoutResult<R>` 继续只区分 `Ready(R)` 与 `TimedOut`；时间域、溢出和
driver/registration failure 属于 `TimeError`，不伪装成 timeout。

### Guard wait

Guard 不再被 wait 方法消费和重新返回。等待操作原地更新 guard：

```rust
impl<T> ParkingLotMonitorGuard<'_, T> {
    pub fn wait(&mut self);

    pub fn wait_for(
        &mut self,
        duration: Duration,
    ) -> Result<WaitTimeoutStatus, TimeError>;

    pub fn wait_until(
        &mut self,
        deadline: MonotonicInstant,
    ) -> Result<WaitTimeoutStatus, TimeError>;
}
```

`StdMonitorGuard` 提供同形 API。`wait_timeout` 被 `wait_for` 取代；新增
`wait_until` 接受绝对 deadline。使用 `&mut self` 的关键原因是 Timer 创建
失败时 guard 仍在调用者手中并继续持有状态锁，不会被错误返回路径吞掉。

Guard 内部使用私有 guard slot/state machine 暂时移出底层 mutex guard，并在
等待结束后重新放回。所有正常返回和 `Err` 返回都保证 slot 中存在已重新取得的
guard；Timer 创建和 deadline 校验发生在移出 guard 之前。panic unwind 仍按底层
mutex 的正常语义释放锁，不能留下可被公开 API 观察到的空 slot。

`WaitTimeoutStatus` 继续使用 `Woken` 与 `TimedOut`。`Woken` 可能来自伪唤醒，
调用者仍需重检状态。

## 统一 timeout 语义

相对 predicate timeout 是 condition-wait budget：

1. 获得状态锁并检查 predicate；
2. predicate 已满足时直接执行 action，不创建 Timer；
3. 首次确实需要挂起时调用一次 `timer.after(timeout)`，固定绝对 deadline；
4. 在持有状态锁期间完成 Monitor waiter 登记和 TimerFuture 首次 poll；
5. 释放状态锁并等待 notification 或 deadline；
6. notification/伪唤醒后复用同一个 TimerFuture，不重新开始 timeout；
7. deadline 到达后重新取得状态锁并做最后一次 predicate 检查；
8. 最终 predicate 已满足时 `Ready` 胜过 `TimedOut`，否则返回 `TimedOut`。

因此，首次获得状态锁之前的竞争不计入 timeout；为重检状态而重新取得锁可能使
实际返回时间晚于 deadline。零 timeout 仍然先检查一次 predicate。

Guard 的 `wait_for` 在调用时固定 deadline；`wait_until` 使用调用者提供的同域
deadline。两者返回前都保证 guard 已重新获得状态锁。Timer 创建失败发生在
释放 guard 之前。

## 同步 Monitor 内部模型

`ParkingLotMonitor<T>` 和 `StdMonitor<T>` 由以下概念状态组成：

- 受 mutex 保护的用户状态；
- 显式、memoryless 的 condition waiter registry；
- `Arc<dyn Timer>`。

不再把原生 Condvar 的 timeout 作为 deadline 驱动器，也不通过一个共享
Condvar 广播所有事件。每个阻塞 waiter 拥有私有 signal，并实现 `Wake`：

- `notify_one` 从 registry 选择至多一个已登记 waiter 并调用同一 signal；
- `notify_all` 取出所有已登记 waiter 并逐一 signal；
- TimerFuture 使用同一个 waiter Waker；
- signal 必须锁存 wake-before-block，避免 Waker 在真正 park 前触发而丢失；
- Waker 和可能析构用户代码的对象都在 registry/state 锁外调用或释放。

登记与释放状态锁形成原子边界：waiter 在持有状态锁检查 predicate 后先加入
registry、建立 TimerFuture 的 Waker，再释放状态。若 notification 先于真正
阻塞发生，私有 signal 会记住该事件。

被 notification 唤醒而 predicate 仍不满足时，waiter 在持有状态锁期间重新
登记，但继续 poll 原来的 TimerFuture。deadline 不因任何 notification、伪唤醒
或重新登记而改变。

无超时 guard wait 使用同一 waiter registry，只是不创建 TimerFuture。
RAII registration guard 负责 panic/unwind 时清理 registry 条目。

## TokioMonitor 内部模型

`TokioMonitor` 保留现有显式 async waiter registry 和每 waiter 私有 signal，
但 timed wait 不再直接创建 Tokio Sleep。它持有 `Arc<dyn Timer>`，并在首次
需要挂起时竞争：

- waiter notification Future；
- 一个固定的 TimerFuture。

每次 notification 后重检 predicate；若仍需等待则重新登记 notification，
同时继续使用原 TimerFuture。Future 被取消时，Monitor waiter registration 和
Timer registration 都通过 Drop 立即清理，不把 notification 转移给其他 waiter。

使用 ManualTimer 时，这条路径与生产 TokioTimer 完全相同，因此集成测试可以
覆盖真实 TokioMonitor 的注册、取消和边界竞争。

## Notification 契约

通知继续遵循 memoryless Monitor 语义：

- `notify_one` 只选择调用发生时已经登记的至多一个 waiter；
- `notify_all` 只选择调用发生时已经登记的全部 waiter；
- 没有 waiter 时通知不产生未来 permit；
- 通知只表示“状态可能变化”，不保证 predicate 已满足；
- 不承诺公平性。

同步和异步实现都必须在 waiter registry 锁外执行 wake。

## 删除 MockMonitor

删除：

- `MockMonitor`
- `ArcMockMonitor`
- mock 专用 waiter registry、state、guard 和测试 hook
- `mock` Cargo feature
- 只验证 Mock 行为而不验证真实 Monitor 的测试

原有 Mock 测试按目标实现迁移为：

```text
ParkingLotMonitor/StdMonitor/TokioMonitor
                +
          ManualTimer
```

测试保留 `Arc<ManualMonotonicClock>` 作为控制面，通过
`wait_for_waiters`、`wait_for_next_deadline` 和
`advance_to_next_deadline` 协调，不在生产 Monitor 中加入 test-only hook。

## 下游 IOC 约束

- 需要 Monitor 的应用组件继续持有具体 Monitor 或其 Arc wrapper；
- 构造函数/builder 接收 Timer，并调用 Monitor 的 `with_timer`；
- 不注入 `dyn Monitor`，因为 predicate closure API 本身不适合 object-safe
  trait object；
- 多个组件需要同一测试时间线时，共享一个 ManualClock，并可共享一个 Timer
  或从同一 Clock 创建多个 Timer；
- 同步 retry 使用 `BlockingSleeper`，异步 retry 和 Monitor 使用 Timer；它们
  可以来自同一 ManualClock，从而由一个测试控制面推进时间。

## 测试要求

所有测试继续位于 `tests/monitor/` 外部目录，并至少覆盖：

- 每个真实 Monitor 的默认 Timer 和 `with_timer` 构造；
- ParkingLot/Std/Tokio Monitor 配合 ManualTimer，不等待真实时间；
- waiter 在释放状态锁前完成登记，确定性证明无 lost wake-up；
- wake-before-block、伪唤醒、重复 notification 与固定 deadline；
- 零 timeout、已经满足 predicate 和 deadline 边界；
- notification 与 timeout 同时发生时最终 predicate readiness 获胜；
- foreign deadline、overflow，以及映射 driver/registration failure 的
  `TimeError::TimerUnavailable`；
- Guard 出错后仍持有并可使用；
- Tokio Future cancellation 同时清理两类注册；
- `notify_one`/`notify_all` 的 memoryless 行为；
- ManualClock 的 waiter/deadline 观察准确包含真实 Monitor 注册；
- 默认、`async` 和 all-features feature matrix，以及 rustdoc warnings denied；
- 编译测试确认 `MockMonitor`/`ArcMockMonitor`/`mock` feature 已删除。

实现遵循 TDD：先把现有 Mock 场景改写为“真实 Monitor + ManualTimer”的失败
测试，再修改生产实现。完成后依次运行 crate 的格式、lint、测试、文档和 feature
matrix 检查。

## 与既有设计的关系

本设计保留 `2026-07-15-monitor-semantics-redesign.md` 中的 memoryless
notification、fixed condition-wait budget、最终 predicate 检查和 Tokio 显式
waiter registry 结论；Timer IOC 取代其中依赖原生 timeout 与 MockMonitor 的
部分。

本设计取代 `2026-07-16-rs-lock-composability-hardening-design.md` 中继续维护
`MockMonitor`、`ArcMockMonitor` 和 `mock` feature 的结论。
