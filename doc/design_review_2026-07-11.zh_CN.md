# rs-lock 设计与实现评审

评审日期：2026-07-11

评审版本：`qubit-lock 0.9.0`

## 1. 评审结论

`rs-lock` 的总体设计成立，是一个可以继续作为 Qubit Rust 并发基础设施使用的 crate。它最有价值的部分是 `parking_lot` monitor 及其 `Arc` 包装：这一部分解决了下游 executor、线程池中的真实协调问题，抽象边界也比直接散落 `Mutex + Condvar` 更清楚。

同步 lock wrapper、标准库 poison 语义、Tokio async wrapper 和 monitor capability trait 的实现总体规整，测试覆盖也较充分。当前没有发现生产 monitor 主路径上的系统性正确性问题。

需要优先修复一个明确的测试替身语义缺陷：`MockMonitor::notify_one` 在多 async timeout waiter 场景中会被共享 epoch/watch 广播解释为“所有 waiter 都收到通知”。除此之外，后续重点应是收敛默认 feature 和 API 命名，而不是扩大 lock/monitor 类型矩阵。

综合评分：**8/10**。可以继续使用，但 `MockMonitor` 的单唤醒语义修复前，不应把它当成真实 monitor 的严格等价替身来验证多 waiter 调度。

## 2. 评审依据

### 2.1 代码与验证

本轮评审覆盖同步和异步 lock、parking_lot/std/Tokio/mock monitor、超时等待、条件等待以及下游调用方式，并执行过以下验证：

- all-feature 测试：432 项通过。
- `--no-default-features` 同步测试：321 项通过。
- nightly clippy：all-feature 与 no-default-feature 均通过，且启用 `-D warnings`。
- rustdoc：all-feature 与 no-default-feature 均通过，且启用 `-D warnings`。
- 直接下游的 all-target/all-feature check：通过。

这些结果说明 feature gate、同步/异步两套实现和公开文档当前保持一致。

### 2.2 下游实际使用

工作区内有 5 个 crate 直接依赖 `qubit-lock`：

- `rs-dcl`
- `rs-executor`
- `rs-rayon-executor`
- `rs-thread-pool`
- `rs-tokio-executor`

非 `rs-dcl` 的生产代码主要使用：

- `ParkingLotMonitor`
- `ArcParkingLotMonitor`
- `ParkingLotMonitorGuard`
- `WaitTimeoutStatus`

`rs-dcl` 主要使用 `ArcMutex` 和 `Lock`。当前工作区尚未形成对 async lock wrapper、Tokio monitor、std monitor 和 mock monitor 的生产级下游使用。

这意味着 crate 的核心价值排序很清楚：

1. parking_lot monitor；
2. `Lock`/`ArcMutex` 这类 closure-scoped lock API；
3. 其他实现作为兼容能力和测试能力存在，但不应在缺少下游证据时继续扩张。

## 3. 值得保留的设计

### 3.1 lock 与 monitor 职责分离

普通互斥访问和带条件通知的协调是两类不同问题。crate 没有把所有能力堆到一个大接口中，而是由 `Lock`/`AsyncLock` 负责受保护数据访问，由 monitor trait 负责 wait/notify/condition/timeout。这一边界是合理的。

### 3.2 closure-scoped API 能缩短临界区的结构范围

`read`、`write` 接收 closure，使 guard 的生存期被限制在一次调用内部。对大多数下游，这比返回 guard 更容易 review，也减少了意外长期持锁的机会。同时 `Arc*` wrapper 通过 `Deref`/`AsRef` 保留原生 guard API，没有封死高级用法。

### 3.3 poison 语义没有被错误统一

`parking_lot` wrapper 与标准库 wrapper 分开建模，调用方可以明确选择是否需要 poison 行为。`TryLockError` 等公开类型也为不同实现提供了可检查的结果，而不是把所有失败压成布尔值。

### 3.4 monitor 的 predicate wait 采用重检模型

真实 monitor 的条件等待围绕“释放锁、等待、重新获得锁、再次检查 predicate”组织，能够正确处理虚假唤醒和竞争。`wait_until`/`wait_while` 及 timeout 结果把这一惯用法集中到库内，降低下游重复写错的概率。

### 3.5 async 能力可被 feature 关闭

Tokio 相关类型由 `async` feature 控制，`--no-default-features` 可以得到同步子集。这一点已经由独立 clippy、test 和 rustdoc 验证，说明 feature 边界不是名义上的。

### 3.6 crate root 作为稳定导入面

`lock` 与 `monitor` 实现模块是私有的，公开类型统一从 crate root 重导出。这减少了调用方依赖内部目录结构的机会，是合适的兼容策略。

## 4. 主要问题

### 4.1 高优先级：`MockMonitor::notify_one` 不保持单 waiter 语义

`MockMonitor` 使用 `notification_epoch` 表示发生了通知。async timeout waiter 还通过 `watch::Sender<u64>` 接收所有状态或时间变化。`notify_one` 当前同时执行：

1. 增加全局 `notification_epoch`；
2. 调用 `Notify::notify_one()`；
3. 通过 watch channel 广播新的 change epoch。

每个 timeout waiter 都保存相同的旧 `notification_epoch`。watch 广播后，所有 waiter 都会重新检查并发现全局 epoch 已变化，因此都返回 `WaitTimeoutStatus::Woken`。实测两个已经开始等待的 `wait_for_async` waiter 在一次 `notify_one` 后都返回 `Woken`。

同步 waiter 虽然依赖 `Condvar::notify_one()`，但同样使用全局 epoch 判断“自己是否被通知”；后续虚假唤醒或其他 change 也可能让未被选中的 waiter 把别人的通知当作自己的通知。因此根因不是 Tokio `Notify`，而是“全局通知代数”无法表达单个 waiter 的消费权。

影响：

- `MockMonitor` 不能严格模拟真实 monitor 的单唤醒行为；
- 使用它测试 worker 唤醒数、公平性、容量和惊群控制会得到假阳性；
- `notify_all` 与 `notify_one` 在部分 timeout 场景中退化为相同观察结果。

建议不要只调整唤醒 primitive，而要调整状态模型。可选方案是：

- 为单通知维护可消费 permit/ticket，由成功消费的一个 waiter 推进自己的观察状态；
- 分离“状态/时间发生变化”的 broadcast epoch 与“通知许可”的计数；
- `notify_one` 增加一个 permit，`notify_all` 记录一代 broadcast，waiter 返回前原子地确认自己拥有相应通知；
- 增加至少两个 blocking waiter 和两个 async timeout waiter 的回归测试，明确一次 `notify_one` 只允许一个 waiter 返回 `Woken`。

相关实现位于 `src/monitor/mock_monitor.rs` 的 `notify_one`、`advance_notification_epoch`、`wait_for` 和 `wait_for_async`。

### 4.2 中优先级：默认启用 async 与主要使用面不匹配

`async` 当前是默认 feature，因此普通依赖会把 Tokio 的 `sync` 和 `time` 带入依赖图。工作区已有两个同步下游显式使用 `default-features = false`，而同步性质的 `rs-dcl` 仍通过默认依赖引入 async 能力。

这说明默认值并不完全符合主要下游。建议在下一个允许破坏 feature 默认值的版本中评估：

- 默认只启用同步 lock 与 monitor；
- async 使用者显式启用 `features = ["async"]`；
- 在迁移文档中列出 Tokio executor 等需要显式打开 feature 的下游。

变更默认 feature 会影响依赖方编译，必须按 breaking change 处理，不能在补丁版本静默调整。

### 4.3 中优先级：closure 方法与原生 guard 方法同名

`Lock::read`/`write` 与 `RwLock`、`Tokio RwLock` 的原生 `read`/`write` 同名。wrapper 又实现了 `Deref`/`AsRef`，因此当 trait 在作用域中时，调用者需要使用 `as_ref().read()` 或显式解引用来选择原生 guard API。README 已经解释这一点，但它仍是长期易用性负担。

建议：

- 当前版本保持兼容并继续强化文档；
- 下一个破坏性版本可评估把 closure API 命名为 `with_read`、`with_write`，使“执行 closure”与“取得 guard”一眼可辨；
- 不建议通过移除 `Deref` 来解决，因为下游确实需要原生能力，且那会降低 wrapper 的逃生能力。

### 4.4 中优先级：monitor capability trait 数量较多

通知、超时通知、条件等待、超时条件等待、同步、异步、共享 monitor 被拆成多组 trait。拆分本身符合 capability 设计，但对普通调用者而言，rustdoc 中会同时出现很多相似名称，第三方实现者也要理解默认方法之间的契约。

建议保持现有拆分，不再增加平行 trait，并补充一张“能力层级与推荐导入”表：

- 普通用户优先使用具体 monitor 的 inherent methods；
- 泛型库边界使用 `Monitor` 或 `AsyncMonitor` 聚合 trait；
- 只有需要最小 capability bound 时才直接依赖细粒度 waiter/notifier trait。

这能保留抽象能力，同时避免把所有用户都暴露给 trait 组合细节。

### 4.5 低优先级：多套 wrapper 的维护收益需要持续用下游证明

crate 同时维护 parking_lot、std 和 Tokio 的 mutex/rwlock/monitor wrapper，以及 `Arc` 和 mock 变体。当前实现尚可控，但继续沿每个底层 primitive 补齐同构 API，容易走向矩阵式增长。

建议设立扩展门槛：新的底层 lock 或 monitor 实现必须有明确生产下游、无法由现有 `Deref`/trait 适配解决，并补齐与现有实现一致的 poison、timeout、cancel safety 和 wake semantics 测试。

## 5. 建议的处理顺序

### 阶段一：正确性修复

1. 为 `MockMonitor` 增加多 waiter 的 `notify_one` 回归测试。
2. 重构 mock 的通知状态，使单通知可被一个 waiter 独占消费。
3. 同时验证 blocking、async、timeout 和 condition wait，不只修一个具体测试入口。
4. 在修复发布前，文档明确 mock 的现有限制。

### 阶段二：兼容性内收敛

1. 补充 monitor capability 导航和推荐使用层级。
2. 统计各下游是否真正需要默认 async。
3. 保持 root re-export，不公开实现模块。
4. 冻结新的同构 wrapper family。

### 阶段三：破坏性版本治理

1. 评估默认关闭 `async`。
2. 评估 closure API 使用 `with_read`/`with_write` 等无冲突名称。
3. 根据下游证据决定低使用率实现是继续维护、转 optional feature，还是停止扩张。

## 6. 最终意见

`rs-lock` 的核心方向正确，最值得继续投资的是已经被生产下游验证的 parking_lot monitor 和清晰的 lock capability 边界。当前不需要架构重写；应先修正 `MockMonitor` 的单唤醒模型，再通过 feature 默认值和命名治理降低外围成本。
