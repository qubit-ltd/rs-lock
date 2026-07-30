# Qubit Lock User Guide

[中文版](user_guide.zh_CN.md)

`qubit-lock` helps you write reusable concurrent code without tying its public
boundary to one synchronization backend. Its lock traits describe access to
protected data. Its monitors package the lock / condition-wait protocol and
can use an injected clock for timeout tests. This guide starts with the design
decision, then explains each public component.

## Reading paths

- **First use:** read Sections 1, 2, 4, and 12 to choose a boundary, enable
  features, and select a component.
- **Component lookup:** start with Sections 5 through 8, then use the API
  documentation for exact signatures.
- **Correct waiting and testing:** read Sections 9 through 11 before writing
  condition waits, deadlines, or deterministic timeout tests.

## Contents

1. [Make the boundary decision first](#1-make-the-boundary-decision-first)
2. [Case study: a closable bounded task queue](#2-case-study-a-closable-bounded-task-queue)
3. [Why these abstractions exist](#3-why-these-abstractions-exist)
4. [Installation and feature selection](#4-installation-and-feature-selection)
5. [Synchronous lock components](#5-synchronous-lock-components)
6. [Asynchronous lock components](#6-asynchronous-lock-components)
7. [Monitor capability components](#7-monitor-capability-components)
8. [Concrete monitor components](#8-concrete-monitor-components)
9. [Waiting, notification, and timeout semantics](#9-waiting-notification-and-timeout-semantics)
10. [Wait results and errors](#10-wait-results-and-errors)
11. [Deterministic time in tests](#11-deterministic-time-in-tests)
12. [Choosing components and avoiding mistakes](#12-choosing-components-and-avoiding-mistakes)

## 1. Make the boundary decision first

Use a native lock directly when it is an internal detail of one implementation.
Add `qubit-lock` when the component must support more than one backend, wait
for a condition, or test timeout behavior without sleeping:

| The component needs | Use |
| --- | --- |
| Read and write access through several native lock types | `DataLock<T>` |
| One guard acquisition mode | `Lock` |
| An acquisition mode that must exclude all other guards | `ExclusiveLock` |
| Shared and exclusive modes | `ReadWriteLock` |
| State plus condition waiting and notification | A concrete monitor or a narrow monitor capability trait |
| A timeout test without sleeping | A monitor built with `with_timer` |

The following case study shows why these boundaries matter before the API
reference sections.

## 2. Case study: a closable bounded task queue

A task queue has two kinds of waiters. A worker waits until the queue has an
item or is closed. A producer waits until the queue has space or is closed.
Both conditions are derived from the same protected state.

> **Feature prerequisite:** the blocking case study uses `StdMonitor` and
> `ParkingLotMonitor`. Enable `monitor` for the former and both `monitor` and
> `parking-lot` for the latter; Section 4 shows the dependency declarations.

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

The invariants are simple: `items.len() <= capacity.get()` always holds;
`closed` rejects new tasks; an empty, closed queue returns `None`; and adding,
removing, or closing can make one of the waiting groups ready.

### One queue implementation, two blocking backends

The domain functions depend on the monitor capabilities they use, not on a
specific lock type or condition-variable guard.

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

This queue has two predicates: “not full” and “not empty.” `notify_one` might
wake a producer when only a worker can proceed, or the other way around. The
example therefore calls `notify_all` after any state change that can affect
readiness. Each waiter then rechecks its own predicate while holding the
monitor lock. This does not make `notify_all` universally better: when every
waiter has the same predicate, `notify_one` is usually the better choice.

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

Choose the concrete monitor where the queue is assembled. `exercise`, `push`,
`pop`, and `close` stay the same. With raw `Mutex`/`Condvar` code, the queue
would instead carry backend-specific guards through waits, repeat each
condition loop by hand, choose a poisoning policy, and preserve the ordering
between state updates and waiter registration itself.

The [complete runnable example](../examples/bounded_queue.rs) uses the same
queue operations with `ParkingLotMonitor`, checks the zero-timeout path, and
wakes a consumer after it has observed an empty queue. Run it with:

```bash
cargo run --example bounded_queue --features monitor,parking-lot
```

### Timed receive and deterministic time

A timed receive returns:

```text
Result<WaitTimeoutResult<Option<T>>, qubit_clock::TimeError>
```

`Ready(Some(task))` returns a task, `Ready(None)` reports a closed and drained
queue, `TimedOut` means the final locked predicate check was still false, and
`Err(TimeError)` identifies Timer registration or completion failure rather
than a real timeout.

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

Every concrete monitor provides `with_timer`. The test injects a
`ManualMonotonicClock` into the production `ParkingLotMonitor` type and advances
only after the waiter is registered:

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

The production wait algorithm runs against the injected timer. The clock lets
the test observe waiter registration before advancing time, so no
`thread::sleep` is needed.

### Tokio keeps the state machine, not the blocking calls

The Tokio version keeps `QueueState<T>`, both predicates, the close rule, and
the result meanings. It uses `AsyncMonitor` and `AsyncTimedMonitor`, with
`TokioMonitor` as the concrete type. The closures run synchronously while the
monitor holds state; do not await or perform blocking I/O inside them.

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

Async wait futures are lazy. Their timer belongs to the captured or injected
target runtime, which must stay alive and have time enabled until a timed wait
completes. Dropping a pending future unregisters its waiter, does not run the
action, and does not roll back protected-state changes made by another task.
If `notify_one` selected that waiter, cancellation discards the selection; it
does not transfer it to another waiter.

## 3. Why these abstractions exist

Rust already has excellent lock implementations. Application and library code
still runs into three recurring problems when it needs to be reused:

1. `std`, `parking_lot`, and Tokio use different APIs. Generic code that only
   needs to acquire a lock or access protected data should not need to know
   which backend the caller selected.
2. A lock protects state, but it does not describe how to wait until a
   condition becomes true. Hand-written condition-variable code can lose a
   notification when state updates, condition checks, and waiter registration
   are not coordinated as one operation.
3. Real sleeps make timeout tests slow and flaky. A useful test runs the
   production wait algorithm while controlling time deterministically.

The queue makes the three boundaries concrete. Lock traits keep a backend out
of the queue API. Monitor operations keep the locked condition-wait protocol
in one place. Timer injection runs the production timeout path without a
second, test-only algorithm.

## 4. Installation and feature selection

The default feature set is empty. Enable the components used by your program:

```toml
[dependencies]
qubit-lock = { version = "0.13", default-features = false, features = ["monitor", "parking-lot"] }
```

Choose features according to the components you use:

| Feature | What it enables |
| --- | --- |
| no optional features | Synchronous lock traits and `std` lock implementations |
| `parking-lot` | Implementations for `parking_lot` mutexes and read-write locks |
| `monitor` | Monitor traits, std monitors, timed waits, and timer injection |
| `async-lock` | Tokio lock traits and adapters |
| `async-monitor` | `async-lock`, monitor support, and Tokio monitors |
| `loom-model` | Internal Loom model checking; not for normal application use |
| default | no optional features |

Lock-only users can avoid all optional dependencies:

```toml
[dependencies]
qubit-lock = { version = "0.13", default-features = false }
```

Enable asynchronous locks without Tokio monitor deadlines:

```toml
[dependencies]
qubit-lock = { version = "0.13", default-features = false, features = ["async-lock"] }
```

Enable Tokio monitor coordination and timed waits:

```toml
[dependencies]
qubit-lock = { version = "0.13", default-features = false, features = ["async-monitor"] }
```

If the application creates a Tokio runtime, enable the required runtime
features, such as `rt` or `rt-multi-thread`, in the application's own
`Cargo.toml`.

Using relative timeout methods does not require naming clock types. Add a
direct `qubit-clock` dependency when application code names `TimeError` or
`MonotonicInstant`, supplies an absolute deadline, or injects a timer.

All public `qubit-lock` types are imported from the crate root.

## 5. Synchronous lock components

### `DataLock<T>`

Use `DataLock<T>` when the data is stored inside the lock and the operation can
be expressed as a closure:

- `with_read` gives the closure `&T`.
- `with_write` gives the closure `&mut T`.
- `try_with_read` and `try_with_write` return immediately with
  `Result<_, TryLockError>`.

It is implemented for `std::sync::Mutex<T>`, `std::sync::RwLock<T>`, and,
with `parking-lot`, the corresponding parking_lot types. For a mutex, read and
write access both acquire the same exclusive lock. For a read-write lock,
`with_read` permits concurrent readers.

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

Keep the closure short. The lock remains held until the closure returns.
Standard-library implementations panic on blocking acquisition if the lock is
poisoned; the `try_*` methods report `TryLockError::Poisoned`.
Any callback panic propagates. With standard-library locks, it may also poison
the lock; parking_lot locks are not poisoned.

### `Lock` and `ExclusiveLock`

Use `Lock` when the lock and protected state are separate, or when generic code
needs only one acquisition mode. `lock` returns an RAII guard and `try_lock`
performs a non-blocking attempt.

`Lock` does not promise that its acquisition mode excludes every other guard:
a read-mode adapter also implements it. Add the marker trait `ExclusiveLock`
when a generic algorithm requires true exclusive entry.

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

`std::sync::Mutex`, `parking_lot::Mutex`, and write-mode adapters implement
`ExclusiveLock`. Read-mode adapters deliberately do not.

### `ReadWriteLock`, `ReadLock`, and `WriteLock`

`ReadWriteLock` represents a backend with explicit shared and exclusive modes:

- `read` and `write` return the backend's guards.
- `try_read` and `try_write` return `TryLockError` instead of blocking.
- `read_lock()` returns a borrowed `ReadLock`.
- `write_lock()` returns a borrowed `WriteLock`.

The adapters let an API written against `Lock` consume one side of a
read-write lock. `ReadLock` implements `Lock`; `WriteLock` implements both
`Lock` and `ExclusiveLock`.

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

All non-blocking lock APIs use the backend-independent `TryLockError`:

- `WouldBlock` means another guard currently prevents acquisition.
- `Poisoned` means a standard-library lock was poisoned by a panic.

parking_lot and Tokio locks do not have poisoning, so they report only
contention.

## 6. Asynchronous lock components

The types in this section require the `async-lock` feature.

### `AsyncDataLock<T>`

`AsyncDataLock<T>` is the asynchronous counterpart of `DataLock<T>`.
`with_read` and `with_write` wait without blocking the executor thread, then
run a synchronous closure while holding the guard. The `try_*` methods do not
wait.

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

Do not perform blocking I/O or await another future inside the closure. The
closure itself is synchronous and the lock stays held until it returns.
Any callback panic propagates; Tokio locks are not poisoned.

### `AsyncLock`, `AsyncReadWriteLock`, `AsyncReadLock`, and `AsyncWriteLock`

`AsyncLock` provides asynchronous `lock` and immediate `try_lock`.
`AsyncReadWriteLock` provides `read`, `write`, `try_read`, and `try_write`, plus
`read_lock()` and `write_lock()` adapters. `AsyncReadLock` represents the
shared side; `AsyncWriteLock` represents the exclusive side.

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

`AsyncLock` and `AsyncReadWriteLock` return `Send` futures. Tokio mutexes
implement `AsyncLock` when `T: Send`; Tokio read-write locks implement
`AsyncReadWriteLock` when `T: Send + Sync`.

## 7. Monitor capability components

A monitor owns protected state and coordinates predicate waits with
notifications. Application code should normally select a concrete monitor.
Use the capability traits at generic API boundaries:

| Component | Capability |
| --- | --- |
| `Notifier` | `notify_one` and `notify_all` only |
| `ConditionWaiter` | Synchronous `wait_until`, `wait_until_ready`, and `wait_while` |
| `TimeoutConditionWaiter` | Synchronous condition-budget `*_for`, absolute-deadline `*_with_deadline`, and operation-wide `*_with_total_timeout` waits |
| `Monitor` | State access, notification, and untimed synchronous waits |
| `TimedMonitor` | `Monitor` plus timed synchronous waits |
| `SharedMonitor` | A cloneable shared synchronous monitor handle |
| `AsyncConditionWaiter` | Asynchronous `wait_until_async`, action-free `wait_until_ready_async`, and `wait_while_async` |
| `AsyncTimeoutConditionWaiter` | Asynchronous relative `*_for_async` and absolute-deadline `*_with_deadline_async` predicate waits |
| `AsyncMonitor` | Async state access, notification, and untimed waits |
| `AsyncTimedMonitor` | `AsyncMonitor` plus timed waits |
| `SharedAsyncMonitor` | A cloneable shared asynchronous monitor handle |

For example, a generic producer that only changes state and wakes one waiter
needs `Monitor`, not the full timed-monitor capability:

```rust
use qubit_lock::Monitor;

fn publish<M>(monitor: &M, value: i32)
where
    M: Monitor<State = Vec<i32>> + ?Sized,
{
    monitor.with_write_notify_one(|items| items.push(value));
}
```

These traits use return-position `impl Future` and generic methods. They are
designed for static generic bounds, not `dyn` trait-object interfaces.

## 8. Concrete monitor components

### `ParkingLotMonitor<T>`

Use `ParkingLotMonitor<T>` for efficient blocking coordination when the
`parking-lot` and `monitor` features are enabled. Use
`Arc<ParkingLotMonitor<T>>` when the handle must be cloned or retained by
multiple threads.

Important methods are:

- `new` and `with_timer` for construction.
- `with_read` and `with_write` for state access.
- `with_write_notify_one` and `with_write_notify_all` for the normal
  state-update-and-notify protocol.
- `wait_until` / `wait_while`, action-free `wait_until_ready`, and their
  `_for` timed variants.
- `lock` when explicit guard-level control is necessary.

Use the standard `Arc` directly; its deref coercion preserves the monitor API
without a crate-specific wrapper.

### `StdMonitor<T>`

`StdMonitor<T>` has the same high-level API and uses standard-library
primitives. Choose it when avoiding the parking_lot dependency matters more
than using that backend. Use `Arc<StdMonitor<T>>` when it must be shared.

Unlike the standard-library `Lock` and `DataLock` adapters, `StdMonitor`
recovers the inner state instead of rejecting access after poisoning. A panic
while the state lock is held can leave that state partially modified.
`is_poisoned` reports whether this has happened; ordinary `lock`, `with_read`,
`with_write`, and wait operations remain available but do not clear the marker.
After inspecting and, when necessary, repairing the protected invariant, call
`clear_poison` to explicitly accept the recovered state. `clear_poison` only
clears the marker: it neither validates nor rolls back state, and a later panic
while holding the monitor can poison it again. `Arc<StdMonitor<T>>` exposes
both methods through deref coercion.

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

### `ParkingLotMonitorGuard` and `StdMonitorGuard`

`ParkingLotMonitor::lock` returns `ParkingLotMonitorGuard`;
`StdMonitor::lock` returns `StdMonitorGuard`. Both dereference to the state and
support:

- `wait`, which releases the state lock, waits, and reacquires it.
- `wait_for` and `wait_until`, which update the guard in place.
- consuming `notify_one` and `notify_all`, which release the guard before
  notification.

Prefer predicate helpers on the monitor. Use guards when an algorithm must
perform several state transitions while explicitly controlling the lock:

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

This type requires `async-monitor`. Use `TokioMonitor<T>` for task-local
ownership and `Arc<TokioMonitor<T>>` for cloneable shared ownership.

- `current` captures the current Tokio runtime handle for the default timer.
- `try_current` reports a missing ambient runtime instead of panicking.
- `with_timer` injects an explicit timer.
- `with_read_async`, `with_write_async`, and the combined
  `with_write_notify_*_async` methods access state.
- `wait_until_async` / `wait_while_async` and their `_for_async` variants wait
  on predicates; `wait_until_ready_async` and
  `wait_until_ready_for_async` provide action-free forms.

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

The captured target runtime must stay alive, have time enabled, and keep
running until a timed wait completes. A timed future may be polled from another
runtime context; the timer still belongs to the captured or injected runtime.

## 9. Waiting, notification, and timeout semantics

### Choose the timeout form

| Need | Use |
| --- | --- |
| One condition-wait budget after initial state-lock acquisition | `*_for` or `*_for_async` |
| One caller-coordinated absolute monotonic deadline | `*_with_deadline` or `*_with_deadline_async` |
| One blocking operation-wide budget that includes initial lock contention | `*_with_total_timeout` |
| A guard-level wait until an absolute deadline | `guard.wait_until(deadline)` |

`*_with_deadline` accepts a caller-supplied `MonotonicInstant`; use it when
several operations must share one cutoff. Async deadline futures are lazy, but
their supplied deadline is not reset on first poll. `*_with_total_timeout` is
available only to blocking monitors. Guard `wait_until` takes a deadline, not
a readiness predicate; inspect the protected state after it returns.

The async deadline forms are `wait_until_with_deadline_async`,
`wait_until_ready_with_deadline_async`, and
`wait_while_with_deadline_async`.

### Notifications are memoryless

`notify_one` selects at most one already registered waiter. A notification
sent when no waiter is registered has no future effect. `notify_all` affects
the currently registered waiters. Neither operation makes a predicate true or
guarantees fairness.

Predicates and callbacks execute while the monitor state lock is held. Their
panics propagate. If a callback passed to `with_write_notify_*` panics, the
monitor sends no notification.

A wakeup, including a spurious wakeup, only causes another locked predicate
check. Always store readiness in state and let the predicate inspect it.

### External predicate state needs the same handshake

If a predicate reads state outside the monitor, such as an atomic, an update
that may make it ready must still take part in the monitor-lock handshake.
Atomic ordering alone cannot stop a notification from falling between the
predicate check and waiter registration:

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

The asynchronous protocol is the same; use the combined async helper so the
update cannot straddle waiter registration:

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

### Timeout budget

A relative timeout is a condition-wait budget, aligned with
`std::sync::Condvar::wait_timeout_while`. It is not a hard deadline for the
whole method call: initial state-lock contention is excluded, and the action
after readiness is excluded.

After acquiring the state lock and before the first predicate check, the
monitor samples the start time and derives one fixed absolute deadline. The
first predicate check, every later predicate check, waiter registration, and
all waiting consume this budget. Notifications and spurious wakeups never
restart it. A timed wait may return after the timeout while reacquiring the
state lock, exactly as a condition variable does.

A zero timeout still performs the initial predicate check. At the deadline, a
final predicate check under the lock wins over a successful timer completion.
Timer registration or completion errors take precedence over post-wait
readiness, and the action is not run.

For blocking code, `wait_while_with_total_timeout`,
`wait_until_with_total_timeout`, and
`wait_until_ready_with_total_timeout` fix their absolute deadline before
initial state-lock acquisition. Lock contention therefore consumes the same
operation-wide budget as predicate evaluation and waiting. These methods are
still not hard return-time guarantees: reaching the deadline cannot interrupt
mutex acquisition or reacquisition, and the ready action runs without a time
limit.

Async wait futures are lazy: time before the first poll does not consume the
budget, and initial async state-lock contention is excluded. Dropping a
pending future unregisters its waiter and does not run the action. Cancellation
does not roll back protected-state changes made by other tasks. If `notify_one`
already selected that waiter, cancellation discards the selection; it is not
transferred to another or future waiter.

## 10. Wait results and errors

Predicate-based timed waits return:

```text
Result<WaitTimeoutResult<R>, qubit_clock::TimeError>
```

`WaitTimeoutResult::Ready(R)` contains the action result.
`WaitTimeoutResult::TimedOut` means the predicate was still false after the
final locked check.

Guard-level timed waits return `WaitTimeoutStatus`:

- `Woken` means the wait returned before the deadline, possibly because of a
  notification or a spurious wakeup.
- `TimedOut` means the deadline was reached.

Callers using a guard must still inspect protected state after either status.
A `TimeError` identifies timer registration or completion failure rather than
a real timeout. The guard remains held and usable after such an error.

`WaitTimeoutResult` provides `is_ready`, `is_timed_out`, `into_option`, and
`map`; `WaitTimeoutStatus` provides `is_woken` and `is_timed_out`.

## 11. Deterministic time in tests

Every concrete monitor provides `with_timer`. Tests can inject a
`ManualTimer` from `qubit-clock` into the same monitor type used in production,
so the production wait algorithm runs without real sleeps.

Declare the test clock directly:

```toml
[dev-dependencies]
qubit-clock = { version = "0.12", features = ["test-util"] }
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

The clock's waiter and deadline observers coordinate advancement after
registration instead of guessing with a real sleep. Monitor and timer
registrations are cancellation-safe, and multiple components can share one
manual clock domain. Tokio monitors use the same injection design.

## 12. Choosing components and avoiding mistakes

### Selection guide

| Need | Start with |
| --- | --- |
| Abstract one acquisition mode | `Lock` |
| Require truly exclusive acquisition | `ExclusiveLock` |
| Read or mutate data inside a lock | `DataLock<T>` |
| Preserve shared and exclusive modes | `ReadWriteLock` |
| Use Tokio locks | The corresponding `Async*` component |
| Coordinate blocking predicate waits | `ParkingLotMonitor` or `StdMonitor` |
| Coordinate Tokio predicate waits | `TokioMonitor` |
| Clone a monitor handle | `Arc<ParkingLotMonitor<T>>`, `Arc<StdMonitor<T>>`, or `Arc<TokioMonitor<T>>` |
| Test deadlines without sleeping | `with_timer` and `ManualMonotonicClock` |
| Express a generic monitor dependency | The narrowest capability trait |

### Common mistakes

- Treating a notification as stored state. Store readiness in protected state.
- Mutating predicate state outside the monitor-lock handshake.
- Calling raw `notify_*` after a separate unlocked update when a combined
  `with_write_notify_*` helper fits.
- Doing slow work, blocking I/O, or unrelated callbacks while holding a lock.
- Re-entering the same monitor from a monitor closure; this can deadlock.
- Assuming `notify_one` is fair or that cancellation transfers its selection.
- Forgetting `async-lock`, `monitor`, `parking-lot`, or `async-monitor` for a
  gated component.
- Requiring a broad aggregate trait when `Notifier` or a waiter trait is
  sufficient.

For exact method signatures and backend-specific trait implementations, see
the crate's [API documentation](https://docs.rs/qubit-lock).
