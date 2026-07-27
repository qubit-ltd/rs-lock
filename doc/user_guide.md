# Qubit Lock User Guide

[中文版](user_guide.zh_CN.md)

`qubit-lock` provides a common vocabulary for synchronous locks, Tokio locks,
and monitor-style condition coordination. This guide starts with the problem
the crate solves, then explains how to choose and use each public component.

## 1. The problem this crate solves

Rust already has excellent lock implementations, but application and library
code still runs into three recurring problems:

1. `std`, `parking_lot`, and Tokio expose different concrete APIs. Generic
   code that only needs "acquire a lock" or "read protected data" should not
   have to know which backend the caller chose.
2. A lock protects state, but it does not by itself express "sleep until this
   predicate is true." Hand-written condition-variable code can lose
   notifications when state updates, predicate checks, and waiter registration
   do not follow one protocol.
3. Real sleeps make timeout tests slow and flaky. A test should run the
   production wait algorithm while controlling time deterministically.

`qubit-lock` addresses these problems with backend-independent lock traits,
closure-based data access, synchronous and asynchronous monitors, and
injectable timers.

### A motivating example: a one-item work queue

The worker must sleep while the queue is empty, then remove an item while
holding the same lock that protects the predicate. The producer must update
the queue and notify a registered waiter without leaving a lost-notification
window:

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

`with_write_notify_one` performs the state-update-and-notify handshake.
`wait_until` checks the predicate under the monitor lock, registers the waiter
when necessary, and checks again after wakeup. The queue state is the source
of truth; the notification is only a prompt to check it again.

## 2. Installation and feature selection

The default configuration provides the complete synchronous API:

```toml
[dependencies]
qubit-lock = "0.11"
```

Choose features according to the components you use:

| Feature | What it enables |
| --- | --- |
| no optional features | Synchronous lock traits and `std` lock implementations |
| `parking-lot` | Implementations for `parking_lot` mutexes and read-write locks |
| `monitor` | Monitor traits, std monitors, timed waits, and timer injection |
| `async-lock` | Tokio lock traits and adapters |
| `async-monitor` | `async-lock`, monitor support, and Tokio monitors |
| default | `monitor` and `parking-lot` |

Lock-only users can avoid all optional dependencies:

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false }
```

Enable asynchronous locks without Tokio monitor deadlines:

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false, features = ["async-lock"] }
```

Enable Tokio monitor coordination and timed waits:

```toml
[dependencies]
qubit-lock = { version = "0.11", default-features = false, features = ["async-monitor"] }
```

If the application creates a Tokio runtime, enable the required runtime
features, such as `rt` or `rt-multi-thread`, in the application's own
`Cargo.toml`.

All public `qubit-lock` types are imported from the crate root.

## 3. Synchronous lock components

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

## 4. Asynchronous lock components

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

## 5. Monitor capability components

A monitor owns protected state and coordinates predicate waits with
notifications. Application code should normally select a concrete monitor.
Use the capability traits at generic API boundaries:

| Component | Capability |
| --- | --- |
| `Notifier` | `notify_one` and `notify_all` only |
| `ConditionWaiter` | Synchronous `wait_until`, `wait_until_ready`, and `wait_while` |
| `TimeoutConditionWaiter` | Synchronous `wait_until_for`, `wait_until_ready_for`, and `wait_while_for` |
| `Monitor` | State access, notification, and untimed synchronous waits |
| `TimedMonitor` | `Monitor` plus timed synchronous waits |
| `SharedMonitor` | A cloneable shared synchronous monitor handle |
| `AsyncConditionWaiter` | Asynchronous untimed predicate waits |
| `AsyncTimeoutConditionWaiter` | Asynchronous timed predicate waits |
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

## 6. Concrete monitor components

### `ParkingLotMonitor<T>` and `ArcParkingLotMonitor<T>`

Use `ParkingLotMonitor<T>` for efficient blocking coordination when the
`parking-lot` and `monitor` features are enabled. Use
`ArcParkingLotMonitor<T>` when the handle must be cloned or retained by
multiple threads.

Important methods are:

- `new` and `with_timer` for construction.
- `with_read` and `with_write` for state access.
- `with_write_notify_one` and `with_write_notify_all` for the normal
  state-update-and-notify protocol.
- `wait_until` / `wait_while`, action-free `wait_until_ready`, and their
  `_for` timed variants.
- `lock` when explicit guard-level control is necessary.

The `Arc*` wrapper dereferences to its inner monitor. `from_arc`, `as_arc`, and
`into_arc` expose the ownership boundary without another allocation.

### `StdMonitor<T>` and `ArcStdMonitor<T>`

`StdMonitor<T>` has the same high-level API and uses standard-library
primitives. Choose it when avoiding the parking_lot dependency matters more
than using that backend. `ArcStdMonitor<T>` is its cloneable shared handle and
also provides `from_arc`, `as_arc`, and `into_arc`.

Unlike the standard-library `Lock` and `DataLock` adapters, `StdMonitor`
recovers the inner state instead of rejecting access after poisoning. A panic
while the state lock is held can leave that state partially modified.
`is_poisoned` reports whether this has happened; ordinary `lock`, `with_read`,
`with_write`, and wait operations remain available but do not clear the marker.
After inspecting and, when necessary, repairing the protected invariant, call
`clear_poison` to explicitly accept the recovered state. `clear_poison` only
clears the marker: it neither validates nor rolls back state, and a later panic
while holding the monitor can poison it again. `ArcStdMonitor` exposes both
methods through its inner monitor.

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

### `TokioMonitor<T>` and `ArcTokioMonitor<T>`

These types require `async-monitor`. Use `TokioMonitor<T>` for task-local
ownership and `ArcTokioMonitor<T>` for cloneable shared ownership.

- `current` captures the current Tokio runtime handle for the default timer.
- `try_current` reports a missing ambient runtime instead of panicking.
- `with_timer` injects an explicit timer.
- `with_read_async`, `with_write_async`, and the combined
  `with_write_notify_*_async` methods access state.
- `wait_until_async` / `wait_while_async` and their `_for_async` variants wait
  on predicates.

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

The captured target runtime must stay alive, have time enabled, and keep
running until a timed wait completes. A timed future may be polled from another
runtime context; the timer still belongs to the captured or injected runtime.

## 7. Waiting, notification, and timeout semantics

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

The asynchronous protocol is the same; use the combined async helper so the
update cannot straddle waiter registration:

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
            .wait_until_async(
                |_| waiter_ready.load(Ordering::Acquire),
                |_| (),
            )
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

A relative timeout is the condition-wait budget. Initial state-lock contention
and the initial predicate check are excluded. If the predicate is not ready,
the monitor establishes one fixed deadline immediately before the first wait
and reuses it across wakeups.

A zero timeout still performs the initial predicate check. At the deadline, a
final predicate check under the lock wins over a successful timer completion.
Timer registration or completion errors take precedence over post-wait
readiness, and the action is not run.

Async wait futures are lazy: time before the first poll does not consume the
budget. Dropping a pending future unregisters its waiter and does not run the
action. Cancellation does not roll back protected-state changes made by other
tasks. If `notify_one` already selected that waiter, cancellation discards the
selection; it is not transferred to another or future waiter.

## 8. Wait results and errors

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

## 9. Deterministic time in tests

Every concrete monitor provides `with_timer`. Tests can inject a
`ManualTimer` from `qubit-clock` into the same monitor type used in production,
so the production wait algorithm runs without real sleeps.

Declare the test clock directly:

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

The clock's waiter and deadline observers coordinate advancement after
registration instead of guessing with a real sleep. Monitor and timer
registrations are cancellation-safe, and multiple components can share one
manual clock domain. Tokio monitors use the same injection design.

## 10. Choosing components and avoiding mistakes

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
| Clone a monitor handle | The corresponding `Arc*Monitor` |
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
