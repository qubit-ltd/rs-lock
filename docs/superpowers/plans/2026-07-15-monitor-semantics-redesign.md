# Monitor Semantics Redesign Implementation Plan

> **For Codex:** Execute this plan test-first, keeping each regression red before
> changing the corresponding production path. Do not create compatibility
> shims for the removed APIs.

**Goal:** Give every `rs-lock` monitor the approved memoryless notification and
condition-wait timeout semantics while simplifying the async and Arc-backed
APIs.

**Architecture:** Blocking monitors continue to use their native condition
variables. `TokioMonitor` keeps an explicit registry of waiter-owned
`tokio::sync::Notify` signals and registers each waiter before releasing the
state mutex. `MockMonitor` keeps its per-waiter registry while moving its
clock-change epoch outside protected user state so clock callbacks never
reacquire the monitor mutex. Async traits use RPITIT concrete futures instead
of boxed trait-object futures.

**Tech Stack:** Rust 2024, `std::sync`, `parking_lot`, Tokio 1.52,
`qubit-clock::ManualMonotonicClock`, Cargo integration tests and rustdoc tests.

---

## Task 1: Add red regressions for the two correctness defects

**Files:**

- Modify: `tests/monitor/tokio_monitor_tests.rs`
- Modify: `tests/monitor/mock_monitor_tests.rs`

**Step 1: Add the Tokio register-before-unlock regression**

Add a multi-thread Tokio test that coordinates a condition waiter and producer
through the protected-state predicate, forces producer contention on the state
mutex, and repeats the handoff enough times to expose the window between
dropping the state guard and polling `Notified`. Each state transition followed
by `notify_one` must let the logically registered waiter finish within a bounded
real timeout.

**Step 2: Run the focused Tokio regression and observe failure**

Run:

```bash
cargo test --all-features --test mod monitor::tokio_monitor_tests::test_tokio_monitor_notify_one_does_not_lose_registered_condition_waiter -- --exact --nocapture
```

Expected: the pre-fix implementation eventually times out because a producer
can notify after the state lock is released but before the `Notified` future is
registered.

**Step 3: Add the Mock clock reentrancy regression**

Create a monitor and shared manual clock, invoke `clock.advance(...)` inside
`monitor.with_write(...)` on a spawned thread, and observe completion through a
bounded channel receive. The test must fail promptly rather than wait forever.

**Step 4: Run the focused Mock regression and observe failure**

Run:

```bash
cargo test --all-features --test mod monitor::mock_monitor_tests::test_mock_monitor_clock_can_advance_inside_state_closure -- --exact --nocapture
```

Expected: the bounded receive times out because the clock callback tries to
relock the monitor state.

## Task 2: Fix Tokio waiter registration and document notification semantics

**Files:**

- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `src/monitor/notifier.rs`
- Modify: `tests/monitor/tokio_monitor_tests.rs`

**Step 1: Register a private condition notification before unlock**

In untimed and timed predicate waits:

- create one waiter-owned `Notify` and add it to the monitor registry while
  holding the state guard;
- drop the state guard only after registration;
- have `notify_one` remove and signal at most one registered waiter and
  `notify_all` remove and signal all registered waiters;
- await only the private signal, or poll it together with the fixed deadline.

Do not add stored-permit behavior at the monitor API level.

**Step 2: Document the common contract**

Update `Notifier`, `TokioMonitor`, and relevant wait method documentation to
state that notifications are memoryless, select already registered waiters,
require predicate rechecks, and provide no fairness guarantee.

Document Tokio cancellation behavior and the timer-driver requirement for
timed waits that reach suspension.

**Step 3: Run the Tokio regression**

Run the focused command from Task 1. Expected: pass consistently.

## Task 3: Remove notification-only waiting APIs

**Files:**

- Delete: `src/monitor/notification_waiter.rs`
- Delete: `src/monitor/timeout_notification_waiter.rs`
- Delete: `src/monitor/async_notification_waiter.rs`
- Delete: `src/monitor/async_timeout_notification_waiter.rs`
- Modify: `src/monitor/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/monitor/monitor.rs`
- Modify: `src/monitor/async_monitor.rs`
- Modify: `src/monitor/std_monitor.rs`
- Modify: `src/monitor/parking_lot_monitor.rs`
- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `src/monitor/mock_monitor.rs`
- Modify: `src/monitor/arc_std_monitor.rs`
- Modify: `src/monitor/arc_parking_lot_monitor.rs`
- Modify: `src/monitor/arc_tokio_monitor.rs`
- Modify: `src/monitor/arc_mock_monitor.rs`
- Modify: `tests/monitor/std_monitor_tests.rs`
- Modify: `tests/monitor/parking_lot_monitor_tests.rs`
- Modify: `tests/monitor/tokio_monitor_tests.rs`
- Modify: `tests/monitor/mock_monitor_tests.rs`
- Modify: `tests/monitor/arc_std_monitor_tests.rs`
- Modify: `tests/monitor/arc_parking_lot_monitor_tests.rs`
- Modify: `tests/monitor/arc_tokio_monitor_tests.rs`
- Modify: `tests/monitor/arc_mock_monitor_tests.rs`
- Modify: `tests/monitor/monitor_trait_tests.rs`
- Modify: `README.md`
- Modify: `README.zh_CN.md`

**Step 1: Change aggregate trait expectations in tests**

Update generic Monitor/AsyncMonitor tests so the aggregate contract requires
only notifier plus predicate condition-wait capabilities. Remove tests that
assert notification-only waiting or queued wake behavior.

**Step 2: Delete the four traits and public exports**

Remove the source modules and all re-exports. Simplify:

- `Monitor = Notifier + TimeoutConditionWaiter`
- `AsyncMonitor = Notifier + AsyncTimeoutConditionWaiter`

Remove all corresponding inherent methods and trait implementations from the
four concrete monitors and four Arc wrappers. Guard-level `wait` and timeout
methods remain unchanged.

**Step 3: Update tests and README migration notes**

Remove obsolete notification-only tests and imports. Replace any legitimate
coordination assertion with predicate-based `wait_until`/`wait_while` tests.
Update English and Chinese README API-change notes to explain that queued
permit needs belong to semaphore/event primitives.

**Step 4: Run monitor tests**

Run:

```bash
cargo test --all-features --test mod monitor:: -- --nocapture
```

Expected: all remaining monitor tests pass.

## Task 4: Replace boxed async monitor futures with RPITIT

**Files:**

- Delete: `src/monitor/async_monitor_future.rs`
- Modify: `src/monitor/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/monitor/async_condition_waiter.rs`
- Modify: `src/monitor/async_timeout_condition_waiter.rs`
- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `src/monitor/mock_monitor.rs`
- Modify: `src/monitor/arc_tokio_monitor.rs`
- Modify: `src/monitor/arc_mock_monitor.rs`
- Modify: `tests/monitor/tokio_monitor_tests.rs`
- Modify: `tests/monitor/mock_monitor_tests.rs`

**Step 1: Add compile-time Send assertions for returned futures**

Extend async monitor tests with a helper accepting `impl Future + Send` and
pass futures returned by condition and timeout-condition trait methods.

**Step 2: Run the focused async tests**

Run:

```bash
cargo test --all-features --test mod monitor::tokio_monitor_tests::
cargo test --all-features --test mod monitor::mock_monitor_tests::
```

Expected: existing boxed futures satisfy Send, establishing the public bound
before changing representation.

**Step 3: Convert async traits and implementations**

Return `impl Future<Output = ...> + Send + 'a` from async trait methods and
implementations. Return `async move` blocks directly, remove every `Box::pin`,
and delete the `AsyncMonitorFuture` alias/module/export.

Ensure default `wait_until_async` and `wait_until_for_async` methods preserve
the Send and lifetime bounds.

**Step 4: Run all async monitor tests**

Run:

```bash
cargo test --all-features --test mod monitor:: -- --nocapture
```

Expected: pass without heap-boxed monitor futures.

## Task 5: Unify condition-wait timeout start and deadline behavior

**Files:**

- Modify: `src/monitor/async_timeout_condition_waiter.rs`
- Modify: `src/monitor/timeout_condition_waiter.rs`
- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `src/monitor/mock_monitor.rs`
- Modify: `tests/monitor/tokio_monitor_tests.rs`
- Modify: `tests/monitor/mock_monitor_tests.rs`
- Modify: `tests/monitor/std_monitor_tests.rs`
- Modify: `tests/monitor/parking_lot_monitor_tests.rs`

**Step 1: Replace call-time tests with condition-wait-budget regressions**

For Tokio and Mock monitors, construct an async timed condition wait, delay
polling while advancing real/mock time beyond the requested duration, then
poll it with a still-blocking predicate. Assert that it receives a fresh budget
and does not time out immediately.

Add or retain tests covering:

- initial state-lock contention does not consume the budget;
- repeated notifications reuse one fixed deadline;
- a predicate that is ready on the final locked check wins over timeout;
- zero timeout checks the predicate once;
- Std and parking_lot agree with the same contract.

**Step 2: Run the new Tokio and Mock timeout regressions and observe failure**

Run:

```bash
cargo test --all-features --test mod uses_condition_wait_budget -- --nocapture
```

Expected: old Tokio and Mock call-time deadline tests fail under the new
expectation.

**Step 3: Move deadline creation behind the initial predicate check**

In Tokio, establish one `Instant`/deadline only after locking state and seeing a
blocking predicate. In Mock, read the manual clock and compute one target at the
same point. Reuse that deadline/target for all wake-recheck cycles.

On timeout, reacquire the state and make a final predicate decision before
returning `TimedOut`.

**Step 4: Run focused and cross-implementation timeout tests**

Run:

```bash
cargo test --all-features --test mod timeout -- --nocapture
```

Expected: all timeout tests pass with the unified contract.

## Task 6: Decouple Mock clock callbacks from protected state

**Files:**

- Modify: `src/monitor/mock_monitor.rs`
- Modify: `src/monitor/mock_monitor_waiter_guard.rs`
- Modify: `tests/monitor/mock_monitor_tests.rs`
- Modify: `tests/monitor/arc_mock_monitor_tests.rs`

**Step 1: Move the change epoch outside `MockMonitorState<T>`**

Add shared `Arc<AtomicU64>` change state to `MockMonitor`. The clock callback
increments the atomic and signals blocking/async change channels without
locking `MockMonitorState<T>`. Notification paths increment the same epoch
after assigning waiter notifications.

Use an ordering sufficient for unique monotonic epoch publication; protected
predicate state remains synchronized by the monitor mutex.

**Step 2: Remove inactive pre-registration behavior**

With notification-only futures removed, simplify waiter state and RAII helpers
so async condition waiters register only when polled under the state lock.
Cancellation still unregisters the active waiter and decrements timeout-waiter
counts exactly once.

**Step 3: Run Mock regressions**

Run:

```bash
cargo test --all-features --test mod mock_monitor -- --nocapture
```

Expected: clock advancement inside state closures completes and all sync/async
manual-time tests pass.

## Task 7: Simplify Arc-backed monitor wrappers

**Files:**

- Modify: `src/monitor/arc_std_monitor.rs`
- Modify: `src/monitor/arc_parking_lot_monitor.rs`
- Modify: `src/monitor/arc_tokio_monitor.rs`
- Modify: `src/monitor/arc_mock_monitor.rs`
- Modify: `tests/monitor/arc_std_monitor_tests.rs`
- Modify: `tests/monitor/arc_parking_lot_monitor_tests.rs`
- Modify: `tests/monitor/arc_tokio_monitor_tests.rs`
- Modify: `tests/monitor/arc_mock_monitor_tests.rs`
- Modify: `tests/monitor/monitor_trait_tests.rs`

**Step 1: Add ownership-boundary tests**

For each wrapper, test construction from an existing `Arc<InnerMonitor<T>>`,
cloning through the wrapper, conversion back into `Arc`, pointer identity, and
normal method resolution through `Deref`.

**Step 2: Add `from_arc`, `as_arc`, and `into_arc`**

Provide documented ownership conversion methods on all four wrappers.

**Step 3: Remove redundant inherent forwarding methods**

Delete inherent forwarding for lock/read/write/wait/notify operations. Retain
constructors, clock access needed by Mock, ownership conversions, `AsRef`,
`Deref`, `Clone`, `From`, `Default`, and explicit notifier/condition trait
implementations needed by generic bounds.

Update tests to invoke ordinary operations through `Deref` and trait UFCS where
the generic contract itself is under test.

**Step 4: Run Arc monitor tests**

Run:

```bash
cargo test --all-features --test mod arc_ -- --nocapture
```

Expected: all Arc monitor tests pass with the smaller inherent API.

## Task 8: Add must-use diagnostics and finish public documentation

**Files:**

- Modify: `src/monitor/wait_timeout_result.rs`
- Modify: `src/monitor/wait_timeout_status.rs`
- Modify: `src/monitor/condition_waiter.rs`
- Modify: `src/monitor/timeout_condition_waiter.rs`
- Modify: `src/monitor/async_condition_waiter.rs`
- Modify: `src/monitor/async_timeout_condition_waiter.rs`
- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `README.md`
- Modify: `README.zh_CN.md`
- Modify: `tests/docs/readme_tests.rs`

**Step 1: Add `#[must_use]` to timeout result types**

Annotate both timeout enums. Add rustdoc compile-fail examples using
`#![deny(unused_must_use)]` so silently dropping either result is rejected.

**Step 2: Align all timeout and cancellation documentation**

Describe the condition-wait budget, initial lock exclusion, final predicate
check, async laziness, Tokio runtime timer requirement, and cancellation
behavior consistently across traits and implementations.

Update both READMEs and their content assertions where necessary.

**Step 3: Run documentation tests**

Run:

```bash
cargo test --all-features --doc
```

Expected: normal examples and compile-fail examples pass.

## Task 9: Full verification and review

**Files:**

- Verify all changed files

**Step 1: Format and inspect the diff**

Run:

```bash
cargo +nightly-2026-06-05 fmt -- --check --config-path .rs-ci/rustfmt.toml
git diff --check
```

If formatting is required, run
`cargo +nightly-2026-06-05 fmt -- --config-path .rs-ci/rustfmt.toml`, then rerun
both checks.

**Step 2: Run the complete feature matrix**

Run:

```bash
cargo test
cargo test --features async
cargo test --features mock
cargo test --all-features
cargo test --all-features --doc
```

Expected: every test and doctest passes in every supported feature set.

**Step 3: Run Clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: no warnings.

**Step 4: Request independent code review**

Ask a reviewer agent to compare the implementation against the approved design
and inspect notification races, timeout edges, cancellation cleanup, API
exports, docs, and test determinism. Address all validated findings and rerun
the affected focused tests plus the full verification commands.

**Step 5: Report completion without committing**

Summarize breaking API changes, semantics, regression coverage, verification
evidence, and remaining risks. Do not stage, commit, push, or open a PR unless
the user explicitly asks.
