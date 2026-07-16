# rs-lock Composability and Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make synchronous lock and monitor traits compose through `Arc`, relax mock-state bounds, correct timeout waiter readiness, strengthen feature documentation coverage, and measure Tokio cancellation scaling.

**Architecture:** Add narrow `Arc` blanket delegations only to synchronous traits, retain existing wrapper types, and separate synchronous mock genericity from async `Send` requirements. Use focused external regression tests and a standalone Criterion benchmark; optimize the Tokio registry only when benchmark data demonstrates superlinear cancellation.

**Tech Stack:** Rust 2024, Rust 1.94, Cargo features, Tokio 1.52, Criterion, parking_lot, qubit-clock.

## Global Constraints

- Preserve all existing public wrapper types and public API paths.
- Add no `Box`, reference, `AsyncLock`, or async monitor blanket implementations.
- Async mock futures remain `Send`; async mock state requires `T: Send` but not `T: 'static`.
- Keep the seven deterministic inline white-box tests in their source files.
- Do not change the Tokio registry without fresh benchmark evidence.
- Do not execute `git add`, `git commit`, or `git push` without explicit user authorization.

---

### Task 1: Feature and docs.rs coverage

**Files:**
- Modify: `Cargo.toml`
- Modify: `.rs-ci-cargo-matrix.json`

**Interfaces:**
- Consumes: Cargo features `async` and `mock`.
- Produces: docs.rs all-feature metadata and an `async-mock` warning-denied doc matrix entry.

- [ ] Add `[package.metadata.docs.rs] all-features = true`.
- [ ] Add the `async-mock` feature combination with `test`, `doc`, and `clippy` commands.
- [ ] Run the combined-feature doc command with warnings denied.

### Task 2: Synchronous `Arc` blanket delegation

**Files:**
- Modify: `tests/lock/lock_tests.rs`
- Modify: `tests/monitor/monitor_trait_tests.rs`
- Modify: `src/lock/lock.rs`
- Modify: `src/monitor/notifier.rs`
- Modify: `src/monitor/condition_waiter.rs`
- Modify: `src/monitor/timeout_condition_waiter.rs`

**Interfaces:**
- Consumes: `Lock<T>`, `Notifier`, `ConditionWaiter`, and `TimeoutConditionWaiter`.
- Produces: corresponding implementations for `Arc<L>` or `Arc<M>` when the inner type implements the trait.

- [ ] Add compile-and-behavior tests passing `Arc<std::sync::Mutex<i32>>` through a generic `Lock<i32>` API and `Arc<ParkingLotMonitor<bool>>` through generic blocking monitor APIs.
- [ ] Run focused tests and verify they fail because the blanket implementations are absent.
- [ ] Add fully-qualified, inline delegations for every required method and associated state type.
- [ ] Rerun focused tests and verify they pass.

### Task 3: Mock monitor generic bounds and timeout overflow

**Files:**
- Modify: `tests/monitor/mock_monitor_tests.rs`
- Modify: `tests/monitor/arc_mock_monitor_tests.rs`
- Modify: `src/monitor/mock_monitor.rs`
- Modify: `src/monitor/arc_mock_monitor.rs`
- Modify: `src/monitor/internal/mock_monitor_waiter_guard.rs`

**Interfaces:**
- Consumes: current mock monitor construction, blocking traits, async traits, and waiter guard.
- Produces: synchronous support for borrowed/non-`Send` state, async support for non-`'static` `Send` state, and readiness-first overflow handling.

- [ ] Add external tests for `Rc<Cell<_>>` state, borrowed `&str` state, a non-`'static` async wait, and `wait_for_timeout_waiters(0, Duration::MAX)`.
- [ ] Run focused tests and verify the generic-bound and overflow regressions fail for the expected reasons.
- [ ] Remove bounds from inherent/synchronous mock implementations and waiter guard; retain only `T: Send` on async trait implementations.
- [ ] Check readiness under the state lock before rejecting an overflowing real-time deadline, and synchronize wrapper rustdoc.
- [ ] Rerun focused mock tests with `mock` and with `async,mock` and verify they pass.

### Task 4: Tokio cancellation benchmark and evidence gate

**Files:**
- Modify: `Cargo.toml`
- Create: `benches/tokio_monitor_cancellation.rs`
- Conditionally modify only with evidence: `src/monitor/tokio_monitor.rs`
- Conditionally modify only with evidence: `src/monitor/internal/tokio_condition_waiter_registration.rs`

**Interfaces:**
- Consumes: `ArcTokioMonitor<bool>` and `AsyncConditionWaiter::wait_until_async`.
- Produces: a Criterion benchmark that registers pending futures and measures their drop cost across multiple registry sizes.

- [ ] Add Criterion as a development dependency and a feature-gated harness-free benchmark target.
- [ ] Poll each owned wait future once before the timed drop so every future has registered.
- [ ] Run the benchmark and record per-size timing and scaling.
- [ ] Retain the current registry if scaling is not materially superlinear; otherwise add a regression-preserving keyed registry change and rerun existing cancellation tests plus the benchmark.

### Task 5: Inline and rustdoc normalization

**Files:**
- Modify: `src/monitor/condition_waiter.rs`
- Modify: `src/monitor/timeout_condition_waiter.rs`
- Modify: `src/monitor/async_condition_waiter.rs`
- Modify: `src/monitor/async_timeout_condition_waiter.rs`
- Modify: `src/monitor/parking_lot_monitor.rs`
- Modify: `src/monitor/std_monitor.rs`
- Modify: `src/monitor/parking_lot_monitor_guard.rs`
- Modify: `src/monitor/std_monitor_guard.rs`
- Modify: `src/monitor/mock_monitor.rs`
- Modify: `src/monitor/arc_mock_monitor.rs`

**Interfaces:**
- Produces: eight agreed forwarding/default-method inline annotations and four standard `# Arguments` headings.

- [ ] Add inline attributes to the four trait default methods and four inherent forwarding methods.
- [ ] Rename the four agreed `# Parameters` headings to `# Arguments` without changing API behavior.
- [ ] Run formatting and focused documentation checks.

### Task 6: Full verification and self-review

**Files:**
- Inspect: all working-tree changes.

**Interfaces:**
- Produces: validated, review-ready uncommitted changes.

- [ ] Run `cargo fmt --check` after formatting any changed Rust files.
- [ ] Run `./align-ci.sh` and inspect any edits it makes.
- [ ] Run `./ci-check.sh` and fix only in-scope failures.
- [ ] Run `./coverage.sh json` only if CI reports coverage below threshold.
- [ ] Inspect `git --no-pager diff --check`, `git status --short`, and the complete diff against this plan.
