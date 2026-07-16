# Monitor Semantics Merge-Readiness Plan

> **For Codex:** Execute this plan in the existing
> `rs-lock-monitor-semantics` worktree. Do not commit, merge, or push without
> separate user authorization.

**Goal:** Prepare the approved monitor-semantics redesign for a clean merge
into the current `dev-starfish` branch without changing production behavior or
public APIs.

**Architecture:** Keep public contract tests in `tests/monitor/`. Retain only
the seven deterministic race regressions that require private synchronization
hooks as inline white-box tests: four for `TokioMonitor` and three for
`MockMonitor`. Document that narrow exception beside the style directive.

**Tech Stack:** Rust 2024, Tokio 1.52, Cargo integration tests, the repository's
nightly rustfmt configuration, and the existing `ci-check.sh` validation flow.

---

## Task 1: Align branch history with the current integration branch

**Files:** None

1. Rebase commits after the duplicate design-spec commit onto `dev-starfish`:

   ```bash
   git rebase --onto dev-starfish bff6863
   ```

2. Verify that the branch is based on `dev-starfish` and the duplicate commit
   is no longer part of the candidate-only range.

## Task 2: Move public Tokio behavior tests to integration tests

**Files:**

- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `tests/monitor/tokio_monitor_tests.rs`

1. Move these tests without changing their assertions:

   - `test_tokio_monitor_notify_one_without_waiter_is_not_retained`
   - `test_tokio_monitor_notify_all_selects_registered_waiters_only`
   - `test_tokio_monitor_cancelled_selected_waiter_discards_notification`
   - `test_tokio_monitor_timeout_duration_max_does_not_overflow`

2. Add the minimal manual-poll helper types to the integration test file.

3. Run the focused integration test target:

   ```bash
   cargo +1.94.0 test --all-features --test mod monitor::tokio_monitor_tests
   ```

## Task 3: Move monitor regressions to external tests

**Files:**

- Modify: `src/monitor/tokio_monitor.rs`
- Modify: `src/monitor/mock_monitor.rs`

1. Re-express Tokio waiter and deadline regressions through public monitor
   behavior in `tests/monitor/tokio_monitor_tests.rs`.

2. Re-express Mock timeout and lock-contention regressions through public
   behavior in `tests/monitor/mock_monitor_tests.rs`.

3. Remove the inline-test exemptions and private test-only synchronization
   hooks from production source files.

## Task 4: Synchronize design and verification documentation

**Files:**

- Modify: `docs/superpowers/specs/2026-07-15-monitor-semantics-redesign.md`
- Modify: `docs/superpowers/plans/2026-07-15-monitor-semantics-redesign.md`

1. Describe the final Tokio design: an explicit waiter registry and one private
   `Notify` per waiter, registered while holding the state lock.

2. Remove stale references to a shared `Notify::notified().enable()` solution.

3. Replace generic `cargo fmt --all` commands with the repository's canonical
   pinned-nightly command:

   ```bash
   cargo +nightly-2026-06-05 fmt -- --check --config-path .rs-ci/rustfmt.toml
   ```

## Task 5: Verify merge readiness

**Files:** Verify all changed files

1. Run format and diff checks.
2. Run Clippy with all features and all targets.
3. Run the complete feature test matrix and doctests.
4. Run the repository style checker and coverage gate.
5. Use `git merge-tree` to verify that merging into `dev-starfish` has no
   unresolved conflicts.
6. Record the known packaging limitation separately if crates.io still lacks
   the required `qubit-clock` version; do not misclassify it as a branch
   regression.
