# rs-lock Composability and Hardening Design

## Goal

Improve `rs-lock`'s downstream composability, feature-matrix coverage, mock
monitor genericity, timeout-edge correctness, documentation consistency, and
Tokio cancellation observability without broadening the asynchronous blanket
implementation surface.

## Scope

- Configure docs.rs to build every feature and add an `async + mock` CI matrix
  entry whose documentation build denies warnings.
- Implement synchronous `Arc<L>` delegation for `Lock<T>` and blocking monitor
  capabilities (`Notifier`, `ConditionWaiter`, and
  `TimeoutConditionWaiter`). Existing named `Arc*` wrappers remain supported.
- Remove unnecessary `Send + 'static` bounds from synchronous `MockMonitor<T>`
  and `ArcMockMonitor<T>` APIs. Async trait implementations require `T: Send`
  but not `T: 'static`.
- Make `wait_for_timeout_waiters` recognize an already-satisfied waiter count
  before rejecting an overflowing real-time deadline, and keep the
  `ArcMockMonitor` documentation aligned.
- Add a Criterion benchmark that measures dropping registered Tokio condition
  wait futures at multiple registry sizes. Change the registry only if the
  benchmark demonstrates material superlinear cancellation cost.
- Add the eight agreed forwarding/default-method inline annotations and rename
  four `# Parameters` rustdoc headings to `# Arguments`.
- Keep the seven deterministic inline white-box regression tests in place as
  an explicit exception; do not expose private hooks or move those tests.

## API Design

`Arc<L>` implements `Lock<T>` whenever `L: Lock<T>`. The implementation
delegates all four methods through `Arc::as_ref` and preserves the inner
implementation's error and poisoning behavior.

Blocking monitor traits use independent `Arc<M>` blanket implementations so a
downstream monitor can become a shared handle by wrapping it in `Arc` without
requiring a crate-specific wrapper. No blanket implementations are added for
`Box`, references, async lock traits, or async monitor traits.

`MockMonitor<T>` construction and blocking operations accept non-`Send` and
borrowed state. The futures returned by its async implementations remain
`Send`; consequently those implementations retain `T: Send`, while the
unnecessary `'static` requirement is removed.

## Correctness and Performance

The timeout-waiter readiness check occurs while holding the state lock and
before an overflowing `Instant` deadline can produce `false`. If readiness is
not already satisfied, overflow still returns `false`.

The Tokio benchmark polls owned wait futures once so they register, then times
their cancellation by dropping them. Registry sizes are varied to expose
linear-removal amplification. The current registry is retained unless fresh
benchmark evidence shows clear superlinear growth.

## Verification

Behavior changes follow red-green TDD with focused external tests. Config and
rustdoc-only changes use compile/doc/CI validation under the user's approved
exception. Final validation runs `./align-ci.sh`, then `./ci-check.sh`; coverage
is run as `./coverage.sh json` only if CI reports coverage below the required
threshold.
