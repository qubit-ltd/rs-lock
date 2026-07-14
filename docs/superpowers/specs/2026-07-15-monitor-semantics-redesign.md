# Monitor Semantics Redesign

## Context

`rs-lock` currently exposes condition-based monitor operations together with
notification-only waiting operations. Its `StdMonitor`, `ParkingLotMonitor`,
`TokioMonitor`, and `MockMonitor` implementations do not provide identical
notification and timeout behavior:

- Tokio's `Notify` stores one permit and does not register a `Notified` future
  for `notify_one` until that future is polled or explicitly enabled.
- Mock waiters are tracked individually and therefore provide stronger behavior
  than the Tokio implementation.
- Timeout budgets start at different points across implementations.
- Advancing a mock clock while holding the monitor state lock can deadlock
  because the clock callback locks the same state.

This redesign intentionally permits breaking API changes and does not retain
compatibility shims.

## Goals

- Give `notify_one` and `notify_all` one monitor-oriented meaning across all
  implementations.
- Eliminate the Tokio lost-notification window in condition waits.
- Give relative timeouts one precise meaning across synchronous, asynchronous,
  and manually-clocked monitors.
- Remove heap allocation and dynamic dispatch from asynchronous monitor trait
  return values.
- Make the Mock monitor safe to use when manual time advances inside a state
  closure.
- Reduce duplicate forwarding APIs on Arc-backed monitor wrappers.

## Non-goals

- Providing semaphore, event-count, or queued-permit behavior.
- Guaranteeing waiter fairness or scheduling order.
- Preserving notification-only waiter APIs.
- Preserving source compatibility with version 0.10.

## Notification semantics

Monitor notification is memoryless condition-variable notification:

- `notify_one` selects at most one waiter that is already registered when the
  operation occurs.
- `notify_all` selects all waiters already registered when it occurs.
- A notification with no registered waiter has no future effect.
- Separate `notify_one` calls do not collapse while distinct registered waiters
  are available.
- Waking a waiter does not make its condition true. Every waiter reacquires the
  state lock and rechecks its predicate.
- No implementation guarantees fairness.

For asynchronous code, creating a future is not registration because futures
are lazy. Registration occurs when a polled future reaches its condition wait.
Once an asynchronous condition wait has checked its predicate while holding the
state lock, it must register before releasing that lock.

`TokioMonitor` will create and pin `Notify::notified()`, call `enable()`, and
only then release the state guard. This provides the same atomic
register-and-release boundary as a condition variable and prevents a producer
from changing state and notifying inside the gap.

## API boundary

Notification-only waiting cannot reliably compose with monitor state because
it has no predicate with which to distinguish an old notification from a
relevant state transition. The following traits and corresponding monitor
methods will be removed:

- `NotificationWaiter`
- `TimeoutNotificationWaiter`
- `AsyncNotificationWaiter`
- `AsyncTimeoutNotificationWaiter`
- `wait` and `wait_for` on monitor values
- `wait_async` and `wait_for_async` on monitor values

Guard-level `wait` and timeout operations remain. They operate on an already
held state guard and therefore retain the standard condition-variable
contract.

Users needing queued permits should use a semaphore or a separately designed
event primitive.

## Timeout semantics

A relative timeout is a condition-wait budget:

1. Acquire the monitor state lock.
2. Evaluate the predicate.
3. If the predicate is already false, return success without starting a timer.
4. If the predicate is true, establish one fixed deadline immediately before
   the first condition suspension.
5. Reuse that deadline across notifications and predicate rechecks.
6. At or after the deadline, reacquire the state lock and evaluate the
   predicate one final time. A satisfied predicate wins; otherwise the result
   is timed out.

Initial state-lock contention is excluded from the timeout. Reacquiring the
state lock can make wall-clock completion occur after the deadline. A zero
timeout still acquires the state lock and evaluates the predicate once.

Asynchronous timeouts are lazy: the budget starts only after the future is
polled, acquires the state lock, observes a blocking predicate, and reaches its
first condition suspension. Constructing and delaying a future consumes no
budget.

All monitor implementations can provide this contract:

- `StdMonitor` and `ParkingLotMonitor` retain their condition-wait deadline
  behavior.
- `TokioMonitor` creates its deadline after the initial predicate check.
- `MockMonitor` reads the manual clock and fixes its target after the initial
  predicate check.

## Mock clock integration

Clock advancement callbacks must not acquire the monitor state lock. The mock
change epoch will move to shared atomic state outside the protected user value.
Both notifications and clock callbacks increment that epoch and signal the
synchronous and asynchronous change channels.

This permits `ManualMonotonicClock::advance` from inside monitor read/write
closures without self-deadlock. Predicate evaluation remains serialized by the
monitor state lock.

## Asynchronous trait returns

The public `AsyncMonitorFuture` boxed-future alias will be removed. Async
monitor traits will return `impl Future<Output = T> + Send + '_`, matching the
crate's async lock traits. Implementations will return concrete async blocks and
will no longer call `Box::pin`.

## Arc-backed monitors

Arc-backed monitor wrappers will retain:

- constructors and ownership-conversion methods;
- `Clone`, `Default`, `From`, `AsRef`, and `Deref` support where applicable;
- explicit monitor trait implementations required for generic bounds.

Redundant inherent forwarding methods will be removed. Normal method calls will
resolve through `Deref`; generic code will use the retained trait
implementations.

## Result usage and documentation

Timeout result and status types will be annotated with `#[must_use]` so ignored
timeouts produce a compiler warning.

Tokio documentation will state that timed waits reaching the suspension path
require a Tokio runtime with the time driver enabled. It will also document
cancellation behavior: dropping a waiting future unregisters that wait and
does not queue a notification for a replacement waiter; no protected-state
action is performed unless the predicate-completion path is reached.

## Verification strategy

Changes will follow test-driven development:

1. Add and run a Tokio regression that exposes the register-before-unlock lost
   notification window.
2. Add and run a Mock regression that advances manual time while a state closure
   holds the monitor lock.
3. Add cross-implementation tests for memoryless notification and fixed
   condition-wait timeout semantics.
4. Update compile-time API tests for RPITIT, removed APIs, `#[must_use]`, and
   Arc wrapper method resolution.
5. Implement only enough production code to make each failing regression pass.
6. Run formatting, all-feature tests, documentation tests, compile-fail tests,
   and Clippy with warnings denied.

The Tokio regression should control the state-lock handoff so it proves that a
condition waiter is registered before a producer can acquire the state lock and
notify. It should not depend only on probabilistic stress timing.

The Mock regression should use a bounded completion signal so the pre-fix
deadlock fails deterministically rather than hanging the test process forever.
