# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

- Add `ExclusiveLock` to distinguish mutually exclusive acquisition modes
  from the broader `Lock` capability, which also supports shared read modes.

### Changed

- Blocking monitor guards now wait in place. Replace `state = state.wait()`
  with `state.wait()`.
- Replace the removed `wait_timeout` call with `wait_for`; the latter returns
  `Result<WaitTimeoutStatus, TimeError>` so callers must choose an explicit
  Timer error policy.
- The default feature set still exposes the complete synchronous API, now
  through the explicit `monitor` and `parking-lot` features. Lock-only users
  may disable default features to omit both optional dependencies.
- Timed predicate waits now return Timer registration or completion errors
  before considering any post-wait predicate result, so a ready state cannot
  hide a failed timeout guarantee.
- Document the required monitor-lock update-and-notify handshake when a
  predicate reads state stored outside the monitor.
