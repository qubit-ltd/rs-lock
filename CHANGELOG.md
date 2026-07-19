# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Changed

- Blocking monitor guards now wait in place. Replace `state = state.wait()`
  with `state.wait()`.
- Replace the removed `wait_timeout` call with `wait_for`; the latter returns
  `Result<WaitTimeoutStatus, TimeError>` so callers must choose an explicit
  Timer error policy.
- The default feature set still exposes the complete synchronous API, now
  through the explicit `monitor` and `parking-lot` features. Lock-only users
  may disable default features to omit both optional dependencies.
