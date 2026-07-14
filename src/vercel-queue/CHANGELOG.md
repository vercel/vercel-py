# Changelog

## 0.7.0 - 2026-07-13

### Features

- Initial release (#158) (#172)

### Bug Fixes

- Stop nesting retry wrappers in delivery lifecycle settlement: the RetryAfter (#175)
- and ACK paths wrapped the already-retrying `extend_lease()`/`acknowledge()` (#175)
- client methods in a second retry layer, multiplying retry attempts and (#175)
- logging duplicate `visibility.retry_attempt` events per request. (#175)
