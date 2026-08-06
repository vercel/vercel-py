# Changelog

## 0.7.3 - 2026-08-05

- Update dependencies.

## 0.7.2 - 2026-07-31

### Bug Fixes

- Don't require `VERCEL_DEPLOYMENT_ID` to be set when running against a `vercel dev` queue broker. (#211)
- This will allow us to revert `vc dev` setting `VERCEL_DEPLOYMENT_ID`. (#211)

## 0.7.1 - 2026-07-16

- No changes.

## 0.7.0 - 2026-07-13

### Features

- Initial release (#158) (#172)

### Bug Fixes

- Stop nesting retry wrappers in delivery lifecycle settlement: the RetryAfter (#175)
- and ACK paths wrapped the already-retrying `extend_lease()`/`acknowledge()` (#175)
- client methods in a second retry layer, multiplying retry attempts and (#175)
- logging duplicate `visibility.retry_attempt` events per request. (#175)
