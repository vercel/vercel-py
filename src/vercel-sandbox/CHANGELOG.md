# Changelog

## 0.4.0 - 2026-08-07

### Breaking Changes

- Require synchronous credential factories when configuring `vercel.sandbox.sync`; use asynchronous factories only with the async Sandbox API. (#242)
- Replace the legacy `runtime` selector with `image` when creating sandboxes. Sandbox creation now uses API v3, defaults to `vercel/sandbox/universal:latest`, and supports custom Vercel Container Registry images. (#252)

### Internal

- Run Sandbox examples through the package-owned workspace Poe task. (#234)

## 0.3.0 - 2026-07-31

### Features

- Add `get_or_create_sandbox` to retrieve a named sandbox or create it when it does not exist, for both async and synchronous APIs. (#220)

## 0.2.0 - 2026-07-22

### Features

- Promote the Sandbox SDK as a standalone distribution at `vercel.sandbox`.
