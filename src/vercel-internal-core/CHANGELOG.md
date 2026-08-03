# Changelog

## 0.1.1 - 2026-07-31

### Bug Fixes

- Bind asynchronous session clients to their running event loop to prevent failures when a shared session is used across multiple event loops or threads. (#220)

## 0.1.0 - 2026-07-22

### Features

- Add the shared session, transport, stream, and runtime foundation for Vercel Python services.
