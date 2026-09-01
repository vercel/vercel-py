# Changelog

## 0.7.2 - 2026-09-01

### Bug Fixes

- Accept request objects with concrete header implementations in the IP address and geolocation type annotations. (#337)

## 0.7.1 - 2026-07-16

- No changes.

## 0.7.0 - 2026-07-13

### Features

- Add request header context snapshots for running callbacks with the active Vercel header context. (#166) (#172)
- Add ASGI scope and WSGI environ helpers for constructing Vercel header mappings. (#172)
