# Changelog

## 0.1.3 - 2026-08-26

### Internal

- Support disabling HTTP timeouts for selected SDK operations while preserving the client default elsewhere. (#307)

## 0.1.2 - 2026-08-07

### Internal

- Key session service options by logical service so synchronous and asynchronous variants share configuration safely. (#242)
- Absorb `typeutils` from `vercel-queue`: the annotation predicates and runtime forward-reference resolution now live in `vercel._internal.core.typeutils`, where more than one package can reach them. (#261)

## 0.1.1 - 2026-07-31

### Bug Fixes

- Bind asynchronous session clients to their running event loop to prevent failures when a shared session is used across multiple event loops or threads. (#220)

## 0.1.0 - 2026-07-22

### Features

- Add the shared session, transport, stream, and runtime foundation for Vercel Python services.
