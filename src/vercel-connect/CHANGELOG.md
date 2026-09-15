# Changelog

## 0.2.0 - 2026-09-14

### Features

- Replace the unmaintained `httpx` dependency with its maintained `httpx2` successor. (#356)

### Internal

- Require `vercel-internal-core>=0.2.0,<0.3.0` for the coordinated `httpx2` release.

## 0.1.1 - 2026-09-01

- Update dependencies.

## 0.1.0 - 2026-08-07

### Features

- Add `vercel.connect`, a Python SDK for Vercel Connect: short-lived third-party credentials brokered through the deployment's Vercel OIDC identity, with token caching, authorization flows, connector metadata, and inbound trigger verification. (#205)
