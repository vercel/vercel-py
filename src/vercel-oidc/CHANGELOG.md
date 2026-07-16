# Changelog

## 0.7.1 - 2026-07-16

- No changes.

## 0.7.0 - 2026-07-13

### Features

- Prepare `vercel-oidc` for independent workspace releases with dynamic dependency metadata and the shared release build hook. (#172)

### Bug Fixes

- Remember the freshest live request OIDC token process-wide so background SDK work can authenticate outside the originating request context. (#172)
