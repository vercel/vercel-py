# Changelog

## 0.9.0 - 2026-08-05

### Breaking Changes

- The local workflow world now stores its `.workflow-data` files as JSON in the same format the TypeScript `@workflow/world-local` package uses, instead of CBOR. Runs, steps, hooks and events written by either SDK are now readable by the other. Existing `.workflow-data` directories are not readable in the new format and should be deleted. (#226)
- Workflow payloads now use the devalue wire format of the TypeScript `@workflow/core` package, and workflows and steps are called with keyword arguments only. (#224)

### Features

- Workflow payloads can now carry native `Decimal`, `UUID`, `date`, `time`, `timedelta` and `Path`, and `@serializable` (or `register_serializable()`) is offered for custom classes. (#224)
- Add sync and async clients with typed models for managing project-level routing rules and versions. (#219)

## 0.8.1 - 2026-08-02

### Bug Fixes

- Fix the `vercel-sandbox` dependency bound, which `vercel` 0.8.0 published as `vercel-sandbox<0.3.0,>=0.3.0` — a range no version can satisfy, so that release could not be installed at all. (#222)

## 0.8.0 - 2026-07-31

### Breaking Changes

- `vercel.sandbox` is now the promoted Sandbox API that previously lived at `vercel.unstable.sandbox`. The former `vercel.sandbox` surface, including `AsyncSandbox`, `Command`, `AsyncCommand`, and `TokenProvider`, is gone, and `vercel.unstable` has been removed. Use `vercel.sandbox` for the async API and `vercel.sandbox.sync` for the synchronous API. (#195)
- The Sandbox implementation now ships in the separate `vercel-sandbox` distribution, which `vercel` depends on, so `vercel.sandbox` imports keep working without installing anything extra. (#195)
- Workflow and step identifiers now use `//` between the module and qualified name to match the TypeScript SDK format. (#220)

### Features

- Add `vercel.functions.wait_until()` for post-response asynchronous work that remains attached to the current Python Function invocation. (#218)
- Port from vercel-workers to vercel-queue (#184)

### Internal

- Move Vercel SDK tests under the package-local test suite. (#194)

## 0.7.2 - 2026-07-21

### Bug Fixes

- Fix workflows to work with current Vercel workflow-server, and prevent similar breakages in the future. (#190)

## 0.7.1 - 2026-07-16

- No changes.

## 0.7.0 - 2026-07-13

### Features

- Add an experimental `vercel.unstable.sandbox` SDK with sync and async sandbox lifecycle, process, filesystem, snapshot, query, and session APIs. (#128) (#172)
- Add streaming read and write support for Sandbox files and process output. (#135) (#172)
- Improve Workflows sandbox execution with configurable cleanup handlers, passthrough modules, namespaced workflows, and more reliable resume/error handling. (#144, #146, #148, #153, #154, #155, #156, #162, #163, #164, #165) (#172)
- Refactor internal HTTP transport handling to support streamed responses, raw bodies, timeouts, retries, and shared token resolution. (#119) (#172)
