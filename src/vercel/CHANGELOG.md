# Changelog

## 0.11.0 - 2026-09-01

### Features

- Expose `get_deadline()` for reading the current Function invocation deadline. (#306)
- Answer workflow health checks for both queue-based transport and HTTP. (#292)
- Add support to read the sealed (`encp`) workflow payloads (X25519 + AES-GCM) an outside writer addresses to a run, under the `encryption` extra. (#297)

### Bug Fixes

- Remove upper bounds on aggregate Sandbox and Workflow dependencies so sibling releases cannot make the `vercel` package un-installable. (#334)

- Start a workflow run even when its queue message arrives before the
  `run_created` event has landed. (#284)

### Internal

- The Workflows implementation now ships in the separate `vercel-workflow`
  distribution, which `vercel` depends on, so `vercel.workflow` imports keep
  working without installing anything extra. (#299)

## 0.10.0 - 2026-08-12

### Breaking Changes

- `bytes` now crosses the wire as a `Uint8Array` instead of an `ArrayBuffer`, and a JavaScript `ArrayBuffer` decodes to `bytearray` instead of `bytes`. (#272)

### Features

- A step can now stream output while it runs with `get_writable()`, and `run.readable()` reads it back. A workflow body can take the stream and pass it to its steps. The same stream is readable from the TypeScript SDK, the dashboard and `workflow inspect stream`. (#258)
- Add support to read the encrypted (`encr`) workflow payloads (AES-GCM) under the new `encryption` extra. (#279)
- Workflow steps can raise `FatalError` to skip the remaining retries. The step fails on the attempt that raised it, instead of replaying the same call until `max_retries` is spent. (#260)

### Bug Fixes

- Send and receive workflow queue messages as CBOR, mirroring `DualTransport` in `@workflow/world-vercel`. (#265)
- Allow workflow events with `specVersion` 6. (#280)
- Send the Trusted Sources bypass header on direct workflow-server requests, and derive a non-2xx error from the HTTP status before parsing the body. (#278)

## 0.9.0 - 2026-08-07

### Breaking Changes

- The local workflow world now stores its `.workflow-data` files as JSON in the same format the TypeScript `@workflow/world-local` package uses, instead of CBOR. Runs, steps, hooks and events written by either SDK are now readable by the other. Existing `.workflow-data` directories are not readable in the new format and should be deleted. (#226)
- Workflow payloads now use the devalue wire format of the TypeScript `@workflow/core` package. (#243)
- Workflow steps now ride the `__wkf_workflow_*` queue as a `stepId` on the workflow invoke payload, matching the TypeScript SDK; the separate `__wkf_step_*` queue is gone. (#251)

### Features

- Workflow payloads can now carry native `Decimal`, `UUID`, `date`, `time`, `timedelta` and `Path`, and `@serializable` (or `register_serializable()`) is offered for custom classes. (#224)
- Add sync and async clients with typed models for managing project-level routing rules and versions. (#219)

### Bug Fixes

- Allow workflows on Python 3.12 and earlier to import `uuid` by safely exposing `platform.system()` while continuing to block host-specific platform inspection. (#242)
- Prevent errors when tasks waiting on steps or hooks are cancelled. (#250)
- The Vercel world now honours `VERCEL_WORKFLOW_SERVER_URL` and `WORKFLOW_VERCEL_BACKEND_URL`, which previously had no effect in Python, so a preview deployment reaches the same workflow-server as its TypeScript peers. (#248)

### Internal

- Use a consistent isolated event-loop lifecycle for workflow execution on Python 3.10. (#242)
- Run workflows on a dedicated event loop that advances execution when the loop becomes idle. (#242)
- Avoid invoking the workflow event loop's idle hook after the loop begins stopping. (#242)

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
