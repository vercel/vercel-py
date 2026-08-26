# Changelog

## 0.10.0 - 2026-08-26

### Breaking Changes

- Use type annotations on workflows and step to allow passing Pydantic models and dataclasses. (#317)
- This is a breaking change, because type annotations will now be enforced. Passing a `dict` when the declaration expects a `list` will fail. (#317)
- Pydantic models and dataclasses can no longer be passed to `@serializable` or `register_serializable()`. Annotate the workflow or step parameter or return value with their type instead. (#317)

### Features

- `get_workflow_metadata()` returns the current run's `WorkflowInfo` (run id, workflow name, start time, deployment URL, and feature flags), callable from a workflow body or a step body — mirroring the JS SDK's `getWorkflowMetadata()`. (#320)
- One current limitation is that `started_at` is `None` from inside a step. (#320)
- `BaseHook.wait()` accepts `metadata` to record on the hook, and `get_hook_by_token()` reads it back for a resumer. (#301)
- A step can raise `RetryableError` to control when its next attempt runs. (#302)
- Accept `specVersion` 7 sealed noop event logs. (#319)
- A workflow or step can attach plaintext metadata to its run with `set_attributes()`. (#303)
- Add a `share_sandboxes` parameter to `SandboxPolicy` to enable reusing already created sandboxes instead of creating a new one on each invocation. This speeds up workflows but means that modifications to global state may persist between invocations. (#310)
- Expose unstable API to serve workflow HTTP endpoint from your own web framework. (#294)
- Added semi-internal manifest API for TS tools and e2e test. (#296)

### Bug Fixes

- Fix failing or even crashing cipher calls inside the workflow sandbox. (#305)
- Support resuming hooks with payload in the queue message. (#300)
- Fixed nulls rejected by server, requiring Pydantic 2.12 or newer. (#321)
- Fixed workflow and step calls with both positional-or-keyword parameters and `*args` failing during replay because their arguments were recorded in an unbindable shape. (#312)

### Internal

- Construct the protocol models by Python field name. (#322)

## 0.9.0 - 2026-08-13

### Features

- Promote Vercel Workflows as a standalone distribution at `vercel.workflow`.
