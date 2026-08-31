# Changelog

## 0.10.0 - 2026-08-31

### Breaking Changes

- Make `await hook` never return `None`
- Raises a new `HookDisposedError` instead of returning `None` when the hook has been disposed. It is now typed to return `T` instead of `T | None`. `async for` over a hook will stop iterating on disposal, still.
- Make sleep() and retry delays treat numbers as seconds, not ms (#346)
- This matches Python standard library APIs. (#346)
- Use type annotations on workflows and step to allow passing Pydantic models and dataclasses. (#317)
- This is a breaking change, because type annotations will now be enforced. Passing a `dict` when the declaration expects a `list` will fail. (#317)
- Pydantic models and dataclasses can no longer be passed to `@serializable` or `register_serializable()`. Annotate the workflow or step parameter or return value with their type instead. (#317)

### Features

- Support `call_later`, `call_at`, and `now` in the event loop implementation. (#343)
- This enables use of `asyncio.sleep()` as well as `asyncio.timeout` and the `timeout` parameter of `asyncio.wait_for`. (#343)
- `get_workflow_metadata()` returns the current run's `WorkflowInfo` (run id, workflow name, start time, deployment URL, and feature flags), callable from a workflow body or a step body — mirroring the JS SDK's `getWorkflowMetadata()`. (#320)
- One current limitation is that `started_at` is `None` from inside a step. (#320)
- Make `HookEvent` an async context manager
- This matches TS, which supports `using`. ``` # disposes the hook on block exit async with SomeHook.wait(...) as hook: res = await hook ```
- `BaseHook.wait()` accepts `metadata` to record on the hook, and `get_hook_by_token()` reads it back for a resumer. (#301)
- A step can raise `RetryableError` to control when its next attempt runs. (#302)
- Accept `specVersion` 7 sealed noop event logs. (#319)
- Failed run and step events now preserve serialized error classes, messages, stacks, and causes. Failed runs also expose a plaintext `errorCode`. (#304)
- A workflow or step can attach plaintext metadata to its run with `set_attributes()`. (#303)
- Add a `share_sandboxes` parameter to `SandboxPolicy` to enable reusing already created sandboxes instead of creating a new one on each invocation. This speeds up workflows but means that modifications to global state may persist between invocations. (#310)
- Support `timedelta` arguments for workflow `sleep()` and retry delays. (#342)
- Expose unstable API to serve workflow HTTP endpoint from your own web framework. (#294)
- Added semi-internal manifest API for TS tools and e2e test. (#296)

### Bug Fixes

- Fix failing or even crashing cipher calls inside the workflow sandbox. (#305)
- Fail a workflow run with `HookConflictError` when another run already owns its hook token instead of leaving it running indefinitely. (#327)
- Support resuming hooks with payload in the queue message. (#300)
- Fix some bugs involving hooks arriving when the workflow was not yet blocked on them. (#339)
- Fixed nulls rejected by server, requiring Pydantic 2.12 or newer. (#321)
- Prevent workflows from having side effects while suspending. (#332)
- `hook.dispose()` will now work properly in a `finally` block.  (That is, the hook will be disposed only when the workflow is actually terminating, and not every time it gets replayed.) (#332)
- More reliably fail runs whose replay diverges from the event log. (#347)
- Runs will now fail even in the case where the main thread of execution is not directly blocked on the suspension that is erroring. (#347)
- Fixed workflow and step calls with both positional-or-keyword parameters and `*args` failing during replay because their arguments were recorded in an unbindable shape. (#312)

### Internal

- Remove a just-added return from a finally block. (#344)
- Correct internal workflow type annotations found by checking untyped function bodies. (#337)
- Refactored event replay. (#341)
- Construct the protocol models by Python field name. (#322)

## 0.9.0 - 2026-08-13

### Features

- Promote Vercel Workflows as a standalone distribution at `vercel.workflow`.
