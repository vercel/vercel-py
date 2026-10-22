`Sandbox.update()` now applies changes to the running session before it saves the configuration, matching the TypeScript SDK. A larger `execution_time_limit` extends the running session, and `network_policy` is applied to it. If applying the policy fails, the configuration and tags are not saved. `update()` never resumes a stopped sandbox.

`Sandbox.update_network_policy()`, `SandboxRuntimeSession.update_network_policy()`, and their sync counterparts are deprecated in favor of `update(network_policy=...)`.
