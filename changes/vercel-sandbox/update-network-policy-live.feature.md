`Sandbox.update(network_policy=...)` now also applies the policy to the attached runtime session, matching the TypeScript SDK, so a single call both saves the policy and applies it live.

Live application is best-effort and never resumes a stopped sandbox: HTTP 410 and HTTP 422 `sandbox_stopping` responses are ignored because the saved policy applies to the next session. HTTP 410 also covers failed sessions. Any other failure, including HTTP 422 `sandbox_snapshotting`, is raised after the saved policy has changed.

`Sandbox.update_network_policy()` and `SyncSandbox.update_network_policy()` are deprecated in favor of `update(network_policy=...)`.
