Workflow steps can raise `FatalError` to skip the remaining retries. The step
fails on the attempt that raised it, instead of replaying the same call until
`max_retries` is spent.
