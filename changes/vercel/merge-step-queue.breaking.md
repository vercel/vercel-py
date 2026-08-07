Workflow steps now ride the `__wkf_workflow_*` queue as a `stepId` on the
workflow invoke payload, matching the TypeScript SDK; the separate
`__wkf_step_*` queue is gone.
