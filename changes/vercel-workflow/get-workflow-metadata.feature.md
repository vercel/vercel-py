`get_workflow_metadata()` returns the current run's `WorkflowInfo` (run id,
workflow name, start time, deployment URL, and feature flags), callable from a
workflow body or a step body — mirroring the JS SDK's `getWorkflowMetadata()`.

One current limitation is that `started_at` is `None` from inside a step.
