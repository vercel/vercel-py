Streams can now carry typed data.

`get_writable(type=Token)` returns a `WorkflowWritable[Token]` whose writes are dumped through pydantic the way typed step arguments are; then `run.readable(type=Token)` and `read_stream(run_id, name, type=Token)` validate each chunk on the way back, raising `TypeValidationError` on a mismatch.

`WorkflowWritable` is now generic, defaulting to `Any`; a step parameter annotated `WorkflowWritable[Token]` becomes a handle the workflow passed in as a writer of that type, and `writable.with_type(Token)` gives a typed view of any writable.
