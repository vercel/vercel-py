Deprecate the Drive functions in favor of the new `vercel-drives` package.
`get_drive`, `get_or_create_drive`, `fork_drive`, `delete_drive`, and
`query_drives` on `vercel.sandbox`, `vercel.sandbox.sync`, and the standalone
clients, and `fork`, `snapshot`, and `delete` on the handles they return, keep
working unchanged and now emit `DeprecationWarning`.

Sandbox mounts accept `vercel.drives` handles and mounts. `DriveMount`,
`DriveHandle`, and `DriveReference` exported by `vercel.sandbox` are now the
`vercel.drives` types, and its Drive query types extend the `vercel.drives`
ones, so queries from either package work with both.
