Deprecate the Drive functions in favor of the new `vercel-drive` package.
`get_drive`, `get_or_create_drive`, `fork_drive`, `delete_drive`, and
`query_drives` on `vercel.sandbox`, `vercel.sandbox.sync`, and the standalone
clients, and `fork`, `snapshot`, and `delete` on the handles they return, keep
working unchanged and now emit `DeprecationWarning`.

Sandbox mounts accept `vercel.drive` handles and mounts. `DriveMount`,
`DriveHandle`, `DriveReference`, and the Drive query types exported by
`vercel.sandbox` are now the `vercel.drive` types.
