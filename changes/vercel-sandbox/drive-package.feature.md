Run Drive operations on the new `vercel-drives` package and the `/v1/drives` API.
The Drive functions, handles, and `DriveMount` exported by `vercel.sandbox` are
the `vercel.drives` ones and keep working as before: they use
`SandboxServiceOptions`, and API failures raise `SandboxApiError`, which is now
also a `DriveApiError`.

A Vercel app token scoped only to Sandbox needs the matching `read:drive` or
`read-write:drive` scope for Drive operations.
