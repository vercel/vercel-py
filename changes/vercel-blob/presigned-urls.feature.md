Add presigned URLs. `issue_blob_signed_token` issues a scoped delegation
(pathname, operations, expiry, upload size, and content types) with the
session's credentials, and `presign_blob_url` signs a `get`, `head`, `put`, or
`delete` URL from it locally, with the same signatures as the TypeScript SDK's
`presignUrl`. Both are mirrored in `vercel.blob.sync`.
