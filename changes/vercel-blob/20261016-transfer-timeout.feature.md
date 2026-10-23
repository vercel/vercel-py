Add a `transfer_timeout` option to `BlobServiceOptions` and
`SyncBlobServiceOptions`, defaulting to five minutes. Uploads, multipart parts
and completion, and copies use it instead of the HTTP client's 60 second
timeout, since the service can take longer than that to respond for large
objects. HTTP timeouts now raise `BlobTimeoutError`, a subclass of
`BlobUnknownError`, so they can be told apart from other network failures.
