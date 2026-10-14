# vercel-blob

Client for Vercel Blob object storage: uploads, downloads, metadata, server-side copy, batch deletion, lazy catalog queries, and multipart uploads.

Lives in [[src/vercel-blob]] and is exposed as [[src/vercel-blob/vercel/blob/__init__.py|vercel.blob]] (async) and [[src/vercel-blob/vercel/blob/sync/__init__.py|vercel.blob.sync]]. It replaced the Blob module that [[vercel]] used to own; that distribution now depends on it. Function names follow [[vercel-sandbox]]: resource-suffixed verbs such as [[src/vercel-blob/vercel/blob/__init__.py#put_blob]] and lazy `query_*` iterators.

## Layering

One async-only service holds every operation, and the async and sync modules are thin function layers over it.

- [[src/vercel-blob/vercel/blob/_internal/api_client.py#BlobApiClient]] speaks the Blob HTTP API over the session's transport. Private Pydantic models translate requests and responses, so public results stay plain frozen dataclasses from [[src/vercel-blob/vercel/blob/models.py]].
- [[src/vercel-blob/vercel/blob/_internal/service.py#BlobService]] validates inputs, resolves credentials, and calls the API client. [[src/vercel-blob/vercel/blob/_internal/service.py#get_blob_service]] and [[src/vercel-blob/vercel/blob/_internal/service.py#get_sync_blob_service]] cache one service per session.
- The public modules look up the active session's service per call. The sync module drives the same service coroutines with `iter_coroutine` over a sync transport, following [[runtime#Sync and async parity]].

Arguments are validated by [[src/vercel-blob/vercel/blob/_internal/validation.py]] before any credential factory or HTTP request runs, so invalid calls never have side effects.

## Credentials and configuration

Each operation obtains credentials from the session's factory, which defaults to OIDC with a store ID and falls back to a read-write token.

[[src/vercel-blob/vercel/blob/_internal/credentials.py#default_async_credentials]] and its sync twin prefer a [[vercel-oidc]] token combined with `BLOB_STORE_ID`, then `BLOB_READ_WRITE_TOKEN` or `VERCEL_BLOB_READ_WRITE_TOKEN`. A read-write token embeds its store ID, which [[src/vercel-blob/vercel/blob/_internal/credentials.py#normalize_credentials]] checks against the declared one. OIDC requests carry the store ID in a header instead.

[[src/vercel-blob/vercel/blob/_internal/options.py#BlobServiceOptions]] and [[src/vercel-blob/vercel/blob/_internal/options.py#SyncBlobServiceOptions]] share one service options key and set the API base URL and credential factory. The factory runs once per operation so tokens can rotate, but [[src/vercel-blob/vercel/blob/_internal/service.py#BlobService#_get_credentials]] fails if it ever names a different store. The sync factory adapter rejects awaitables rather than running them.

## Pathnames and delivery URLs

Targets are store-relative pathnames or delivery URLs on the standard Blob host, both validated strictly before use.

Pathnames are literal object names: one leading slash is stripped, while empty paths, control characters, lone surrogates, `//`, and dot segments are rejected. Uploads and copy destinations are capped at 950 UTF-16 code units to match the TypeScript SDK. Delivery URLs must be HTTPS on `<store>.<access>.blob.vercel-storage.com`; custom domains are unsupported, and redirects are never followed.

Downloads build the delivery URL themselves rather than asking the API. A public URL can be downloaded without credentials. Private downloads attach credentials only after confirming that the URL's store matches the credential's store.

## Uploads

Uploads send an exact, known length without spooling or chunked transfer encoding, and each runtime accepts only sources of its own shape.

[[src/vercel-blob/vercel/blob/_internal/upload.py#SyncUploadRuntime]] accepts buffers, blocking readers, and iterables; [[src/vercel-blob/vercel/blob/_internal/upload.py#AsyncUploadRuntime]] accepts buffers, async readers, and async iterables, and raises `TypeError` for sync sources instead of moving blocking reads to worker threads. Buffers are snapshotted at call time, and streaming sources require `content_length`.

[[src/vercel-blob/vercel/blob/_internal/upload.py#send_streaming_upload]] writes through the transport's request stream and probes the source past the declared length, so a short or long source raises `BlobContentLengthError` and excess bytes are never sent. When the server rejects the upload mid-stream, the error response is mapped instead of the write failure; see [[vercel-internal-core#HTTP transports]]. Sources remain caller-owned and are never closed, seeked, or retried.

## Multipart uploads

A multipart upload is a scoped context that numbers sequential parts itself and publishes the object only on a clean exit.

[[src/vercel-blob/vercel/blob/_internal/multipart.py#AsyncMultipartUpload]] and [[src/vercel-blob/vercel/blob/_internal/multipart.py#SyncMultipartUpload]] share one state machine. Entering creates the upload, each `upload_part` call sends one complete part and records its ETag, and a successful exit completes it and sets `result`. Parts hold 1 byte to 5 GiB, at most 10,000 of them, and only the last may be under 5 MiB.

Any failure poisons the handle: a failed or overlapping part, cancellation, or an exception leaves the object unpublished, even if user code caught the error inside the block. The async exit checkpoints first so swallowed cancellation still prevents completion. The backend has no abort, so remote parts are never cleaned up, and completion is not retried.

## Downloads

Downloads are streamed with one owner per response; the buffered form is a thin wrapper over the streamed one.

[[src/vercel-blob/vercel/blob/__init__.py#stream_blob]] returns a context that opens the response on entry. [[src/vercel-blob/vercel/blob/_internal/download.py#AsyncBlobDownload]] and [[src/vercel-blob/vercel/blob/_internal/download.py#SyncBlobDownload]] may be iterated once, reject concurrent reads, and close the response at EOF, on error, on cancellation, or on context exit. [[src/vercel-blob/vercel/blob/__init__.py#get_blob]] joins that stream into `bytes` and returns a result that outlives the session.

Download metadata comes from response headers, with no preliminary HEAD request. Missing headers stay `None`, even when the body has been read.

## Collections

Catalog queries are lazy iterators that follow cursors on demand, mirroring Sandbox's `query_*` APIs.

[[src/vercel-blob/vercel/blob/__init__.py#query_blobs]] yields every object under a prefix, and [[src/vercel-blob/vercel/blob/__init__.py#query_blob_entries]] yields one directory level as objects plus frozen [[src/vercel-blob/vercel/blob/models.py#BlobPrefix]] folders. Both share the cursor loop in [[src/vercel-blob/vercel/blob/_internal/pagination.py]]: no request is sent until iteration begins, one page is held at a time, and iteration stops at an empty page or a missing cursor. Folder-only pages carry entries, so browsing continues through them. Page results stay internal.

[[src/vercel-blob/vercel/blob/__init__.py#copy_blob]] copies on the server without downloading, and its `if_match` checks the source ETag. [[src/vercel-blob/vercel/blob/__init__.py#delete_blob]] takes one target or a batch of 1–1,000 sent as one request, validates the whole batch first, and allows `if_match` only for a single target. Batches are not split, retried, or atomic.

## Errors

Service failures map to `BlobError` subclasses carrying the HTTP status and Blob error code.

[[src/vercel-blob/vercel/blob/_internal/api_client.py#_map_http_error]] turns error responses into types from [[src/vercel-blob/vercel/blob/errors.py]], such as [[src/vercel-blob/vercel/blob/errors.py#BlobNotFoundError]] and [[src/vercel-blob/vercel/blob/errors.py#BlobPreconditionFailedError]], parsing `Retry-After` for rate limits. Transport failures become [[src/vercel-blob/vercel/blob/errors.py#BlobUnknownError]]. The SDK does not retry requests itself.

## Tests and examples

Unit tests drive the public API through SDK sessions over mocked HTTPX transports; live tests and examples run against whichever real stores the environment configures.

Unit tests live in [[src/vercel-blob/tests]]. The live suite in [[src/vercel-blob/tests/live]] is selected with `-m live` and runs once per configured store: the default store plus `BLOB_PUBLIC_*` and `BLOB_PRIVATE_*` prefixed stores, covering both access modes. It skips when no store is configured. The [[src/vercel-blob/examples]] scripts each seed and clean up their own uniquely named objects and run through the package's `test-examples` task.
