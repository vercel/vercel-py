# Vercel Blob Python SDK

`vercel-blob` provides async object operations through `vercel.blob` and matching
sync operations through `vercel.blob.sync`.

```sh
pip install vercel-blob
```

## Object lifecycle

Set `BLOB_READ_WRITE_TOKEN` to a read-write token for your store. On Vercel, you
can instead use OIDC credentials with `BLOB_STORE_ID`.

Use `access="public"` for a public store or `access="private"` for a private store.
The access value must match the store.

```python
from vercel import blob

async def round_trip() -> None:
	uploaded = await blob.put_blob("hello.txt", b"hello, Blob!", access="public")
	try:
		result = await blob.get_blob(uploaded.url, access="public")
		print(result.metadata.content_type)
		print(result.body)
		async with blob.stream_blob(uploaded.url, access="public") as download:
			async for chunk in download:
				print(chunk)
		metadata = await blob.head_blob(uploaded.url)
		print(metadata.size)
	finally:
		await blob.delete_blob(uploaded.url)
```

The sync API needs no event loop:

```python
from vercel.blob import sync as blob

uploaded = blob.put_blob("hello.txt", b"hello, Blob!", access="public")
try:
	result = blob.get_blob(uploaded.url, access="public")
	print(result.metadata.content_type)
	print(result.body)
	with blob.stream_blob(uploaded.url, access="public") as download:
		for chunk in download:
			print(chunk)
	print(blob.head_blob(uploaded.url).size)
finally:
	blob.delete_blob(uploaded.url)
```

## Operations

| Operation | Input | Result |
| --- | --- | --- |
| `put_blob` | Pathname, `bytes` or streaming body, required `access` | `PutResult` from the upload response |
| `get_blob` | Pathname or Blob delivery URL, required `access` | `GetResult` with download metadata and the complete body as `bytes` |
| `stream_blob` | Pathname or Blob delivery URL, required `access` | Context manager yielding an iterable byte download |
| `head_blob` | Pathname or Blob delivery URL | `HeadResult` with size, upload time, and object metadata |
| `delete_blob` | One target or a sequence of 1–1,000 pathnames or Blob delivery URLs | `None` |
| `query_blobs` | Optional prefix, page size, and starting cursor | Iterator of `ListBlob` entries that follows pagination automatically |
| `copy` | Source pathname or Blob delivery URL, destination pathname, required `access` | `CopyResult` from the copy response |
| `multipart_upload_blob` | Pathname, required `access`, upload options | Context yielding a sequential part uploader; `result: PutResult` after successful exit |

`put_blob`, `get_blob`, `head_blob`, `copy`, and `delete_blob` are awaited in the async API.
Iterate over `query_blobs` with `async for`, without awaiting the call. Enter
`stream_blob` with `async with`. The sync API uses ordinary calls, iterates over
`query_blobs` with `for`, and enters `stream_blob` with `with`.

Pathnames are relative to the store. One leading slash is removed. Empty paths,
control characters, invalid Unicode, `//`, and `.` or `..` path segments are
rejected. `put_blob` and copy destinations accept at most 950 UTF-16 code units in
the original pathname,
matching the TypeScript SDK. Characters outside the Unicode Basic Multilingual
Plane count as two units. Reads do not inherit this upload limit.

Pathnames are literal object names, not URL-encoded strings or local file paths.
Characters such as `?`, `#`, and `%` are preserved and encoded when constructing
a delivery URL. Delivery URLs must use HTTPS and a standard
`<store>.<access>.blob.vercel-storage.com` host. Their path is checked after one
UTF-8 percent-decode, including for control characters and dot segments. The
encoded path is preserved, queries are retained, and fragments are removed.
Private URL downloads must belong to the credential's store. Redirects are not
followed. Custom delivery domains are not supported.

The SDK does not execute object names or write downloads to local files. Pathname
validation is not a shell-escaping or filesystem-safety guarantee. Applications
must not use untrusted object names as commands or unchecked local file paths.

### Upload options

| Keyword | Default | Behavior |
| --- | --- | --- |
| `access` | Required | `"public"` or `"private"` |
| `content_length` | `None` | Byte length; optional for buffers, required for streaming bodies |
| `content_type` | `None` | Explicit media type; otherwise the service determines it |
| `add_random_suffix` | `False` | Set to `True` to add a random suffix and avoid pathname collisions |
| `allow_overwrite` | `False` | Permit replacement of an existing object when enabled |
| `cache_control_max_age` | `None` | Cache lifetime in whole seconds; otherwise use the service default |

`put_blob` accepts in-memory buffers (`bytes`, `bytearray`, `memoryview`) and streaming
sources with a known length. It infers content length for buffers and snapshots them
at call time. Uploads use a stable pathname by default, matching the TypeScript SDK.
To replace an existing object, set `allow_overwrite=True`. To create distinct objects with
random suffixes, set `add_random_suffix=True` and use the returned `pathname` or
`url` when referring to each upload.

`PutResult` contains `url`, `download_url`, `pathname`, `content_type`,
`content_disposition`, and `etag`. It does not contain size or upload time.
`head_blob` returns those additional fields without downloading the object's content.
No extra metadata request runs after `put_blob`.

### Streaming uploads

`put_blob` also accepts byte readers and byte iterables when `content_length` is provided.
Each API accepts only sources of its own shape:

- **Async API**: `AsyncByteReader` (an object with `async def read`) or
  `AsyncIterable[bytes]`.
- **Sync API**: `SyncByteReader` (an object with a blocking `read`) or `Iterable[bytes]`.

The async API does not run blocking reads on worker threads for you. It rejects sync
readers and sync iterables with a `TypeError`; adapt them as shown below.

Streaming uploads require an explicit `content_length` in bytes. Unknown-length streams
and spooling to temporary files are not supported. The SDK validates that the uploaded
stream matches the declared length:

- If the source ends before `content_length` bytes, `BlobContentLengthError` is raised.
- If the source yields more than `content_length` bytes, `BlobContentLengthError` is
  raised and the excess data is never sent.
- Uploads send an explicit `Content-Length` header without chunked transfer encoding.

Sources remain caller-owned. The SDK does not close, seek, or retry caller sources. If
an upload fails, the source's stream position is unspecified.

Upload a local file with the sync API:

```python
from pathlib import Path
from vercel.blob import sync as blob

path = Path("report.pdf")
size = path.stat().st_size
with path.open("rb") as file:
    uploaded = blob.put_blob("report.pdf", file, access="public", content_length=size)
```

In async code, open the file with `anyio.open_file`:

```python
import anyio
from vercel import blob

async def upload_report() -> blob.PutResult:
    path = anyio.Path("report.pdf")
    size = (await path.stat()).st_size
    async with await anyio.open_file(path, "rb") as file:
        return await blob.put_blob("report.pdf", file, access="public", content_length=size)
```

Wrap a sync file object you already have with `anyio.wrap_file(file)`. Adapt a sync
iterator into an async iterable. An async generator is enough for in-memory chunks;
if producing a chunk can block, pull each chunk on a worker thread:

```python
from collections.abc import AsyncIterator, Iterable, Iterator

import anyio

async def from_memory(chunks: Iterable[bytes]) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk

async def from_blocking(chunks: Iterator[bytes]) -> AsyncIterator[bytes]:
    while (chunk := await anyio.to_thread.run_sync(next, chunks, None)) is not None:
        yield chunk
```

### Multipart uploads

Use `multipart_upload_blob` when you want to choose complete parts explicitly. It
accepts the same metadata, suffix, and overwrite options as `put_blob`. Enter the
context with `async with` (or `with` for `blob.sync`); do not await its creation.

```python
from collections.abc import AsyncIterator
from vercel import blob

async def first_part() -> AsyncIterator[bytes]:
    for _ in range(80):
        yield b"x" * (64 * 1024)

async def upload_parts() -> blob.PutResult:
    async with blob.multipart_upload_blob("parts.bin", access="public") as upload:
        await upload.upload_part(first_part(), content_length=5 * 1024 * 1024)
        await upload.upload_part(b"tail")
    return upload.result
```

The sync API accepts sync readers and iterables:

```python
from vercel.blob import sync as blob

with blob.multipart_upload_blob("parts.bin", access="public") as upload:
    upload.upload_part(
        (b"x" * (64 * 1024) for _ in range(80)), content_length=5 * 1024 * 1024
    )
    upload.upload_part(b"tail")
print(upload.result.url)
```

`upload_part` returns `None`. The SDK assigns consecutive numbers starting at 1
and retains successful ETags internally. There is no part-number argument,
receipt list, or manual completion step. Upload one part at a time; overlapping
calls fail the handle. There is no background scheduling or automatic retry.

Parts accept the same sources and exact-length checks as `put_blob`: buffer lengths
are inferred, streaming lengths are required, and sources stay caller-owned.
Each part must contain 1 byte through 5 GiB, with at most 10,000 parts. Every
part except the last must be at least 5 MiB. After a smaller part, a further
part is rejected before consuming its source or sending its request. An empty
scope raises without completing; use `put(pathname, b"", ...)` for an empty object.

A successful context exit publishes the object and makes `upload.result`
available. Normal early `return` or `break` is a successful exit and publishes
the uploaded parts. Exceptions and cancellation prevent completion. A failed
part, including a validation or source error caught inside the block, permanently
fails that handle; normal exit then raises `BlobError` instead of publishing a
prefix. Result access before successful exit raises `BlobError`, as do part
uploads before entry or after exit.
Keep the multipart context inside its session scope.

Completion errors propagate from context exit and leave `result` unavailable.
A lost completion response can leave publication uncertain; the SDK does not
retry completion. The backend has no multipart abort action. Local cleanup does
not delete remote parts or roll back an object that may already have published.

### Collections

Both runtimes expose `query_blobs`, `copy`, and batch `delete_blob`. Like Sandbox's
`query_*` APIs, `query_blobs` lazily yields individual objects and automatically
follows pagination cursors. Each `ListBlob` includes URL, download URL, pathname,
size, upload date, and ETag.

```python
async for item in blob.query_blobs(prefix="reports/", page_size=100):
    print(item.pathname, item.size)

# Synchronous iteration needs no event loop.
for item in blob.sync.query_blobs(prefix="reports/", page_size=100):
    print(item.pathname, item.size)
```

`page_size` is the maximum number of objects fetched per API request, an integer
from 1 to 1,000 (the service defaults to 1,000). It does not limit the total number
of objects yielded. `prefix` is a literal pathname prefix. The optional opaque
`cursor` specifies where to begin iteration; subsequent cursors are followed
internally.

Creating an iterator sends no HTTP request. Validation and the first request
happen when iteration begins. The iterator retains one page at a time and fetches
the next page only after yielding the current page's objects. Breaking out early
does not fetch further pages. Like Sandbox, iteration stops at an empty page or
when no next cursor remains. Errors propagate during iteration without restarting
or retrying the listing. Iterate within the session's lifetime.

Listing includes objects in nested folders. Folded folder listing and public page
results are not exposed. Listing is not a snapshot while objects change.

```python
copied = await blob.copy(
    "reports/source.bin",
    "reports/archive.bin",
    access="private",
    content_type="application/octet-stream",
    cache_control_max_age=300,
    if_match=source_etag,
)
await blob.delete_blob(["reports/source.bin", copied.url])
```

Copy runs on the server within the credential's store and returns `CopyResult`
from the copy response without a download or preliminary HEAD. Source accepts
a pathname or Blob delivery URL; destination is a pathname. As with PUT,
`access` is required, and `add_random_suffix` and `allow_overwrite` default to
`False`. Source metadata is not preserved: set content type and cache age
explicitly when needed. Copy's `if_match` checks the **source** ETag, not the
destination. A mismatch raises `BlobPreconditionFailedError`.

Delete accepts one target or a sequence of 1–1,000 targets in one request.
Every target is validated before credentials or HTTP. It does not split large
batches, retry them, or promise atomic deletion; an error can leave a batch's
outcome uncertain. Missing targets are harmless. `if_match` is supported only
for a single target (including a sequence of length one), and checks that
object's ETag. Invalid sizes and batch preconditions fail locally.

For synchronous usage, iterate over `blob.sync.query_blobs` and call `blob.sync.copy` and
`blob.sync.delete_blob` without `await`. The package examples exercise cursor
pagination, conditional copy, and batch cleanup in both runtimes.

### Downloads and ownership

`get_blob` buffers the complete body in memory and returns a frozen, slotted
`GetResult` with `metadata: DownloadMetadata` and `body: bytes`. It closes the
response before returning, and the result remains usable after the session closes.
Read errors raise an exception rather than returning a partial body. There is no
`max_bytes` parameter or decoding helper.

`stream_blob` provides incremental reads without buffering the complete body. Entering
its context obtains the response. Both APIs accept the same pathname or Blob
delivery URL and require `access`. Neither issues a preliminary HEAD request.

`result.metadata` and `download.metadata` contain the URL, status code, and
response-derived headers such as size, content type, ETag, and last-modified time.
Header-derived fields are `None` when the server omits them. In particular,
`metadata.size` stays `None` when the size header is absent, even after `get_blob`
reads the body. Use `len(result.body)` for the actual number of downloaded bytes.

A streamed download has one consumer. Iterate it once inside its context. It yields
byte chunks of at most 64 KiB, with a smaller final chunk. It closes its response at
EOF, on early context exit, on read errors, and on cancellation. Explicit closure
uses `await download.aclose()` or `download.close()`. Closed downloads reject
further reads. There is no automatic restart after a partial download.

The session owns the HTTP client; each streamed download owns its response.
Exiting a download does not close the session. Enter `stream_blob` contexts inside their
session scope. Operations and streaming reads reject a closed session. Module
calls outside an explicit session use the SDK's default session.

A complete public delivery URL can be downloaded without Blob credentials.
Pathname downloads need credentials to identify the store. Private downloads
attach credentials only after validating the delivery URL and store.

### Errors

`get_blob`, `stream_blob`, and `head_blob` raise `BlobNotFoundError` for missing objects.
Deleting an absent object succeeds when the backend returns its normal successful
deletion response.
Other deletion errors, including an unknown store, are not suppressed.

Service errors inherit from `BlobError` and expose `status_code` and `code` when
available. Common subclasses include `BlobAccessError`, `BlobStoreNotFoundError`,
`BlobPreconditionFailedError`, and `BlobServiceRateLimited`. Malformed responses
raise `BlobStreamError`; missing or invalid credentials raise `BlobCredentialsError`.
Invalid arguments fail before credential factories or HTTP requests run.

## Session configuration

Default credential discovery uses OIDC with `BLOB_STORE_ID` when both are
available, then `BLOB_READ_WRITE_TOKEN` or `VERCEL_BLOB_READ_WRITE_TOKEN`.
Store IDs may include the `store_` prefix.

For application-managed credentials, configure a factory on the session:

```python
import os

from vercel import blob
from vercel.api import session

async def credentials() -> blob.BlobCredentials:
	return blob.BlobCredentials(
		token=os.environ["MY_BLOB_TOKEN"],
		store_id=os.environ["MY_BLOB_STORE_ID"],
	)

async def inspect_object() -> None:
	options = blob.BlobServiceOptions(credentials_factory=credentials)
	async with session(service_options=[options]):
		print(await blob.head_blob("hello.txt"))
```

Set `kind=blob.CredentialKind.OIDC` for an OIDC token. The factory runs for each
operation that needs credentials, so it can refresh tokens. A session's factory
must continue to identify the same store.

Use `vercel.blob.sync.SyncBlobServiceOptions` with a synchronous factory for sync
calls. An async factory supplied to the sync API is rejected. Custom HTTP clients
are configured through the SDK session's `httpx_client_factory`, not through Blob.

## Examples and verification

[Async lifecycle](examples/blob_async.py) and [sync lifecycle](examples/blob_sync.py)
upload a unique object, compare downloaded bytes, inspect metadata, and delete the
object in `finally`. The async example also adapts a sync file and a sync iterator
for streaming uploads. Both examples also stream multipart parts, verify the
published content, and delete the resulting object.

From the repository root:

```sh
uv run poe test vercel-blob
uv run poe qa vercel-blob
uv run poe test vercel-blob -- -m live
uv run poe test-examples vercel-blob
```

### Live tests

Live tests and examples run against every store they find in the environment:

| Store | Credentials | Access |
| --- | --- | --- |
| Default | `BLOB_READ_WRITE_TOKEN`, or `BLOB_STORE_ID` with Vercel OIDC | `BLOB_TEST_ACCESS`, default `public` |
| `BLOB_PUBLIC` | `BLOB_PUBLIC_READ_WRITE_TOKEN`, or `BLOB_PUBLIC_STORE_ID` with Vercel OIDC | `public` |
| `BLOB_PRIVATE` | `BLOB_PRIVATE_READ_WRITE_TOKEN`, or `BLOB_PRIVATE_STORE_ID` with Vercel OIDC | `private` |

The prefixes match the environment variable prefix you choose when you connect a
store to a Vercel project. Each test object lives under a unique `vercel-py-live/`
or `vercel-py-examples/` pathname, and each test deletes its objects. With no
store configured, the tests skip, and a skip does not verify the service.

To test with a linked project, pull its development environment and source it
from the repository root. This example uses a project whose private store has no
prefix and whose public store uses `BLOB_PUBLIC`:

```sh
vc link --scope <team> --project <project>
vc env pull .env.local
(set -a && . ./.env.local && BLOB_TEST_ACCESS=private uv run poe test vercel-blob -- -m live)
```

Source the file instead of passing `uv run --env-file`. uv does not override
variables that are already set, so a `VERCEL_OIDC_TOKEN` exported for another
project wins, and the Blob API answers `403 Invalid token`.

This release does not include unknown-length streaming uploads or file-like
`open` operations.
