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
	uploaded = await blob.put("hello.txt", b"hello, Blob!", access="public")
	try:
		result = await blob.get(uploaded.url, access="public")
		print(result.metadata.content_type)
		print(result.body)
		async with blob.stream(uploaded.url, access="public") as download:
			async for chunk in download:
				print(chunk)
		metadata = await blob.head(uploaded.url)
		print(metadata.size)
	finally:
		await blob.delete(uploaded.url)
```

The sync API needs no event loop:

```python
from vercel.blob import sync as blob

uploaded = blob.put("hello.txt", b"hello, Blob!", access="public")
try:
	result = blob.get(uploaded.url, access="public")
	print(result.metadata.content_type)
	print(result.body)
	with blob.stream(uploaded.url, access="public") as download:
		for chunk in download:
			print(chunk)
	print(blob.head(uploaded.url).size)
finally:
	blob.delete(uploaded.url)
```

## Operations

| Operation | Input | Result |
| --- | --- | --- |
| `put` | Pathname, `bytes` or streaming body, required `access` | `PutResult` from the upload response |
| `get` | Pathname or Blob delivery URL, required `access` | `GetResult` with download metadata and the complete body as `bytes` |
| `stream` | Pathname or Blob delivery URL, required `access` | Context manager yielding an iterable byte download |
| `head` | Pathname or Blob delivery URL | `HeadResult` with size, upload time, and object metadata |
| `delete` | One pathname or Blob delivery URL | `None` |

`put`, `get`, `head`, and `delete` are awaited in the async API. `stream` is not
awaited; enter it with `async with`. The sync API uses ordinary calls and enters
`stream` with `with`.

Pathnames are relative to the store. One leading slash is removed. Empty paths,
control characters, invalid Unicode, `//`, and `.` or `..` path segments are
rejected. `put` accepts at most 950 UTF-16 code units in the original pathname,
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

`put` accepts in-memory buffers (`bytes`, `bytearray`, `memoryview`) and streaming
sources with a known length. It infers content length for buffers and snapshots them
at call time. Uploads use a stable pathname by default, matching the TypeScript SDK.
To replace an existing object, set `allow_overwrite=True`. To create distinct objects with
random suffixes, set `add_random_suffix=True` and use the returned `pathname` or
`url` when referring to each upload.

`PutResult` contains `url`, `download_url`, `pathname`, `content_type`,
`content_disposition`, and `etag`. It does not contain size or upload time.
`head` returns those additional fields without downloading the object's content.
No extra metadata request runs after `put`.

### Streaming uploads

`put` also accepts byte readers and byte iterables when `content_length` is provided.
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
    uploaded = blob.put("report.pdf", file, access="public", content_length=size)
```

In async code, open the file with `anyio.open_file`:

```python
import anyio
from vercel import blob

async def upload_report() -> blob.PutResult:
    path = anyio.Path("report.pdf")
    size = (await path.stat()).st_size
    async with await anyio.open_file(path, "rb") as file:
        return await blob.put("report.pdf", file, access="public", content_length=size)
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

Copy a Blob object by piping `stream` directly into `put`. Delivery responses
can omit `Content-Length`, so take the size from `head`:

```python
from vercel import blob

async def copy_object(source_url: str, destination_path: str) -> blob.PutResult:
    size = (await blob.head(source_url)).size
    async with blob.stream(source_url, access="public") as download:
        return await blob.put(
            destination_path,
            download,
            access="public",
            content_length=size,
        )
```

### Downloads and ownership

`get` buffers the complete body in memory and returns a frozen, slotted
`GetResult` with `metadata: DownloadMetadata` and `body: bytes`. It closes the
response before returning, and the result remains usable after the session closes.
Read errors raise an exception rather than returning a partial body. There is no
`max_bytes` parameter or decoding helper.

`stream` provides incremental reads without buffering the complete body. Entering
its context obtains the response. Both APIs accept the same pathname or Blob
delivery URL and require `access`. Neither issues a preliminary HEAD request.

`result.metadata` and `download.metadata` contain the URL, status code, and
response-derived headers such as size, content type, ETag, and last-modified time.
Header-derived fields are `None` when the server omits them. In particular,
`metadata.size` stays `None` when the size header is absent, even after `get`
reads the body. Use `len(result.body)` for the actual number of downloaded bytes.

A streamed download has one consumer. Iterate it once inside its context. It yields
byte chunks of at most 64 KiB, with a smaller final chunk. It closes its response at
EOF, on early context exit, on read errors, and on cancellation. Explicit closure
uses `await download.aclose()` or `download.close()`. Closed downloads reject
further reads. There is no automatic restart after a partial download.

The session owns the HTTP client; each streamed download owns its response.
Exiting a download does not close the session. Enter `stream` contexts inside their
session scope. Operations and streaming reads reject a closed session. Module
calls outside an explicit session use the SDK's default session.

A complete public delivery URL can be downloaded without Blob credentials.
Pathname downloads need credentials to identify the store. Private downloads
attach credentials only after validating the delivery URL and store.

### Errors

`get`, `stream`, and `head` raise `BlobNotFoundError` for missing objects.
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
		print(await blob.head("hello.txt"))
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
for streaming uploads.

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

This release does not include unknown-length streaming uploads, multipart uploads, listing,
batch deletion, copying, or file-like `open` operations.
