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
		async with blob.get(uploaded.url, access="public") as download:
			print(download.metadata.content_type)
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
	with blob.get(uploaded.url, access="public") as download:
		for chunk in download:
			print(chunk)
	print(blob.head(uploaded.url).size)
finally:
	blob.delete(uploaded.url)
```

## Operations

| Operation | Input | Result |
| --- | --- | --- |
| `put` | Pathname, `bytes`, required `access` | `PutResult` from the upload response |
| `get` | Pathname or Blob delivery URL, required `access` | Context manager yielding an iterable byte download |
| `head` | Pathname or Blob delivery URL | `HeadResult` with size, upload time, and object metadata |
| `delete` | One pathname or Blob delivery URL | `None` |

`put`, `head`, and `delete` are awaited in the async API. `get` is not awaited;
enter it with `async with`. The sync API uses ordinary calls and `with`.

Pathnames are relative to the store. Leading slashes are removed. Empty paths,
control characters, and `.` or `..` path segments are rejected. Delivery URLs
must use HTTPS and a standard `<store>.<access>.blob.vercel-storage.com` host.
Private URL downloads must belong to the credential's store. Redirects are not
followed. Custom delivery domains are not supported.

### Upload options

| Keyword | Default | Behavior |
| --- | --- | --- |
| `access` | Required | `"public"` or `"private"` |
| `content_type` | `None` | Explicit media type; otherwise the service determines it |
| `add_random_suffix` | `True` | Add a random suffix to avoid pathname collisions |
| `allow_overwrite` | `False` | Permit replacement of an existing object when enabled |
| `cache_control_max_age` | `None` | Cache lifetime in whole seconds; otherwise use the service default |

`put` accepts only `bytes`, including `b""`. It infers content length and never
chooses multipart upload, reads an iterable, or stages data in a temporary file.
To write a stable pathname, set `add_random_suffix=False`. To replace that object,
also set `allow_overwrite=True`. Always use the returned `pathname` or `url` when
referring to an upload that has a random suffix.

`PutResult` contains `url`, `download_url`, `pathname`, `content_type`,
`content_disposition`, and `etag`. It does not contain size or upload time.
`head` returns those additional fields without downloading the object's content.
No extra metadata request runs after `put`.

### Downloads and ownership

Entering `get` obtains the response without reading its complete body or issuing
a preliminary HEAD. `download.metadata` contains the URL, status code, and
response-derived headers such as size, content type, ETag, and last-modified time.
Header-derived fields are `None` when the server omits them.

A download has one consumer. Iterate it once inside its context. It yields byte
chunks of at most 64 KiB, with a smaller final chunk. It closes its response at
EOF, on early context exit, on read errors, and on cancellation. Explicit closure
uses `await download.aclose()` or `download.close()`. Closed downloads reject
further reads. There is no automatic restart after a partial download.

The session owns the HTTP client; each download owns its response. Exiting a
download does not close the session. Enter download contexts inside their session
scope. Operations and reads reject a closed session. Module calls outside an
explicit session use the SDK's default session.

A complete public delivery URL can be downloaded without Blob credentials.
Pathname downloads need credentials to identify the store. Private downloads
attach credentials only after validating the delivery URL and store.

### Errors

`get` and `head` raise `BlobNotFoundError` for missing objects. Deleting an absent
object succeeds when the backend returns its normal successful deletion response.
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
object in `finally`.

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

This release does not include streaming uploads, multipart uploads, listing,
batch deletion, copying, or file-like `open` operations.
