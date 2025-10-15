# vercel-sdk

Python SDK for Vercel.


## Installation

```bash
pip install vercel-sdk
```

## Requirements

- Python 3.10+

## Usage

### Headers and request context

```python
from vercel.headers import geolocation, ip_address

# In a framework handler, pass request.headers
city_info = geolocation(request)
client_ip = ip_address(request.headers)
```

### Runtime Cache

The SDK talks to Vercel’s Runtime Cache when the following env vars are present; otherwise it falls back to an in-memory cache.

- `RUNTIME_CACHE_ENDPOINT`: base URL of the runtime cache API (e.g. https://cache.vercel.com/...)  
- `RUNTIME_CACHE_HEADERS`: JSON object of headers to send (e.g. '{"authorization": "Bearer <token>"}')
- Optional: `SUSPENSE_CACHE_DEBUG=true` to log fallback behavior

```python
from vercel.cache import get_cache

def main():
    cache = get_cache(namespace="demo")

    cache.delete("greeting")
    cache.set("greeting", {"hello": "world"}, {"ttl": 60, "tags": ["demo"]})
    value = cache.get("greeting")  # dict or None
    cache.expire_tag("demo")        # invalidate by tag

main()
```

### OIDC (Vercel project tokens)

If the `VERCEL_OIDC_TOKEN` header is not present, the SDK will try to refresh a token using the local Vercel CLI session and your project configuration.

```python
from vercel.oidc import get_vercel_oidc_token, decode_oidc_payload

def main():
    token = get_vercel_oidc_token()
    payload = decode_oidc_payload(token)
    sub = payload.get("sub")

main()
```

Notes:
- Requires a valid Vercel CLI login on the machine running the code for refresh.
- Project info is resolved from `.vercel/project.json`.

### Blob Storage

The SDK includes a Blob client for uploading, listing, downloading, copying, and deleting objects in Vercel Blob.

- Required: set `BLOB_READ_WRITE_TOKEN` or pass `token` when constructing a client

Async usage:

```python
import asyncio
from vercel.blob import AsyncBlobClient

async def main():
    client = AsyncBlobClient()  # uses BLOB_READ_WRITE_TOKEN from env

    # Upload bytes
    uploaded = await client.put(
        "examples/assets/hello.txt",
        b"hello from python",
        access="public",
        content_type="text/plain",
    )

    # Inspect metadata, list, download bytes, then delete
    meta = await client.head(uploaded.url)
    listing = await client.list_objects(prefix="examples/assets/")
    content = await client.get(uploaded.url)
    await client.delete([b.url for b in listing.blobs])

asyncio.run(main())
```

Synchronous usage:

```python
from vercel.blob import BlobClient

client = BlobClient()  # or BlobClient(token="...")

# Create a folder entry, upload a local file, list, then download
client.create_folder("examples/assets", overwrite=True)
uploaded = client.upload_file(
    "./README.md",
    "examples/assets/readme-copy.txt",
    access="public",
    content_type="text/plain",
)
listing = client.list_objects(prefix="examples/assets/")
client.download_file(uploaded.url, "/tmp/readme-copy.txt", overwrite=True)
```

## Examples

See `examples/` for runnable scripts:
- `runtime_cache_basic.py`: set/get with fallback to in-memory
- `cache_tags.py`: tag-based invalidation
- `build_cache_env.py`: shows behavior when cache env vars are set
- `blob_storage.py`: end-to-end blob operations (sync and async)
- `fastapi_oidc_plus_cache/`: small FastAPI demo wiring headers/oidc

## Development

- Lint/typecheck/tests:
```bash
uv pip install -e .[dev]
uv run ruff format --check && uv run ruff check . && uv run mypy src && uv run pytest -v
```
- CI runs lint, typecheck, examples as smoke tests, and builds wheels.
- Publishing: push a tag (`vX.Y.Z`) that matches `project.version` to publish via PyPI Trusted Publishing.

## License

MIT


