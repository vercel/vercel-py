# Vercel Python SDK


## Installation

```bash
pip install vercel
```

## Requirements

- Python 3.10+

## Usage

This package provides both synchronous and asynchronous clients to interact with the Vercel API.

<br/>

---



### Headers and request context

```python
from typing import Callable

from fastapi import FastAPI, Request
from vercel.headers import geolocation, ip_address, set_headers

app = FastAPI()

@app.middleware("http")
async def vercel_context_middleware(request: Request, call_next: Callable):
    set_headers(request.headers)
    return await call_next(request)

@app.get("/api/headers")
async def headers_info(request: Request):
    ip = ip_address(request.headers)
    geo = geolocation(request)
    return {"ip": ip, "geo": geo}
```

<br/>

---

### Runtime Cache

#### Sync
```python
from vercel.cache import get_cache

def main():
    cache = get_cache(namespace="demo")

    cache.delete("greeting")
    cache.set("greeting", {"hello": "world"}, {"ttl": 60, "tags": ["demo"]})
    value = cache.get("greeting")  # dict or None
    cache.expire_tag("demo")        # invalidate by tag
```

#### Sync Client
```python
from vercel.cache import RuntimeCache

cache = RuntimeCache(namespace="demo")

def main():
    cache.delete("greeting")
    cache.set("greeting", {"hello": "world"}, {"ttl": 60, "tags": ["demo"]})
    value = cache.get("greeting")  # dict or None
    cache.expire_tag("demo")        # invalidate by tag
```

#### Async
```python
from vercel.cache.aio import get_cache

async def main():
    cache = get_cache(namespace="demo")

    await cache.delete("greeting")
    await cache.set("greeting", {"hello": "world"}, {"ttl": 60, "tags": ["demo"]})
    value = await cache.get("greeting")  # dict or None
    await cache.expire_tag("demo")        # invalidate by tag
```

#### Async Client
```python
from vercel.cache import AsyncRuntimeCache

cache = AsyncRuntimeCache(namespace="demo")

async def main():
    await await cache.delete("greeting")
    await await cache.set("greeting", {"hello": "world"}, {"ttl": 60, "tags": ["demo"]})
    value = await cache.get("greeting")  # dict or None
    await cache.expire_tag("demo")        # invalidate by tag
```

<br/>

---

<br/>

### Vercel OIDC Tokens

```python
from typing import Callable

from fastapi import FastAPI, Request
from vercel.oidc import decode_oidc_payload, get_vercel_oidc_token
# async
# from vercel.oidc.aio import get_vercel_oidc_token

app = FastAPI()

@app.middleware("http")
async def vercel_context_middleware(request: Request, call_next: Callable):
    set_headers(request.headers)
    return await call_next(request)

@app.get("/oidc")
def oidc():
    token = get_vercel_oidc_token()
    payload = decode_oidc_payload(token)
    user_id = payload.get("user_id")
    project_id = payload.get("project_id")

    return {
        "user_id": user_id,
        "project_id" project_id,
    }
```

Notes:
- When run locally, this requires a valid Vercel CLI login on the machine running the code for refresh.
- Project info is resolved from `.vercel/project.json`.

<br/>

---

<br/>


### Blob Storage


Requires `BLOB_READ_WRITE_TOKEN` to be set as an env var or `token` to be set when constructing a client


#### Sync


```python
from vercel.blob import BlobClient

client = BlobClient()  
# or BlobClient(token="...")

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

#### Multipart Uploads

For large files, the SDK provides three approaches with different trade-offs:

##### 1. Automatic (Simplest)

The SDK handles everything automatically:

```python
from vercel.blob import auto_multipart_upload

# Synchronous
result = auto_multipart_upload(
    "large-file.bin",
    large_data,  # bytes, file object, or iterator
    part_size=8 * 1024 * 1024,  # 8MB parts (default)
)

# Asynchronous
result = await auto_multipart_upload_async(
    "large-file.bin",
    large_data,
)
```

##### 2. Uploader Pattern (Recommended)

A middle-ground that provides a clean API while giving you control over parts and concurrency:

```python
from vercel.blob import BlobClient, create_multipart_uploader

# Create the uploader (initializes the upload)
client = BlobClient()
uploader = client.create_multipart_uploader("large-file.bin", content_type="application/octet-stream")

# Upload parts (you control when and how)
parts = []
for i, chunk in enumerate(chunks, start=1):
    part = uploader.upload_part(i, chunk)
    parts.append(part)

# Complete the upload
result = uploader.complete(parts)
```

Async version with concurrent uploads:

```python
from vercel.blob import AsyncBlobClient, create_multipart_uploader_async

client = AsyncBlobClient()
uploader = await client.create_multipart_uploader("large-file.bin")

# Upload parts concurrently
tasks = [uploader.upload_part(i, chunk) for i, chunk in enumerate(chunks, start=1)]
parts = await asyncio.gather(*tasks)

# Complete
result = await uploader.complete(parts)
```

The uploader pattern is ideal when you:
- Want to control how parts are created (e.g., stream from disk, manage memory)
- Need custom concurrency control
- Want a cleaner API than the manual approach

Notes:
- Part numbers must be in the range 1..10,000.
- `add_random_suffix` defaults to True for the uploader (matches TS SDK); manual create defaults to False.
- Abort/cancel: an abortable uploader API is not yet exposed (future enhancement).

##### 3. Manual (Most Control)

Full control over each step, but more verbose:

```python
from vercel.blob import (
    create_multipart_upload,
    upload_part,
    complete_multipart_upload,
)

# Phase 1: Create
resp = create_multipart_upload("large-file.bin")
upload_id = resp["uploadId"]
key = resp["key"]

# Phase 2: Upload parts
part1 = upload_part(
    "large-file.bin",
    chunk1,
    upload_id=upload_id,
    key=key,
    part_number=1,
)
part2 = upload_part(
    "large-file.bin",
    chunk2,
    upload_id=upload_id,
    key=key,
    part_number=2,
)

# Phase 3: Complete
result = complete_multipart_upload(
    "large-file.bin",
    [part1, part2],
    upload_id=upload_id,
    key=key,
)
```

See `examples/multipart_uploader.py` for complete working examples.

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