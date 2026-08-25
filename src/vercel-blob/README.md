# Vercel Blob Python SDK

`vercel-blob` provides asynchronous and synchronous file-like APIs for Vercel Blob.

Install it independently:

```sh
pip install vercel-blob
```

Configure either `BLOB_READ_WRITE_TOKEN` (or the historical
`VERCEL_BLOB_READ_WRITE_TOKEN`) or Vercel OIDC credentials paired with `BLOB_STORE_ID`.

The primary API at `vercel.blob` is asynchronous:

```python
from vercel import blob

async with blob.open("reports/today.txt", "w", content_type="text/plain") as writer:
    await writer.write("ready\n")

async with blob.open("reports/today.txt") as reader:
    print(await reader.read())

metadata = await blob.stat("reports/today.txt")
await blob.remove(metadata.pathname)
```

The synchronous mirror lives at `vercel.blob.sync`:

```python
from vercel.blob import sync as blob

with blob.open("artifacts/data.bin", "wb") as writer:
    writer.write(b"data")

with blob.open("artifacts/data.bin", "rb") as reader:
    assert reader.read() == b"data"

blob.remove("artifacts/data.bin")
```

Currently supports `r`, `rb`, `w`, and `wb`. Text modes accept `encoding`, `errors`, and
`newline`; binary modes reject those arguments. Pass `access="private"` when writing to a
private Blob store.

Writers stage data locally and publish it when `close()` succeeds. A normal context-manager
exit closes and publishes the object. An exceptional exit aborts the staged write, so use a
context manager whenever publication and cleanup should be coupled. Readers and writers are
owned by the SDK session in which they were opened and reject I/O after that session closes.

Use unique pathnames in scripts and tests, and call `remove(pathname, missing_ok=True)` from a
`finally` block for best-effort cleanup. Complete runnable lifecycle examples are in
`examples/blob_async_text.py` and `examples/blob_sync_binary.py`. The standalone
`examples/blob_sandbox_streaming.py` example copies in bounded chunks from a local file to
Blob, from Blob to a Sandbox, and from the transformed Sandbox result back through Blob to a
local file. It requires both `vercel-blob` and `vercel-sandbox` plus credentials for both
services.
