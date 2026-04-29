# Vercel Blob

`vercel.blob` provides Vercel Blob upload, download, metadata, listing, copy,
folder, delete, and multipart helpers.

## Async Client

```python
from vercel.blob import AsyncBlobClient


async def main() -> None:
    async with AsyncBlobClient() as client:
        uploaded = await client.put(
            "avatars/user-123.txt",
            b"hello",
            access="public",
            content_type="text/plain",
        )
        metadata = await client.head(uploaded.url)
        content = await client.get(uploaded.url)
        listing = await client.list_objects(prefix="avatars/")
        await client.delete([item.url for item in listing.blobs])
```

## Files

```python
from vercel.blob import AsyncBlobClient


async def main() -> None:
    async with AsyncBlobClient() as client:
        uploaded = await client.upload_file(
            "./avatar.png",
            "avatars/user-123.png",
            access="public",
            content_type="image/png",
        )
        await client.download_file(uploaded.url, "/tmp/user-123.png", overwrite=True)
```

Use `BlobClient` or sync functions in `vercel.blob` for synchronous code.
