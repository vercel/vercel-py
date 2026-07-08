from __future__ import annotations

from collections.abc import AsyncIterable

from vercel.queue import asgi_app, subscribe


@subscribe(topic="files", consumer_group=f"api/{__name__}.py")
async def archive_file(payload: AsyncIterable[bytes]) -> None:
    size = 0
    async for chunk in payload:
        size += len(chunk)
    print(f"received {size} bytes")


app = asgi_app()
