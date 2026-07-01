from __future__ import annotations

from collections.abc import Iterable

from vercel.queue import asgi_app, subscribe


@subscribe(topic="logs", consumer_group=f"api/{__name__}.py")
def index_logs(payload: Iterable[str]) -> None:
    print("".join(payload), end="")


app = asgi_app()
