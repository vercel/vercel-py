from __future__ import annotations

from collections.abc import Iterable

from vercel.queue import subscribe


@subscribe(topic="logs")
def index_logs(payload: Iterable[str]) -> None:
    print("".join(payload), end="")
