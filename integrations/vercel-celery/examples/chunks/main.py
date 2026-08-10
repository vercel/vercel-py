from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from tasks import add

RESULT_TIMEOUT_SECONDS = 30

app = FastAPI()


@app.get("/enqueue", response_class=PlainTextResponse)
def enqueue_chunks() -> str:
    result = add.chunks(zip(range(100), range(100), strict=False), 10).apply_async()
    print(f"queued chunk group {result.id}")

    values = result.get(
        timeout=RESULT_TIMEOUT_SECONDS,
        disable_sync_subtasks=False,
    )
    print(f"chunk group {result.id} results: {values}")
    return f"chunk group {result.id}\nresults: {values}\n"
