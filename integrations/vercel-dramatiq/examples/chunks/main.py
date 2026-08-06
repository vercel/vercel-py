from __future__ import annotations

from itertools import starmap

from dramatiq.composition import group
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from tasks import add

RESULT_TIMEOUT_MILLISECONDS = 30000

app = FastAPI()


@app.get("/enqueue", response_class=PlainTextResponse)
def enqueue_chunks() -> str:
    messages = list(starmap(add.message, zip(range(100), range(100), strict=False)))
    result_group = group(messages).run()
    print("queued chunk group")

    values = list(result_group.get_results(block=True, timeout=RESULT_TIMEOUT_MILLISECONDS))
    print(f"chunk group results: {values}")
    return f"chunk group\nresults: {values}\n"
