from __future__ import annotations

import time
from itertools import starmap

from django.http import HttpRequest, HttpResponse
from django.tasks import TaskResultStatus

from .tasks import add

RESULT_TIMEOUT_SECONDS = 30


def send_chunks(request: HttpRequest) -> HttpResponse:
    del request
    pairs = zip(range(100), range(100), strict=False)
    results = list(starmap(add.enqueue, pairs))
    deadline = time.monotonic() + RESULT_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        for result in results:
            if not result.is_finished:
                result.refresh()
        if all(result.is_finished for result in results):
            break
        time.sleep(0.25)

    if not all(result.is_finished for result in results):
        finished = sum(result.is_finished for result in results)
        return HttpResponse(
            f"queued {len(results)} add tasks\nfinished {finished}/{len(results)} before timeout\n",
            content_type="text/plain",
            status=504,
        )

    values = [
        result.return_value for result in results if result.status == TaskResultStatus.SUCCESSFUL
    ]
    return HttpResponse(
        f"queued {len(results)} add tasks\nresults: {values}\n",
        content_type="text/plain",
    )
