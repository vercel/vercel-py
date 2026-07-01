from __future__ import annotations

from django.tasks import task


@task
def add(left: int, right: int) -> int:
    return left + right
