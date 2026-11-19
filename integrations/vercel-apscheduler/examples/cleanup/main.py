from __future__ import annotations

from fastapi import FastAPI
from scheduler import scheduler

app = FastAPI()


@app.get("/")
def list_jobs() -> list[dict[str, str]]:
    return [{"id": job.id, "trigger": str(job.trigger)} for job in scheduler.get_jobs()]
