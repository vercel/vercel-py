from __future__ import annotations

from hmac import compare_digest
from os import environ

from fastapi import FastAPI, Header, HTTPException
from scheduler import scheduler

app = FastAPI()


@app.get("/")
def list_jobs() -> list[dict[str, str]]:
    return [{"id": job.id, "trigger": str(job.trigger)} for job in scheduler.get_jobs()]


def _authorize(secret: str | None) -> None:
    expected = environ.get("APSCHEDULER_ADMIN_SECRET")
    if not expected or not secret or not compare_digest(secret, expected):
        raise HTTPException(status_code=401, detail="invalid admin secret")


@app.post("/jobs/pause")
def pause_jobs(x_admin_secret: str | None = Header(default=None)) -> dict[str, str]:
    _authorize(x_admin_secret)
    scheduler.pause()
    return {"state": "paused"}


@app.post("/jobs/resume")
def resume_jobs(x_admin_secret: str | None = Header(default=None)) -> dict[str, str]:
    _authorize(x_admin_secret)
    scheduler.resume()
    return {"state": "running"}
