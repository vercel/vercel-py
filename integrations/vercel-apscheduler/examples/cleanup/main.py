from __future__ import annotations

from hmac import compare_digest
from os import environ

from fastapi import FastAPI, Header, HTTPException
from scheduler import scheduler

app = FastAPI()


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "hello world"}


def _authorize(secret: str | None) -> None:
    expected = environ.get("APSCHEDULER_ADMIN_SECRET")
    if not expected or not secret or not compare_digest(secret, expected):
        raise HTTPException(status_code=401, detail="invalid admin secret")


@app.post("/scheduler/start")
def start_scheduler(
    x_admin_secret: str | None = Header(default=None),
) -> dict[str, str]:
    _authorize(x_admin_secret)
    scheduler.start()
    return {"state": "running"}


@app.post("/scheduler/pause")
def pause_scheduler(
    x_admin_secret: str | None = Header(default=None),
) -> dict[str, str]:
    _authorize(x_admin_secret)
    scheduler.pause()
    return {"state": "paused"}


@app.post("/scheduler/resume")
def resume_scheduler(
    x_admin_secret: str | None = Header(default=None),
) -> dict[str, str]:
    _authorize(x_admin_secret)
    scheduler.resume()
    return {"state": "running"}
