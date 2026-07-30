from __future__ import annotations

from dataclasses import asdict
from hmac import compare_digest
from os import environ

from fastapi import FastAPI, Header, HTTPException
from scheduler import control

app = FastAPI()


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "hello world"}


def _authorize(secret: str | None) -> None:
    expected = environ.get("APSCHEDULER_CONTROL_SECRET")
    if not expected or not secret or not compare_digest(secret, expected):
        raise HTTPException(status_code=401, detail="invalid control secret")


@app.post("/scheduler/start")
def start_scheduler(
    deployment: str | None = None,
    x_control_secret: str | None = Header(default=None),
) -> dict[str, object]:
    _authorize(x_control_secret)
    return asdict(control.start(deployment=deployment))


@app.post("/scheduler/stop")
def stop_scheduler(
    deployment: str | None = None,
    x_control_secret: str | None = Header(default=None),
) -> dict[str, object]:
    _authorize(x_control_secret)
    return asdict(control.stop(deployment=deployment))
