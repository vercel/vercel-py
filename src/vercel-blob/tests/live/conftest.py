"""Blob stores for live tests and examples.

Each store is a set of environment overrides, so live tests and examples resolve
credentials through the SDK default path. The default store comes from
``BLOB_READ_WRITE_TOKEN`` or ``BLOB_STORE_ID`` with Vercel OIDC, and
``BLOB_TEST_ACCESS`` names its access mode. The same variables under a
``BLOB_PUBLIC_`` or ``BLOB_PRIVATE_`` prefix add a store with that access.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pytest

from vercel import blob

_ACCESS: dict[str, blob.Access] = {"public": "public", "private": "private"}
_PREFIXED_ACCESS: dict[str, blob.Access] = {"BLOB_PUBLIC": "public", "BLOB_PRIVATE": "private"}
_MISSING = (
    "requires BLOB_READ_WRITE_TOKEN or BLOB_STORE_ID with Vercel OIDC, "
    "or the same variables prefixed with BLOB_PUBLIC_ or BLOB_PRIVATE_"
)


@dataclass(frozen=True)
class LiveStore:
    prefix: str
    access: blob.Access
    env: dict[str, str | None]


def _default_store() -> LiveStore | None:
    if not any(
        os.getenv(name)
        for name in ("BLOB_READ_WRITE_TOKEN", "VERCEL_BLOB_READ_WRITE_TOKEN", "BLOB_STORE_ID")
    ):
        return None
    access = _ACCESS.get(os.getenv("BLOB_TEST_ACCESS", "public"))
    if access is None:
        raise pytest.UsageError("BLOB_TEST_ACCESS must be public or private")
    return LiveStore("BLOB", access, {"BLOB_TEST_ACCESS": access})


def _prefixed_store(prefix: str, access: blob.Access) -> LiveStore | None:
    store_id = os.getenv(f"{prefix}_STORE_ID")
    token = os.getenv(f"{prefix}_READ_WRITE_TOKEN")
    if not (store_id or token):
        return None
    env = {
        "BLOB_STORE_ID": store_id,
        "BLOB_READ_WRITE_TOKEN": token,
        "VERCEL_BLOB_READ_WRITE_TOKEN": None,
        "BLOB_TEST_ACCESS": access,
    }
    return LiveStore(prefix, access, env)


def _store_params() -> list[Any]:
    stores = [_default_store()]
    stores += [_prefixed_store(prefix, access) for prefix, access in _PREFIXED_ACCESS.items()]
    params = [pytest.param(s, id=f"{s.access}-{s.prefix}") for s in stores if s is not None]
    return params or [pytest.param(None, id="no-store", marks=pytest.mark.skip(reason=_MISSING))]


@pytest.fixture(params=_store_params())
def store(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> LiveStore:
    live_store: LiveStore = request.param
    for name, value in live_store.env.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    return live_store
