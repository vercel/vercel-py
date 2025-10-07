from __future__ import annotations

from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from vercel.functions import set_headers
from vercel.functions.cache import get_cache
from vercel.oidc import get_vercel_oidc_token, decode_oidc_payload


app = FastAPI()


@app.middleware("http")
async def vercel_context_middleware(request: Request, call_next: Callable):
    headers = {k.lower(): v for k, v in request.headers.items()}
    set_headers(headers)
    response = await call_next(request)
    return response


@app.get("/api/oidc")
async def oidc_info():
    token = await get_vercel_oidc_token()
    payload = decode_oidc_payload(token)
    return JSONResponse({"sub": payload.get("sub"), "exp": payload.get("exp")})


@app.get("/api/cache")
async def cache_demo():
    cache = get_cache(namespace="fastapi-e2e")
    key = "hit"
    val = await cache.get(key)
    if val is None:
        await cache.set(key, {"count": 1}, {"ttl": 60, "tags": ["demo"]})
        val = {"count": 1}
    else:
        count = int(val.get("count", 0)) + 1 if isinstance(val, dict) else 1
        await cache.set(key, {"count": count}, {"ttl": 60, "tags": ["demo"]})
        val = {"count": count}
    return JSONResponse(val)


