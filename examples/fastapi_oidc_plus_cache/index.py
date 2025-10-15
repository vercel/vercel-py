from __future__ import annotations

from typing import Callable

from fastapi import FastAPI, Request, HTTPException
import logging
import traceback

from vercel.cache import AsyncRuntimeCache

from vercel.headers import geolocation, ip_address, set_headers
from vercel.oidc import get_vercel_oidc_token, decode_oidc_payload


logger = logging.getLogger(__name__)
app = FastAPI()


cache = AsyncRuntimeCache()


@app.middleware("http")
async def vercel_context_middleware(request: Request, call_next: Callable):
    headers = {k.lower(): v for k, v in request.headers.items()}
    set_headers(headers)
    response = await call_next(request)
    return response


@app.get("/api/oidc")
async def oidc_info():
    try:
        token = await get_vercel_oidc_token()
        payload = decode_oidc_payload(token)
        return {"sub": payload.get("sub"), "exp": payload.get("exp")}
    except Exception as e:
        logger.error(f"Error getting OIDC info: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error getting OIDC info")


@app.get("/api/cache")
async def cache_demo():
    key = "hit"
    val = await cache.get(key)
    if val is None:
        await cache.set(key, {"count": 1}, {"ttl": 60, "tags": ["demo"]})
        val = {"count": 1}
    else:
        count = int(val.get("count", 0)) + 1 if isinstance(val, dict) else 1
        await cache.set(key, {"count": count}, {"ttl": 60, "tags": ["demo"]})
        val = {"count": count}
    return val


@app.get("/test")
async def test():
    # Test the core logic: OIDC and cache endpoints
    # Test OIDC logic
    token = await get_vercel_oidc_token()
    payload = decode_oidc_payload(token)
    oidc_sub = payload.get("sub")
    oidc_exp = payload.get("exp")

    # Test cache logic
    key = "test:logic"
    await cache.delete(key)
    val = await cache.get(key)
    cache_result = {"before": val}
    await cache.set(key, {"foo": "bar"}, {"ttl": 10, "tags": ["logic"]})
    after_val = await cache.get(key)
    cache_result["after"] = after_val

    return {
        "oidc": {"sub": oidc_sub, "exp": oidc_exp},
        "cache": cache_result,
    }


@app.get("/api/geo")
async def geo_info(request: Request):
    """Return geolocation info inferred from request headers."""
    info = geolocation(request)
    return info


@app.get("/api/ip")
async def ip_info(request: Request):
    """Return client IP inferred from request headers."""
    ip = ip_address(request.headers)
    return {"ip": ip}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
