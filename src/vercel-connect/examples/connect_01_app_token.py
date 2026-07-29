#!/usr/bin/env python3
"""Mint an app-scoped credential and call the upstream provider with it.

This is the whole point of Connect: no provider secret is stored anywhere, and
the credential is minted against the deployment's own identity.
"""

import asyncio

import httpx
from _shared import HINTS, describe_error, load_environment, mask, require_connector

from vercel.connect import ConnectAppTokenSubject, ConnectError, get_token_response

load_environment()


async def main() -> None:
    connector = require_connector()

    try:
        response = await get_token_response(connector, subject=ConnectAppTokenSubject())
    except ConnectError as error:
        print(describe_error(error))
        print(HINTS)
        raise SystemExit(1) from error

    print(f"connector:   {response.connector.uid} ({response.connector.type})")
    print(f"token:       {mask(response.token)}")
    print(f"token id:    {response.token_id}")
    print(f"expires at:  {response.expires_at.isoformat()}")
    print(f"installation:{response.installation_id}")

    # The credential is an ordinary bearer token; Connect is not a proxy, so the
    # provider call is a plain HTTP request that you make yourself.
    async with httpx.AsyncClient(timeout=10) as client:
        probe = await client.get(
            "https://api.github.com/rate_limit",
            headers={"Authorization": f"Bearer {response.token}"},
        )
    print(f"\nprovider probe: HTTP {probe.status_code}")
    print("(swap the URL for whichever provider this connector targets)")


if __name__ == "__main__":
    asyncio.run(main())
