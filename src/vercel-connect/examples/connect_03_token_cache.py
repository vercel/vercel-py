#!/usr/bin/env python3
"""Show the token cache: hits, forced refresh, and scoped eviction.

Timings make the cache visible. Tokens are short-lived, so ask for one per use
rather than holding it; the cache is what makes that cheap.
"""

import asyncio
import time
from collections.abc import Awaitable

from _shared import describe_error, load_environment, mask, require_connector

from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectError,
    ConnectOptions,
    clear_token_cache,
    delete_token_cache_entry,
    get_token,
)

load_environment()


async def timed(label: str, pending: Awaitable[str]) -> str:
    started = time.perf_counter()
    token = await pending
    elapsed_ms = (time.perf_counter() - started) * 1000
    print(f"{label:<34} {elapsed_ms:7.1f} ms  {mask(token)}")
    return token


async def main() -> None:
    connector = require_connector()
    subject = ConnectAppTokenSubject()

    try:
        await timed("cold fetch (network)", get_token(connector, subject=subject))
        await timed("cached", get_token(connector, subject=subject))
        await timed("cached again", get_token(connector, subject=subject))

        await timed(
            "force_refresh (network)",
            get_token(connector, subject=subject, options=ConnectOptions(force_refresh=True)),
        )

        # Cheaper than force_refresh on every call: evict, then retry once. This
        # is the right response to a 401 from the provider.
        delete_token_cache_entry(connector, subject=subject)
        await timed("after eviction (network)", get_token(connector, subject=subject))

        await timed(
            "no_cache (network, not stored)",
            get_token(connector, subject=subject, options=ConnectOptions(no_cache=True)),
        )
        await timed("still cached from before", get_token(connector, subject=subject))

        # A near-expiry token is treated as stale, so a large buffer forces a
        # refetch without any clock manipulation.
        await timed(
            "wide validity buffer (network)",
            get_token(
                connector,
                subject=subject,
                options=ConnectOptions(validity_buffer=86_400),
            ),
        )

        clear_token_cache()
        await timed("after clear (network)", get_token(connector, subject=subject))
    except ConnectError as error:
        print(describe_error(error))
        raise SystemExit(1) from error

    print("\nConcurrent cold calls collapse into one request (single-flight):")
    clear_token_cache()
    started = time.perf_counter()
    tokens = await asyncio.gather(*(get_token(connector, subject=subject) for _ in range(8)))
    elapsed_ms = (time.perf_counter() - started) * 1000
    print(f"  8 concurrent calls took {elapsed_ms:.1f} ms")
    print(f"  distinct credentials returned: {len(set(tokens))}")


if __name__ == "__main__":
    asyncio.run(main())
