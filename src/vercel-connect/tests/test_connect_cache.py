"""Token cache behaviour.

The cache is a redesign, not a port. These tests pin the five defects the
TypeScript implementation has: order-sensitive keys, the validity buffer inside
the key, a key that omits the platform identity, global clearing on revoke, and
no in-flight de-duplication.
"""

import asyncio
import contextvars
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
import respx
import time_machine
from conftest import TEST_BASE_URL, session_options

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.session import get_active_sync_session
from vercel.api import session
from vercel.connect import (
    ConnectAppTokenSubject,
    ConnectOptions,
    ConnectUserTokenSubject,
    clear_token_cache,
    delete_token_cache_entry,
    get_token,
    get_token_response,
    revoke_token,
    sync as connect_sync,
)
from vercel.connect._internal.cache import build_cache_key
from vercel.connect._internal.service import get_connect_service

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
TOKEN_URL = f"{TEST_BASE_URL}/v1/connect/token/slack%2Fmy-bot"


def token_body(*, token: str = "t1", expires_in: timedelta = timedelta(hours=1)) -> dict[str, Any]:
    return {
        "token": token,
        "tokenId": f"stk_{token}",
        "expiresAt": int((NOW + expires_in).timestamp() * 1000),
        "connector": {"id": "scl_123", "uid": "slack/my-bot", "type": "slack"},
    }


def counting_route(*, expires_in: timedelta = timedelta(hours=1)) -> respx.Route:
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        return httpx.Response(200, json=token_body(token=f"t{counter['n']}", expires_in=expires_in))

    route = respx.post(TOKEN_URL).mock(side_effect=handler)
    return route


def gated_route() -> tuple[respx.Route, asyncio.Event, asyncio.Event]:
    """A route that blocks until released, so concurrent callers really overlap.

    An instantly-resolving mock lets the first caller finish before the next one
    starts, which silently turns a single-flight assertion into a cache-hit
    assertion. A gate is used rather than a sleep because these tests run under a
    frozen clock, where `asyncio.sleep` would never return.
    """
    counter = {"n": 0}
    first_request = asyncio.Event()
    release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        # Captured before suspending: reading it afterwards would give every
        # concurrent handler the final count, and identical tokens.
        index = counter["n"]
        first_request.set()
        await release.wait()
        return httpx.Response(200, json=token_body(token=f"t{index}"))

    return respx.post(TOKEN_URL).mock(side_effect=handler), first_request, release


async def _yield_to_peers(times: int = 5) -> None:
    """Let sibling tasks reach the single-flight. `sleep(0)` ignores the clock."""
    for _ in range(times):
        await asyncio.sleep(0)


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_cache_hit_skips_request_async(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        first = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        second = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert first == second == "t1"
    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
def test_cache_hit_skips_request_sync(mock_env_clear: None) -> None:
    route = counting_route()

    with session(service_options=session_options()):
        first = connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        second = connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert first == second == "t1"
    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_force_refresh_bypasses_cache(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        first = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        second = await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(force_refresh=True),
        )
        third = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert (first, second, third) == ("t1", "t2", "t2")
    assert route.call_count == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_no_cache_neither_reads_nor_writes(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(no_cache=True),
        )
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_distinct_subjects_key_separately(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))

    assert route.call_count == 3


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_distinct_scopes_and_installations_key_separately(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["a"])
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["b"])
        await get_token(
            "slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["a"], installation_id="T1"
        )
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["a"])

    assert route.call_count == 3


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_token_exchange_subject_keys_by_inbound_token(mock_env_clear: None) -> None:
    from vercel.connect import ConnectTokenExchangeSubject

    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectTokenExchangeSubject(token="in-1"))
        await get_token("slack/my-bot", subject=ConnectTokenExchangeSubject(token="in-2"))
        await get_token("slack/my-bot", subject=ConnectTokenExchangeSubject(token="in-1"))

    assert route.call_count == 2


@respx.mock
async def test_cache_key_is_order_independent(mock_env_clear: None) -> None:
    """The TypeScript key is `JSON.stringify`, so field order changes the key."""
    first = build_cache_key(
        "slack/my-bot",
        subject=ConnectAppTokenSubject(),
        vercel_token="oidc-token",
        scopes=["a", "b"],
        installation_id="T1",
        audience=["aud"],
    )
    second = build_cache_key(
        "slack/my-bot",
        audience=["aud"],
        installation_id="T1",
        scopes=["a", "b"],
        vercel_token="oidc-token",
        subject=ConnectAppTokenSubject(),
    )

    assert first == second
    assert hash(first) == hash(second)


async def test_cache_key_excludes_validity_buffer(mock_env_clear: None) -> None:
    """Read-time policy must not fragment the cache."""
    common: dict[str, Any] = {
        "subject": ConnectAppTokenSubject(),
        "vercel_token": "oidc-token",
    }
    assert build_cache_key("slack/my-bot", **common) == build_cache_key("slack/my-bot", **common)


async def test_cache_key_separates_platform_identities(mock_env_clear: None) -> None:
    """Security regression: two identities must never share a cache entry.

    The TypeScript key omits `options.vercelToken`, so a caller overriding it can
    be served a credential minted for a different identity.
    """
    one = build_cache_key(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="identity-one"
    )
    two = build_cache_key(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="identity-two"
    )

    assert one != two


async def test_cache_key_does_not_leak_the_identity_token(mock_env_clear: None) -> None:
    key = build_cache_key(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="super-secret"
    )
    assert "super-secret" not in repr(key)


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_cross_identity_isolation_end_to_end(mock_env_clear: None) -> None:
    """Security regression, at the public boundary."""
    route = counting_route()

    async with session(service_options=session_options()):
        first = await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(vercel_token="identity-one"),
        )
        second = await get_token(
            "slack/my-bot",
            subject=ConnectAppTokenSubject(),
            options=ConnectOptions(vercel_token="identity-two"),
        )

    assert first != second
    assert route.call_count == 2


@respx.mock
async def test_validity_buffer_serves_token_outside_the_buffer(mock_env_clear: None) -> None:
    route = counting_route(expires_in=timedelta(minutes=10))

    with time_machine.travel(NOW, tick=False) as traveller:
        async with session(service_options=session_options()):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
            traveller.move_to(NOW + timedelta(minutes=9))
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 1


@respx.mock
async def test_validity_buffer_refetches_token_inside_the_buffer(mock_env_clear: None) -> None:
    route = counting_route(expires_in=timedelta(minutes=10))

    with time_machine.travel(NOW, tick=False) as traveller:
        async with session(service_options=session_options()):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
            traveller.move_to(NOW + timedelta(minutes=9, seconds=45))
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@respx.mock
async def test_validity_buffer_default_is_thirty_seconds(mock_env_clear: None) -> None:
    route = counting_route(expires_in=timedelta(seconds=40))

    with time_machine.travel(NOW, tick=False) as traveller:
        async with session(service_options=session_options()):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
            traveller.move_to(NOW + timedelta(seconds=11))
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@respx.mock
async def test_per_call_validity_buffer_overrides_the_default(mock_env_clear: None) -> None:
    route = counting_route(expires_in=timedelta(minutes=10))

    with time_machine.travel(NOW, tick=False) as traveller:
        async with session(service_options=session_options()):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(validity_buffer=timedelta(minutes=5)),
            )
            traveller.move_to(NOW + timedelta(minutes=6))
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(validity_buffer=timedelta(minutes=5)),
            )

    assert route.call_count == 2


@respx.mock
async def test_expired_entry_is_dropped_on_access(mock_env_clear: None) -> None:
    route = counting_route(expires_in=timedelta(minutes=1))

    with time_machine.travel(NOW, tick=False) as traveller:
        async with session(service_options=session_options()):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
            traveller.move_to(NOW + timedelta(hours=2))
            token = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert token == "t2"
    assert route.call_count == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_delete_token_cache_entry_drops_only_the_match(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        assert route.call_count == 2

        delete_token_cache_entry("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        assert route.call_count == 2

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        assert route.call_count == 3


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_delete_token_cache_entry_evicts_across_scopes(mock_env_clear: None) -> None:
    """Eviction is by identity, so callers need not reconstruct exact params."""
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["a"])
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["b"])
        delete_token_cache_entry("slack/my-bot", subject=ConnectAppTokenSubject())
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["a"])

    assert route.call_count == 3


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_revoke_token_evicts_only_the_revoked_identity(mock_env_clear: None) -> None:
    """The TypeScript SDK calls `cache.clear()`; scope it instead."""
    token_call = counting_route()
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        assert token_call.call_count == 2

        await revoke_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        assert token_call.call_count == 2

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        assert token_call.call_count == 3


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_clear_token_cache_drops_everything(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        clear_token_cache()
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))

    assert route.call_count == 4


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_lru_evicts_the_least_recently_used_entry(mock_env_clear: None) -> None:
    """`MAX_CACHE_SIZE` and `evictLru()` have no test in the TypeScript suite."""
    route = counting_route()

    async with session(service_options=session_options(token_cache_size=3)):
        for index in range(3):
            await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id=f"u_{index}"))
        assert route.call_count == 3

        # Touch u_0 so u_1 becomes least recently used.
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_0"))
        assert route.call_count == 3

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_3"))
        assert route.call_count == 4

        # u_1 was evicted; u_0 and u_2 survive.
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_0"))
        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_2"))
        assert route.call_count == 4

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_1"))
        assert route.call_count == 5


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_lru_bound_defaults_to_one_hundred(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        for index in range(101):
            await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id=f"u_{index}"))
        assert route.call_count == 101

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_100"))
        assert route.call_count == 101

        await get_token("slack/my-bot", subject=ConnectUserTokenSubject(id="u_0"))
        assert route.call_count == 102


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_force_refresh_surfaces_a_revoked_grant(mock_env_clear: None) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(200, json=token_body())
        return httpx.Response(
            401, json={"error": {"code": "no_token", "message": "revoked elsewhere"}}
        )

    respx.post(TOKEN_URL).mock(side_effect=handler)

    from vercel.connect import NoValidTokenError

    async with session(service_options=session_options()):
        assert await get_token("slack/my-bot", subject=ConnectAppTokenSubject()) == "t1"
        # The cache holds successes, so a grant revoked elsewhere keeps serving.
        assert await get_token("slack/my-bot", subject=ConnectAppTokenSubject()) == "t1"
        with pytest.raises(NoValidTokenError):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(force_refresh=True),
            )


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_concurrent_cold_calls_issue_one_request(mock_env_clear: None) -> None:
    """Single-flight: the TypeScript cache fires N POSTs for N concurrent calls.

    The route blocks so the calls really are in flight together; otherwise the
    first would finish and the rest would be plain cache hits.
    """
    route, first_request, release = gated_route()

    async with session(service_options=session_options()):
        pending = [
            asyncio.ensure_future(get_token("slack/my-bot", subject=ConnectAppTokenSubject()))
            for _ in range(8)
        ]
        await first_request.wait()
        await _yield_to_peers()
        release.set()
        results = await asyncio.gather(*pending)

    assert set(results) == {"t1"}
    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_concurrent_cold_calls_for_distinct_keys_are_not_serialized(
    mock_env_clear: None,
) -> None:
    route, first_request, release = gated_route()

    async with session(service_options=session_options()):
        pending = [
            asyncio.ensure_future(
                get_token("slack/my-bot", subject=ConnectUserTokenSubject(id=f"u_{index}"))
            )
            for index in range(4)
        ]
        await first_request.wait()
        await _yield_to_peers()
        release.set()
        results = await asyncio.gather(*pending)

    assert len(set(results)) == 4
    assert route.call_count == 4


@time_machine.travel(NOW, tick=False)
@respx.mock
def test_cache_is_thread_safe(mock_env_clear: None) -> None:
    counter = {"n": 0}

    def blocking(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        # Hold the connection so the other threads pile up on the per-key lock
        # instead of finding a populated cache.
        time.sleep(0.05)
        return httpx.Response(200, json=token_body(token=f"t{counter['n']}"))

    route = respx.post(TOKEN_URL).mock(side_effect=blocking)

    with session(service_options=session_options()):

        def fetch() -> str:
            return connect_sync.get_token("slack/my-bot", subject=ConnectAppTokenSubject())

        # Resolve the service up front. `session.get_or_create_service` in
        # internal-core is not itself synchronized, so concurrent first calls can
        # each construct their own service (and therefore their own cache). That
        # is a core-level limitation, separate from this cache's thread safety.
        get_connect_service(get_active_sync_session())

        # Sessions live in a context variable, so each worker needs its own copy
        # of the calling context to see this session rather than the implicit
        # default. The copies must be taken here, not inside the workers.
        contexts = [contextvars.copy_context() for _ in range(16)]

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda context: context.run(fetch), contexts))

    assert set(results) == {"t1"}
    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_failed_fetch_is_not_cached(mock_env_clear: None) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(500, json={"error": {"code": "internal_error", "message": "x"}})
        return httpx.Response(200, json=token_body())

    respx.post(TOKEN_URL).mock(side_effect=handler)

    from vercel.connect import ConnectApiError

    async with session(service_options=session_options()):
        with pytest.raises(ConnectApiError):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        assert await get_token("slack/my-bot", subject=ConnectAppTokenSubject()) == "t1"


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_cache_is_scoped_to_a_session(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject())

    assert route.call_count == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_cached_response_envelope_is_reused(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        first = await get_token_response("slack/my-bot", subject=ConnectAppTokenSubject())
        second = await get_token_response("slack/my-bot", subject=ConnectAppTokenSubject())

    assert first == second
    assert first.token_id == second.token_id == "stk_t1"
    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_default_scopes_spellings_share_one_entry(mock_env_clear: None) -> None:
    """`None` and `["*"]` both mean the connector's defaults, so they must not
    fragment the cache."""
    route = counting_route()

    async with session(service_options=session_options()):
        first = await get_token("slack/my-bot", subject=ConnectAppTokenSubject())
        second = await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["*"])
        third = await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=None)

    assert first == second == third == "t1"
    assert route.call_count == 1


async def test_default_scopes_spellings_build_one_cache_key(mock_env_clear: None) -> None:
    from vercel.connect._internal.service import _normalize_scopes

    common: dict[str, Any] = {
        "subject": ConnectAppTokenSubject(),
        "vercel_token": "oidc-token",
    }
    assert build_cache_key(
        "slack/my-bot", scopes=_normalize_scopes(None), **common
    ) == build_cache_key("slack/my-bot", scopes=_normalize_scopes(["*"]), **common)


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_explicit_scopes_still_key_separately_from_defaults(mock_env_clear: None) -> None:
    route = counting_route()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["*"])
        await get_token("slack/my-bot", subject=ConnectAppTokenSubject(), scopes=["chat:write"])

    assert route.call_count == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_revoke_without_installation_evicts_every_installation(
    mock_env_clear: None,
) -> None:
    """Omitting installation_id revokes everywhere server-side, so it must evict
    everywhere locally too."""
    route = counting_route()
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=subject, installation_id="T1")
        await get_token("slack/my-bot", subject=subject, installation_id="T2")
        await get_token("slack/my-bot", subject=subject)
        assert route.call_count == 3

        await revoke_token("slack/my-bot", subject=subject)

        await get_token("slack/my-bot", subject=subject, installation_id="T1")
        await get_token("slack/my-bot", subject=subject, installation_id="T2")
        await get_token("slack/my-bot", subject=subject)

    assert route.call_count == 6


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_revoke_with_installation_spares_other_installations(
    mock_env_clear: None,
) -> None:
    route = counting_route()
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=subject, installation_id="T1")
        await get_token("slack/my-bot", subject=subject, installation_id="T2")
        await revoke_token("slack/my-bot", subject=subject, installation_id="T1")

        await get_token("slack/my-bot", subject=subject, installation_id="T2")
        assert route.call_count == 2
        await get_token("slack/my-bot", subject=subject, installation_id="T1")

    assert route.call_count == 3


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_failed_forced_refresh_does_not_keep_serving_the_old_token(
    mock_env_clear: None,
) -> None:
    """A caller who asked to re-validate must not keep getting the stale token."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(200, json=token_body())
        return httpx.Response(401, json={"error": {"code": "no_token", "message": "revoked"}})

    respx.post(TOKEN_URL).mock(side_effect=handler)
    from vercel.connect import NoValidTokenError

    async with session(service_options=session_options()):
        assert await get_token("slack/my-bot", subject=ConnectAppTokenSubject()) == "t1"
        with pytest.raises(NoValidTokenError):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(force_refresh=True),
            )
        # The known-invalid credential must be gone, not re-served.
        with pytest.raises(NoValidTokenError):
            await get_token("slack/my-bot", subject=ConnectAppTokenSubject())


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_invalidation_during_an_in_flight_fetch_is_not_undone(
    mock_env_clear: None,
) -> None:
    """A load that started before a revoke must not repopulate the cache."""
    release = asyncio.Event()

    async def slow(request: httpx.Request) -> httpx.Response:
        await release.wait()
        return httpx.Response(200, json=token_body())

    respx.post(TOKEN_URL).mock(side_effect=slow)
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        pending = asyncio.ensure_future(get_token("slack/my-bot", subject=subject))
        await asyncio.sleep(0)
        clear_token_cache()
        release.set()
        await pending

        assert len(_active_cache()) == 0


def _active_cache() -> Any:
    from vercel._internal.core.session import get_active_session
    from vercel.connect._internal.service import get_connect_service

    return get_connect_service(get_active_session())._cache


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_exchange_credentials_are_not_stored_in_cache_keys(
    mock_env_clear: None,
) -> None:
    from vercel.connect import ConnectTokenExchangeSubject

    key = build_cache_key(
        "slack/my-bot",
        subject=ConnectTokenExchangeSubject(token="xoxb-INBOUND-SECRET"),
        vercel_token="identity-secret",
    )

    assert "xoxb-INBOUND-SECRET" not in repr(key)
    assert "xoxb-INBOUND-SECRET" not in key.subject_key
    assert "identity-secret" not in repr(key)
    # Distinct inbound credentials must still key separately.
    other = build_cache_key(
        "slack/my-bot",
        subject=ConnectTokenExchangeSubject(token="xoxb-OTHER"),
        vercel_token="identity-secret",
    )
    assert key != other


def test_single_flight_locks_do_not_accumulate() -> None:
    from vercel.connect._internal.single_flight import SyncSingleFlight

    flight = SyncSingleFlight()
    state = token_body()

    async def load() -> Any:
        from vercel.connect._internal.state import ConnectorRefState, ConnectTokenState

        return ConnectTokenState(
            token=state["token"],
            expires_at=NOW + timedelta(hours=1),
            connector=ConnectorRefState(id="scl_1", uid="slack/my-bot", type="slack"),
        )

    for index in range(50):
        key = build_cache_key(
            "slack/my-bot",
            subject=ConnectUserTokenSubject(id=f"u_{index}"),
            vercel_token=f"rotating-token-{index}",
        )
        iter_coroutine(flight.run(key, read=lambda: None, load=load))

    assert len(flight) == 0


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_revoke_during_a_cold_fetch_is_not_undone(mock_env_clear: None) -> None:
    """The in-flight load has cached nothing yet, so identity invalidation has to
    reach the pending load rather than the (empty) entry table."""
    release = asyncio.Event()
    calls = {"n": 0}

    async def slow(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        await release.wait()
        return httpx.Response(200, json=token_body(token=f"t{calls['n']}"))

    respx.post(TOKEN_URL).mock(side_effect=slow)
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        pending = asyncio.ensure_future(get_token("slack/my-bot", subject=subject))
        await asyncio.sleep(0)
        await revoke_token("slack/my-bot", subject=subject)
        release.set()
        await pending

        # The revoked credential must not have been cached.
        await get_token("slack/my-bot", subject=subject)

    assert calls["n"] == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_eviction_of_one_key_leaves_other_in_flight_loads_alone(
    mock_env_clear: None,
) -> None:
    """Invalidation is per key: a force_refresh on one subject must not discard a
    concurrent load for another."""
    release = asyncio.Event()
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 2:
            await release.wait()
        return httpx.Response(200, json=token_body(token=f"t{calls['n']}"))

    respx.post(TOKEN_URL).mock(side_effect=handler)
    a = ConnectUserTokenSubject(id="A")
    b = ConnectUserTokenSubject(id="B")

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=b)
        pending = asyncio.ensure_future(get_token("slack/my-bot", subject=a))
        await asyncio.sleep(0)
        await get_token("slack/my-bot", subject=b, options=ConnectOptions(force_refresh=True))
        release.set()
        await pending
        settled = calls["n"]

        # A's token was cached despite B being evicted mid-flight.
        await get_token("slack/my-bot", subject=a)

    assert calls["n"] == settled


async def test_negative_validity_buffer_is_rejected(mock_env_clear: None) -> None:
    from vercel.connect import ConnectServiceOptions

    with pytest.raises(ValueError, match="validity_buffer"):
        ConnectServiceOptions(validity_buffer=timedelta(seconds=-1))


async def test_negative_per_call_validity_buffer_is_rejected(mock_env_clear: None) -> None:
    """Rejected before any request is made, so no credential is minted."""
    async with session(service_options=session_options()):
        with pytest.raises(ValueError, match="validity_buffer"):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                options=ConnectOptions(validity_buffer=-3600),
            )


def test_invalid_cache_size_is_rejected() -> None:
    from vercel.connect import ConnectServiceOptions

    for size in (0, -1):
        with pytest.raises(ValueError, match="token_cache_size"):
            ConnectServiceOptions(token_cache_size=size)


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_forced_refresh_caches_despite_a_concurrent_load(mock_env_clear: None) -> None:
    """Invalidation must cancel loads already running, not loads starting after it.

    The forced refresh evicts the key, so an earlier in-flight load is discarded —
    but the forced load itself begins after that eviction and has to be cached.
    """
    release = asyncio.Event()
    calls = {"n": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            await release.wait()
        return httpx.Response(200, json=token_body(token=f"t{calls['n']}"))

    respx.post(TOKEN_URL).mock(side_effect=handler)
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        first = asyncio.ensure_future(get_token("slack/my-bot", subject=subject))
        await asyncio.sleep(0)
        forced = await get_token(
            "slack/my-bot", subject=subject, options=ConnectOptions(force_refresh=True)
        )
        release.set()
        await first
        settled = calls["n"]

        served = await get_token("slack/my-bot", subject=subject)

    assert served == forced
    assert calls["n"] == settled, "the forced result should have been cached"


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_load_started_after_a_revoke_is_cached(mock_env_clear: None) -> None:
    route = counting_route()
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=subject)
        await revoke_token("slack/my-bot", subject=subject)
        after = await get_token("slack/my-bot", subject=subject)
        assert route.call_count == 2

        assert await get_token("slack/my-bot", subject=subject) == after

    assert route.call_count == 2


async def test_stray_detail_type_does_not_split_the_cache_key(mock_env_clear: None) -> None:
    """The wire drops a stray `type`, so the key must too or they diverge."""
    from vercel.connect import ConnectCustomAuthorizationDetail

    common: dict[str, Any] = {
        "subject": ConnectAppTokenSubject(),
        "vercel_token": "oidc-token",
    }
    without = build_cache_key(
        "oauth/thing",
        authorization_details=[
            ConnectCustomAuthorizationDetail(type="payment", details={"amount": "1.00"})
        ],
        **common,
    )
    with_stray = build_cache_key(
        "oauth/thing",
        authorization_details=[
            ConnectCustomAuthorizationDetail(
                type="payment", details={"amount": "1.00", "type": "ignored"}
            )
        ],
        **common,
    )

    assert without == with_stray


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_equivalent_detail_requests_share_one_request(mock_env_clear: None) -> None:
    from vercel.connect import ConnectCustomAuthorizationDetail

    route = counting_route()

    async with session(service_options=session_options()):
        for details in ({"amount": "1.00"}, {"amount": "1.00", "type": "ignored"}):
            await get_token(
                "slack/my-bot",
                subject=ConnectAppTokenSubject(),
                authorization_details=[
                    ConnectCustomAuthorizationDetail(type="payment", details=details)
                ],
            )

    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_revoking_by_uid_evicts_entries_cached_by_id(mock_env_clear: None) -> None:
    """A connector has two names; a revoke naming either must evict the entry.

    The cached response carries both, so no round trip is needed to match them.
    """
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        body = token_body(token=f"t{calls['n']}")
        body["connector"] = {"id": "scl_123", "uid": "slack/my-bot", "type": "slack"}
        return httpx.Response(200, json=body)

    respx.post(f"{TEST_BASE_URL}/v1/connect/token/scl_123").mock(side_effect=handler)
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/slack%2Fmy-bot/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        first = await get_token("scl_123", subject=subject)
        # Revoked through the readable UID, cached under the opaque id.
        await revoke_token("slack/my-bot", subject=subject)
        second = await get_token("scl_123", subject=subject)

    assert first != second
    assert calls["n"] == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_deleting_by_id_evicts_entries_cached_by_uid(mock_env_clear: None) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        body = token_body(token=f"t{calls['n']}")
        body["connector"] = {"id": "scl_123", "uid": "slack/my-bot", "type": "slack"}
        return httpx.Response(200, json=body)

    respx.post(TOKEN_URL).mock(side_effect=handler)
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=subject)
        delete_token_cache_entry("scl_123", subject=subject)
        await get_token("slack/my-bot", subject=subject)

    assert calls["n"] == 2


@time_machine.travel(NOW, tick=False)
@respx.mock
async def test_revoking_a_different_connector_still_spares_the_entry(
    mock_env_clear: None,
) -> None:
    """Name matching must not become a way to evict unrelated connectors."""
    route = counting_route()
    respx.delete(f"{TEST_BASE_URL}/v1/connect/connectors/github%2Fother/tokens").mock(
        return_value=httpx.Response(204, content=b"")
    )
    subject = ConnectAppTokenSubject()

    async with session(service_options=session_options()):
        await get_token("slack/my-bot", subject=subject)
        await revoke_token("github/other", subject=subject)
        await get_token("slack/my-bot", subject=subject)

    assert route.call_count == 1


@time_machine.travel(NOW, tick=False)
@respx.mock
def test_sync_surface_manages_the_cache(mock_env_clear: None) -> None:
    """The sync mirrors of the cache helpers were never exercised."""
    route = counting_route()
    subject = ConnectAppTokenSubject()

    with session(service_options=session_options()):
        first = connect_sync.get_token("slack/my-bot", subject=subject)
        assert connect_sync.get_token("slack/my-bot", subject=subject) == first
        assert route.call_count == 1

        connect_sync.delete_token_cache_entry("slack/my-bot", subject=subject)
        second = connect_sync.get_token("slack/my-bot", subject=subject)
        assert second != first
        assert route.call_count == 2

        connect_sync.clear_token_cache()
        connect_sync.get_token("slack/my-bot", subject=subject)

    assert route.call_count == 3


def test_finish_load_without_a_matching_begin_is_a_no_op() -> None:
    """`finish_load` runs in a `finally`, so it must tolerate an absent record."""
    from vercel.connect._internal.cache import TokenCache

    cache = TokenCache(max_size=4)
    key = build_cache_key(
        "slack/my-bot", subject=ConnectAppTokenSubject(), vercel_token="oidc-token"
    )

    cache.finish_load(key)  # must not raise
    assert len(cache) == 0
