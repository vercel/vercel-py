"""The wire shape of ``hook_created``.

A backend reads a ``hook_created`` without ``isWebhook`` as a legacy webhook:
the hook is then resumable through the public webhook endpoint, and refused by
the TypeScript SDK's ``resumeHook(token)``, which does not deliver to webhooks.
This SDK has no webhooks, so every hook it creates says so explicitly.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import cbor2
import httpx2 as httpx

from vercel._internal.core.polyfills import UTC
from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds.vercel import VercelWorld


def test_hook_created_always_says_it_is_not_a_webhook() -> None:
    assert w.HookCreatedEventData(token="tok").model_dump() == {
        "token": "tok",
        "isWebhook": False,
    }


def test_system_hook_created_is_not_a_webhook_either() -> None:
    assert w.HookCreatedEventData(token="tok", is_system=True).model_dump() == {
        "token": "tok",
        "isWebhook": False,
        "isSystem": True,
    }


async def test_vercel_transports_retention_as_cbor_datetime(respx_mock) -> None:
    world = VercelWorld(token="test-token")
    route = respx_mock.post(f"{world._base_url}/v3/runs/wrun_test/events").mock(
        return_value=httpx.Response(
            200, content=cbor2.dumps({}), headers={"content-type": "application/cbor"}
        )
    )
    deadline = datetime(2026, 9, 22, tzinfo=UTC) + timedelta(days=1)
    await world.events_create(
        "wrun_test",
        w.HookCreatedEventData(token="order:retained", token_retention_until=deadline).into_event(
            "hook_test"
        ),
    )
    assert (
        cbor2.loads(route.calls.last.request.content)["eventData"]["tokenRetentionUntil"]
        == deadline
    )
