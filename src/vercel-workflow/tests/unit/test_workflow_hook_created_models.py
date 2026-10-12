"""The wire shape of ``hook_created``.

A backend reads a ``hook_created`` without ``isWebhook`` as a legacy webhook:
the hook is then resumable through the public webhook endpoint, and refused by
the TypeScript SDK's ``resumeHook(token)``, which does not deliver to webhooks.
This SDK has no webhooks, so every hook it creates says so explicitly.
"""

from __future__ import annotations

from vercel.workflow._internal import world as w


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
