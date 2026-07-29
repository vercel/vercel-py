#!/usr/bin/env python3
"""Verify an inbound Connect trigger.

A connector with triggers enabled forwards provider webhooks to your project with
a Vercel OIDC token as the bearer, so you verify one thing instead of a different
signature scheme per provider.

Run with no arguments to see verification reject a forged request. Set
CONNECT_WEBHOOK_TOKEN to a real forwarded token to see it accept one.
"""

import asyncio
import os

from _shared import load_environment

from vercel.connect import ConnectWebhookVerificationError, verify_connect_webhook

load_environment()


async def main() -> None:
    if not os.environ.get("VERCEL_PROJECT_ID"):
        print("VERCEL_PROJECT_ID is not set.")
        print("Verification fails closed, so every request below will be rejected.\n")

    await attempt("no Authorization header", {})
    await attempt("wrong scheme", {"Authorization": "Basic aGk="})
    await attempt("forged bearer", {"Authorization": "Bearer not-a-real-token"})

    forwarded = os.environ.get("CONNECT_WEBHOOK_TOKEN")
    if forwarded:
        await attempt("forwarded trigger token", {"Authorization": f"Bearer {forwarded}"})
    else:
        print("\nSet CONNECT_WEBHOOK_TOKEN to a forwarded trigger token to see a success.")

    print(
        "\nTrust boundary: verification accepts any valid Vercel OIDC token for\n"
        "this project and environment. It is not pinned to a connector or a\n"
        "deployment, so treat the payload as authenticated, not authorized."
    )


async def attempt(label: str, headers: dict[str, str]) -> None:
    try:
        claims = await verify_connect_webhook(headers)
    except ConnectWebhookVerificationError as error:
        print(f"{label:<26} rejected: {error}")
        return
    print(f"{label:<26} verified")
    print(f"  project:     {claims.project_id}")
    print(f"  environment: {claims.environment}")
    print(f"  owner:       {claims.owner_id}")
    print(f"  subject:     {claims.subject}")
    print(f"  expires at:  {claims.expires_at}")


if __name__ == "__main__":
    asyncio.run(main())
