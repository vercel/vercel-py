#!/usr/bin/env python3
"""Handle a user who has not consented yet.

`UserAuthorizationRequiredError` is not a bug, it is a state with a remedy: catch
it, start an authorization, and hand the user a URL. A web app would redirect;
this script prints the URL and, for a device-code flow, polls until consent
lands.
"""

import asyncio
import os

from _shared import describe_error, load_environment, mask, require_connector

from vercel.connect import (
    ConnectOptions,
    ConnectUserTokenSubject,
    NoValidTokenError,
    UserAuthorizationRequiredError,
    get_token,
    start_authorization,
)

load_environment()

POLL_INTERVAL_SECONDS = 3
POLL_ATTEMPTS = 20


async def main() -> None:
    connector = require_connector()
    user_id = os.environ.get("CONNECT_USER_ID", "demo-user")
    subject = ConnectUserTokenSubject(id=user_id)

    try:
        token = await get_token(connector, subject=subject)
    except UserAuthorizationRequiredError as error:
        print(describe_error(error))
        print("\nThis user has not consented yet, so start an authorization.\n")
        token = await authorize_then_poll(connector, subject)
    else:
        print(f"already authorized: {mask(token)}")
        return

    print(f"\nauthorized: {mask(token)}")


async def authorize_then_poll(connector: str, subject: ConnectUserTokenSubject) -> str:
    # device_code suits a CLI: nothing is delivered back to this process, so the
    # only way to learn that consent happened is to ask for the token again.
    authorization = await start_authorization(connector, subject=subject, device_code=True)

    print(f"open this URL to consent: {authorization.url}")
    if authorization.device_code:
        print(f"device code:              {authorization.device_code}")
    if authorization.expires_at:
        print(f"request expires at:       {authorization.expires_at.isoformat()}")
    print(f"request id:               {authorization.request}")
    print("\n(`request` and `verifier` are returned for parity with the TypeScript")
    print(" SDK; no Connect endpoint consumes them today, so treat them as opaque.)")

    print(f"\npolling every {POLL_INTERVAL_SECONDS}s for up to {POLL_ATTEMPTS} attempts...")
    for attempt in range(1, POLL_ATTEMPTS + 1):
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        try:
            # force_refresh bypasses the local cache, which otherwise keeps
            # serving the failure-free view of a grant that did not exist yet.
            return await get_token(
                connector,
                subject=subject,
                options=ConnectOptions(force_refresh=True),
            )
        except (UserAuthorizationRequiredError, NoValidTokenError):
            print(f"  attempt {attempt}: not yet consented")

    raise SystemExit("consent did not complete before the polling window closed")


if __name__ == "__main__":
    asyncio.run(main())
