#!/usr/bin/env python3
"""The same operations from synchronous code.

`vercel.connect.sync` mirrors `vercel.connect` exactly: identical names,
arguments, and docstrings, with no `await`. Useful in scripts, Celery tasks, and
anywhere a WSGI stack rules out an event loop.
"""

from _shared import describe_error, load_environment, mask, require_connector

from vercel.connect.sync import (
    ConnectAppTokenSubject,
    ConnectError,
    ConnectOptions,
    get_connector_metadata,
    get_token,
    get_token_response,
)

load_environment()


def main() -> None:
    connector = require_connector()
    subject = ConnectAppTokenSubject()

    try:
        response = get_token_response(connector, subject=subject)
        print(f"token:      {mask(response.token)}")
        print(f"token id:   {response.token_id}")
        print(f"expires at: {response.expires_at.isoformat()}")

        # Cached, so this does not hit the network.
        cached = get_token(connector, subject=subject)
        print(f"cached:     {mask(cached)}")

        refreshed = get_token(
            connector, subject=subject, options=ConnectOptions(force_refresh=True)
        )
        print(f"refreshed:  {mask(refreshed)}")

        metadata = get_connector_metadata(connector)
        print(f"connector:  {metadata.uid} ({metadata.type})")
    except ConnectError as error:
        print(describe_error(error))
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
