#!/usr/bin/env python3
"""Inspect a connector, and dump whatever the API returned beyond the typed subset.

Read metadata instead of hardcoding assumptions about scopes or subject types.
Only the documented fields are typed; everything else is preserved verbatim in
`extra`, so printing `extra` shows exactly which server fields are not yet
modelled.
"""

import asyncio
import json

from _shared import describe_error, load_environment, require_connector

from vercel.connect import ConnectError, get_connector_metadata

load_environment()


async def main() -> None:
    connector = require_connector()

    try:
        metadata = await get_connector_metadata(connector)
    except ConnectError as error:
        print(describe_error(error))
        raise SystemExit(1) from error

    print("typed fields")
    print(f"  id:         {metadata.id}")
    print(f"  uid:        {metadata.uid}")
    print(f"  type:       {metadata.type}")
    print(f"  name:       {metadata.name}")
    print(f"  service:    {metadata.service}")
    print(f"  client url: {metadata.client_url}")
    print(f"  created at: {metadata.created_at}")
    print(f"  updated at: {metadata.updated_at}")

    # Provider secrets come back redacted as {"encrypted": true}.
    print("\nvendor configuration (secrets redacted server-side)")
    print(_indent(metadata.vendor))

    print("\nfields not yet modelled, preserved in `extra`")
    if metadata.extra:
        print(_indent(metadata.extra))
        print(
            "\nThese are the connector capabilities worth promoting to typed\n"
            "attributes. Capture this payload as a test fixture before doing so."
        )
    else:
        print("  (none: the response contained only documented fields)")


def _indent(payload: object) -> str:
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str)
    return "\n".join(f"  {line}" for line in rendered.splitlines())


if __name__ == "__main__":
    asyncio.run(main())
