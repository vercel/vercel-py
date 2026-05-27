#!/usr/bin/env python3
"""Update sandbox settings and inspect running sessions."""

import asyncio
from uuid import uuid4

from dotenv import load_dotenv

from vercel.unstable import sandbox

load_dotenv()


async def main() -> None:
    name = f"vercel-py-controls-{uuid4().hex[:12]}"
    async with sandbox.create_sandbox(
        name=name,
        runtime="python3.13",
        ports=[3000],
        execution_time_limit=60_000,
    ) as sandbox_:
        updated = await sandbox_.update(tags={"example": "session-controls"})
        assert updated.tags == {"example": "session-controls"}

        current = await sandbox_.extend_execution_time_limit(60_000)
        assert current.id == sandbox_.current_session_id
        assert current.execution_time_limit is not None

        sessions = await sandbox_.list_sessions(page_size=10)
        assert any(session.id == sandbox_.current_session_id for session in sessions)
        print(f"{name}: {len(sessions)} session(s)")


if __name__ == "__main__":
    asyncio.run(main())
