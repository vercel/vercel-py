"""Live Drive lifecycle against the Vercel API."""

import os
from uuid import uuid4

import pytest

from vercel import drive
from vercel.api import session

requires_drive_credentials = pytest.mark.skipif(
    not (
        os.getenv("VERCEL_OIDC_TOKEN")
        or (
            os.getenv("VERCEL_TOKEN")
            and os.getenv("VERCEL_TEAM_ID")
            and os.getenv("VERCEL_PROJECT_ID")
        )
    ),
    reason="Requires VERCEL_OIDC_TOKEN, or VERCEL_TOKEN plus VERCEL_TEAM_ID and VERCEL_PROJECT_ID",
)


@requires_drive_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_drive_lifecycle() -> None:
    """A Drive can be created, found by name and ID, listed, and deleted."""
    name = f"vercel-py-drive-{uuid4().hex[:10]}"

    async with session():
        created, was_created = await drive.get_or_create_drive(
            name=name, region="iad1", max_size_bytes=1024**3
        )
        try:
            assert was_created is True
            assert (created.name, created.region, created.max_size_bytes) == (name, "iad1", 1024**3)

            existing, was_created = await drive.get_or_create_drive(name=name)
            assert was_created is False
            assert existing.id == created.id

            assert (await drive.get_drive(name_or_id=name)).id == created.id
            assert (await drive.get_drive(name_or_id=created.id)).name == name
            listed = [
                item.id
                async for item in drive.query_drives(query=drive.DriveQueryByName(name_prefix=name))
            ]
            assert listed == [created.id]

            # A Drive that was never mounted has no committed data to copy.
            with pytest.raises(drive.DriveApiError) as exc_info:
                await created.fork(name=f"{name}-fork")
            assert exc_info.value.status_code == 409
        finally:
            await drive.delete_drive(name=name)

        with pytest.raises(drive.DriveApiError) as exc_info:
            await drive.get_drive(name_or_id=name)
        assert exc_info.value.status_code == 404
