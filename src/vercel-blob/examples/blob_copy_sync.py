"""Publish a reviewed draft while keeping its source document."""

from __future__ import annotations

import os
from uuid import uuid4

from vercel.blob.sync import Access, copy_blob, delete_blob, get_blob, put_blob


def main() -> None:
    access: Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    prefix = f"vercel-py-examples/{uuid4().hex}/"
    draft_path = prefix + "drafts/getting-started.md"
    published_path = prefix + "published/getting-started.md"
    try:
        draft = put_blob(
            draft_path,
            b"# Getting started\nCreate a project and upload your first document.\n",
            access=access,
            content_type="text/markdown",
        )

        # In an application, retain the ETag of the revision that was reviewed.
        # The server copies only if the draft still matches that revision.
        reviewed_etag = draft.etag
        published = copy_blob(
            draft.url,
            published_path,
            access=access,
            if_match=reviewed_etag,
            content_type="text/markdown",
            cache_control_max_age=3600,
        )
        # Copy metadata is explicit; it is not inherited from the draft.
        # A stale reviewed_etag would raise BlobPreconditionFailedError.
        print(f"Published document: {published.url}")
        preview = get_blob(published.url, access=access)
        print(preview.body.decode("utf-8"))
    finally:
        delete_blob([draft_path, published_path])


if __name__ == "__main__":
    main()
