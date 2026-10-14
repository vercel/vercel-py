"""Browse a documentation library, list storage usage, and follow virtual folders."""

from __future__ import annotations

import os
from uuid import uuid4

import anyio

from vercel.blob import Access, BlobPrefix, delete_blob, put_blob, query_blob_entries, query_blobs


async def print_directory_tree(prefix: str, *, indent: str = "") -> None:
    """Browse one level, then follow each returned prefix into its subfolder."""
    async for entry in query_blob_entries(prefix=prefix, page_size=2):
        if isinstance(entry, BlobPrefix):
            print(f"{indent}{entry.prefix.removeprefix(prefix)}")
            await print_directory_tree(entry.prefix, indent=indent + "  ")
        else:
            name = entry.pathname.removeprefix(prefix)
            print(f"{indent}{name} ({entry.size} bytes)")


async def main() -> None:
    access: Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    prefix = f"vercel-py-examples/{uuid4().hex}/docs/"
    # Seed a small library so this example can run independently of existing data.
    documents = {
        "index.md": b"# Documentation\nStart with the guides, then consult the API reference.\n",
        "guides/getting-started.md": (
            b"# Getting started\nCreate a project and upload your first document.\n"
        ),
        "guides/deploying.md": (
            b"# Deploying\nConfigure credentials before deploying your application.\n"
        ),
        "reference/api.md": b"# API reference\nObject operations and their options.\n",
    }
    try:
        for name, body in documents.items():
            await put_blob(prefix + name, body, access=access, content_type="text/markdown")

        # Use query_blobs for an inventory across every subfolder. A small page
        # size demonstrates pagination; the iterator follows cursors for us.
        print("Storage inventory:")
        count = 0
        total_bytes = 0
        async for item in query_blobs(prefix=prefix, page_size=2):
            print(f"  {item.pathname.removeprefix(prefix)}: {item.size} bytes")
            count += 1
            total_bytes += item.size
        print(f"{count} documents, {total_bytes} bytes in total")

        # Use query_blob_entries for a folder browser: it returns immediate documents
        # and BlobPrefix entries, which we query in turn to display a tree.
        print("\nDirectory tree:")
        await print_directory_tree(prefix)
    finally:
        await delete_blob([prefix + name for name in documents])


if __name__ == "__main__":
    anyio.run(main)
