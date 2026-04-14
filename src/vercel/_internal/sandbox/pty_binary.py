"""Internal PTY server binary management helpers."""

from __future__ import annotations

from pathlib import Path

import httpx

BINARY_BASE_URL = "https://pty-tunnel.labs.vercel.dev"

CACHE_DIR = Path.home() / ".cache" / "vercel-sandbox"

SERVER_BIN_NAME = "vc-interactive-server"

# Sandboxes are currently always Linux x86_64
DEFAULT_SANDBOX_ARCH = "x86_64"


def get_binary_cache_path(arch: str | None = None) -> Path:
    if arch is None:
        arch = DEFAULT_SANDBOX_ARCH
    return CACHE_DIR / f"pty-server-linux-{arch}"


def download_binary(arch: str | None = None, *, force: bool = False) -> Path:
    if arch is None:
        arch = DEFAULT_SANDBOX_ARCH

    cache_path = get_binary_cache_path(arch)

    if cache_path.exists() and not force:
        return cache_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    url = f"{BINARY_BASE_URL}/linux-{arch}"
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        cache_path.write_bytes(response.content)

    return cache_path


def get_binary_bytes(arch: str | None = None) -> bytes:
    path = download_binary(arch)
    return path.read_bytes()


async def download_binary_async(arch: str | None = None, *, force: bool = False) -> Path:
    if arch is None:
        arch = DEFAULT_SANDBOX_ARCH

    cache_path = get_binary_cache_path(arch)

    if cache_path.exists() and not force:
        return cache_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    url = f"{BINARY_BASE_URL}/linux-{arch}"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        cache_path.write_bytes(response.content)

    return cache_path


async def get_binary_bytes_async(arch: str | None = None) -> bytes:
    path = await download_binary_async(arch)
    return path.read_bytes()
