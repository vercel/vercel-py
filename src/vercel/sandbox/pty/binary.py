"""PTY server binary management - download and cache from Vercel CDN.

The PTY server is a Go binary that runs inside the sandbox to manage
pseudo-terminal sessions. This module handles downloading and caching
the binary from Vercel's CDN.
"""

from __future__ import annotations

from pathlib import Path

import httpx

BINARY_BASE_URL = "https://pty-tunnel.labs.vercel.dev"

CACHE_DIR = Path.home() / ".cache" / "vercel-sandbox"

SERVER_BIN_NAME = "vc-interactive-server"

# Sandboxes are currently always Linux x86_64
DEFAULT_SANDBOX_ARCH = "x86_64"


def get_binary_cache_path(arch: str | None = None) -> Path:
    """Get the cache path for the PTY server binary.

    Args:
        arch: Target sandbox architecture (x86_64 or aarch64).
            Defaults to x86_64 (current sandbox architecture).

    Returns:
        Path to the cached binary file.
    """
    if arch is None:
        arch = DEFAULT_SANDBOX_ARCH
    return CACHE_DIR / f"pty-server-linux-{arch}"


def download_binary(arch: str | None = None, *, force: bool = False) -> Path:
    """Download the PTY server binary if not already cached.

    Args:
        arch: Target sandbox architecture (x86_64 or aarch64).
            Defaults to x86_64 (current sandbox architecture).
        force: Force re-download even if cached.

    Returns:
        Path to the cached binary.

    Raises:
        httpx.HTTPError: If the download fails.
    """
    if arch is None:
        arch = DEFAULT_SANDBOX_ARCH

    cache_path = get_binary_cache_path(arch)

    if cache_path.exists() and not force:
        return cache_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Download from Vercel CDN
    url = f"{BINARY_BASE_URL}/linux-{arch}"
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        cache_path.write_bytes(response.content)

    return cache_path


def get_binary_bytes(arch: str | None = None) -> bytes:
    """Get the PTY server binary bytes, downloading if necessary.

    This is the main entry point for getting the binary content to upload
    to a sandbox.

    Args:
        arch: Target sandbox architecture (x86_64 or aarch64).
            Defaults to x86_64 (current sandbox architecture).

    Returns:
        Binary content as bytes.
    """
    path = download_binary(arch)
    return path.read_bytes()


async def download_binary_async(arch: str | None = None, *, force: bool = False) -> Path:
    """Download the PTY server binary asynchronously.

    Args:
        arch: Target sandbox architecture (x86_64 or aarch64).
            Defaults to x86_64 (current sandbox architecture).
        force: Force re-download even if cached.

    Returns:
        Path to the cached binary.

    Raises:
        httpx.HTTPError: If the download fails.
    """
    if arch is None:
        arch = DEFAULT_SANDBOX_ARCH

    cache_path = get_binary_cache_path(arch)

    if cache_path.exists() and not force:
        return cache_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Download from Vercel CDN
    url = f"{BINARY_BASE_URL}/linux-{arch}"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        cache_path.write_bytes(response.content)

    return cache_path


async def get_binary_bytes_async(arch: str | None = None) -> bytes:
    """Get the PTY server binary bytes asynchronously.

    Args:
        arch: Target sandbox architecture (x86_64 or aarch64).
            Defaults to x86_64 (current sandbox architecture).

    Returns:
        Binary content as bytes.
    """
    path = await download_binary_async(arch)
    return path.read_bytes()
