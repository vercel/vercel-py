from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

PACKAGE_PATHS: dict[str, Path] = {
    "vercel-headers": ROOT / "src/vercel-headers",
    "vercel-internal-telemetry": ROOT / "src/vercel-internal-telemetry",
    "vercel-oidc": ROOT / "src/vercel-oidc",
    "vercel-queue": ROOT / "src/vercel-queue",
    "vercel-cache": ROOT / "src/vercel-cache",
    "vercel": ROOT / "src/vercel",
}

VERSION_FILES: dict[str, Path] = {
    "vercel-headers": PACKAGE_PATHS["vercel-headers"] / "vercel/headers/version.py",
    "vercel-internal-telemetry": PACKAGE_PATHS["vercel-internal-telemetry"]
    / "vercel/internal/telemetry/version.py",
    "vercel-oidc": PACKAGE_PATHS["vercel-oidc"] / "vercel/oidc/version.py",
    "vercel-queue": PACKAGE_PATHS["vercel-queue"] / "vercel/queue/version.py",
    "vercel-cache": PACKAGE_PATHS["vercel-cache"] / "vercel/cache/version.py",
    "vercel": PACKAGE_PATHS["vercel"] / "version.py",
}


def workspace_packages() -> dict[str, Path]:
    """Return workspace packages in dependency and publish order."""
    return dict(PACKAGE_PATHS)


def version_files() -> dict[str, Path]:
    return dict(VERSION_FILES)
