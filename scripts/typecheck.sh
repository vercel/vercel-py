#!/usr/bin/env bash
set -euo pipefail

uv run mypy src
uv run python -c "import vercel, vercel.cache, vercel.headers, vercel.oidc, vercel.sandbox"
