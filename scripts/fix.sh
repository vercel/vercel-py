#!/usr/bin/env bash
set -euo pipefail

source_dirs=()
while IFS= read -r source_dir; do
    source_dirs+=("$source_dir")
done < <(python3 scripts/source-dirs.py)

uv run --all-packages ruff check --fix "${source_dirs[@]}"
uv run --all-packages ruff format "${source_dirs[@]}"
