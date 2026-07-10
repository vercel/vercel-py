#!/usr/bin/env bash
set -euo pipefail

packages=()
while IFS= read -r package; do
    packages+=("$package")
done < <(python3 scripts/workspace.py list --names --topological)

for package in "${packages[@]}"; do
    uv build --package "$package" --no-sources
done
