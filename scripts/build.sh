#!/usr/bin/env bash
set -euo pipefail

packages=()
while IFS= read -r package; do
    packages+=("$package")
done < <(python3 scripts/package-names.py)

for package in "${packages[@]}"; do
    uv build --package "$package" --no-sources
done
