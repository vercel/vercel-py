#!/usr/bin/env bash
set -euo pipefail

dist_dir="dist"
mkdir -p "$dist_dir"

packages=()
package_list=$(uv run python scripts/workspace.py list --names --topological)
while IFS= read -r package; do
    packages+=("$package")
done <<< "$package_list"

for package in "${packages[@]}"; do
    uv build --package "$package" --no-sources --out-dir "$dist_dir"
done

uv run python scripts/bundle_release.py build \
    --package vercel-internal-shared-vendored-deps \
    --out-dir "$dist_dir"

for package in "${packages[@]}"; do
    if uv run python scripts/bundle_release.py plan --package "$package" >/dev/null 2>&1; then
        uv run python scripts/bundle_release.py build --package "$package" --out-dir "$dist_dir"
    fi
done
