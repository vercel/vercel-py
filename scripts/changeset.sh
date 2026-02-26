#!/usr/bin/env bash
set -euo pipefail

CHANGESETS_CLI_VERSION="2.29.4"
CHANGELOG_PLUGIN_VERSION="1.1.0"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PACKAGE_JSON="$ROOT_DIR/package.json"

# Generate ephemeral package.json from pyproject.toml
VERSION=$(uv run "$SCRIPT_DIR/get-version.py")
cat > "$PACKAGE_JSON" <<EOF
{
  "name": "vercel",
  "version": "$VERSION",
  "private": true
}
EOF
trap 'rm -f "$PACKAGE_JSON"' EXIT

pnpm dlx \
  --package="@changesets/cli@${CHANGESETS_CLI_VERSION}" \
  --package="@svitejs/changesets-changelog-github-compact@${CHANGELOG_PLUGIN_VERSION}" \
  changeset "$@"
