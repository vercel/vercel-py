#!/usr/bin/env bash
set -euo pipefail

package=${PACKAGE:?}
: "${DRY_RUN:?}" "${PUBLISH_SHARED:?}" "${VERCEL_INTERNAL_SHARED_VENDORED_DEPS_VERSION?}"
state_dir=${PUBLISH_DIR:?}
built_dir=${BUILD_DIR:?}
: > "$state_dir/tags.txt"
mkdir -p "$state_dir/release-bodies"

is_shared_vendored_deps=false
if [ "$package" = "vercel-internal-shared-vendored-deps" ]; then
  is_shared_vendored_deps=true
fi
if [ "$is_shared_vendored_deps" = "true" ]; then
  version=$(python scripts/bundle_release.py shared-version)
  publish_shared_vendored_deps="$PUBLISH_SHARED"
else
  version=$(python scripts/get-version.py "$package")
  publish_shared_vendored_deps=false
fi
if [ "$is_shared_vendored_deps" != "true" ] && [ "$version" = "0.0.0" ]; then
  echo "$package is at the unpublished sentinel version, skipping publish"
  exit 0
fi
tag="$package-v$version"
if [ "$is_shared_vendored_deps" != "true" ] || [ "$publish_shared_vendored_deps" = "true" ]; then
  echo "$tag" >> "$state_dir/tags.txt"
  if [ "$is_shared_vendored_deps" = "true" ]; then
    python scripts/bundle_release.py shared-github-release-body > "$state_dir/release-bodies/$tag.md"
  else
    python scripts/release.py github-release-body "$package" > "$state_dir/release-bodies/$tag.md"
  fi
fi

http_status=$(curl -s -o /dev/null -w "%{http_code}" "https://pypi.org/pypi/${package}/${version}/json")
if [ "$is_shared_vendored_deps" = "true" ] && [ "$publish_shared_vendored_deps" != "true" ]; then
  echo "$package dependency set already matches latest PyPI release, skipping publish"
elif [ "$http_status" = "200" ]; then
  echo "$package $version already exists on PyPI, skipping publish"
elif [ "$http_status" != "404" ]; then
  echo "Unexpected PyPI response for $package $version: $http_status"
  exit 1
fi

publish_matching_artifacts() {
  local target_pkg=$1
  local target_ver=$2
  local artifacts=()
  local artifact_list
  artifact_list=$(python3 - "$built_dir" "$target_pkg" "$target_ver" <<'PY'
import sys, re
from pathlib import Path

dist_dir = Path(sys.argv[1])
pkg = sys.argv[2]
version = sys.argv[3]
norm = pkg.replace("-", "_")
pattern = re.compile(rf"^{re.escape(norm)}-{re.escape(version)}(?:-.+\.whl|\.tar\.gz)$")
matching = sorted(p.resolve() for p in dist_dir.iterdir() if pattern.match(p.name))
if not matching:
    sys.exit(f"No built artifacts found for {pkg} {version} in {dist_dir}")
for path in matching:
    print(path)
PY
  )
  while IFS= read -r artifact; do
    [ -n "$artifact" ] && artifacts+=("$artifact")
  done <<< "$artifact_list"
  if [ "${#artifacts[@]}" -eq 0 ]; then
    echo "No built artifacts resolved for $target_pkg $target_ver" >&2
    exit 1
  fi
  if [ "$DRY_RUN" = "true" ]; then
    printf 'Dry run: would publish %s\n' "${artifacts[@]}"
  else
    uv publish "${artifacts[@]}"
  fi
}

bundle_eligible=$(python - "$package" <<'PY'
import sys
from scripts import bundle_release, workspace

name = sys.argv[1]
eligible = name == bundle_release.SHARED_VENDORED_PACKAGE or bundle_release.is_vendored_eligible(
    workspace.packages()[name]
)
print("true" if eligible else "false")
PY
)
if [ "$bundle_eligible" = "true" ]; then
  cat "$built_dir/bundle-plan.txt"

  bundle_package=$(sed -n 's/^bundle-package: //p' "$built_dir/bundle-plan.txt")
  if [ -z "$bundle_package" ]; then
    echo "missing bundle-package in plan"
    exit 1
  fi
  bundle_tag="${bundle_package}-v${version}"
  if [ "$bundle_tag" != "$tag" ]; then
    echo "$bundle_tag" >> "$state_dir/tags.txt"
    cp "$state_dir/release-bodies/$tag.md" "$state_dir/release-bodies/$bundle_tag.md"
  fi

  if [ "$is_shared_vendored_deps" = "true" ] && [ "$publish_shared_vendored_deps" != "true" ]; then
    bundle_http_status=200
  else
    bundle_http_status=$(curl -s -o /dev/null -w "%{http_code}" "https://pypi.org/pypi/${bundle_package}/${version}/json")
  fi
  if [ "$is_shared_vendored_deps" = "true" ]; then
    :
  elif [ "$bundle_http_status" = "200" ]; then
    echo "$bundle_package $version already exists on PyPI, skipping publish"
  elif [ "$bundle_http_status" != "404" ]; then
    echo "Unexpected PyPI response for $bundle_package $version: $bundle_http_status"
    exit 1
  else
    publish_matching_artifacts "$bundle_package" "$version"
  fi
else
  echo "$package is not vendored-eligible"
fi

if [ "$is_shared_vendored_deps" = "true" ]; then
  if [ "$publish_shared_vendored_deps" != "true" ]; then
    :
  elif [ "$http_status" = "200" ]; then
    :
  else
    publish_matching_artifacts "$package" "$version"
  fi
elif [ "$http_status" = "200" ]; then
  :
else
  publish_matching_artifacts "$package" "$version"
fi
