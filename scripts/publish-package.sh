#!/usr/bin/env bash
set -euo pipefail

package=${PACKAGE:?}
: "${DRY_RUN:?}" "${PUBLISH_SHARED:?}" "${VERCEL_INTERNAL_SHARED_VENDORED_DEPS_VERSION?}"
state_dir=${PUBLISH_DIR:?}
: > "$state_dir/tags.txt"
mkdir -p "$state_dir/release-bodies" "$state_dir/verify" "$state_dir/bundle" \
  "$state_dir/artifacts"/standard "$state_dir/artifacts"/bundle
shopt -s nullglob
for wheel in "$state_dir/dependencies"/standard/*.whl; do
  cp "$wheel" "$state_dir/verify"/
done
for wheel in "$state_dir/dependencies"/bundle/*.whl; do
  cp "$wheel" "$state_dir/bundle"/
done

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

if [ "$http_status" = "404" ] && [ "$is_shared_vendored_deps" != "true" ]; then
  uv build --package "$package" --no-sources --out-dir "$state_dir/standard"
  mkdir -p "$state_dir/verify"
  cp "$state_dir/standard"/*.whl "$state_dir/verify"/
  python scripts/verify_dist.py --dist-dir "$state_dir/verify" --package "$package"
fi
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
  python scripts/bundle_release.py plan --package "$package" > "$state_dir/bundle-plan.txt"
  cat "$state_dir/bundle-plan.txt"
  python scripts/bundle_release.py build --package "$package" --out-dir "$state_dir/bundle"
  python scripts/bundle_release.py test-wheel --package "$package" --dist-dir "$state_dir/bundle"

  bundle_package=$(sed -n 's/^bundle-package: //p' "$state_dir/bundle-plan.txt")
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
  elif [ "$DRY_RUN" = "true" ]; then
    echo "Dry run: would publish $bundle_package $version"
    bundle_http_status=404
  else
    bundle_http_status=$(curl -s -o /dev/null -w "%{http_code}" "https://pypi.org/pypi/${bundle_package}/${version}/json")
  fi
  if [ "$is_shared_vendored_deps" = "true" ]; then
    :
  elif [ "$bundle_http_status" = "200" ]; then
    echo "$bundle_package $version already exists on PyPI, skipping publish"
  elif [ "$DRY_RUN" = "true" ]; then
    :
  elif [ "$bundle_http_status" != "404" ]; then
    echo "Unexpected PyPI response for $bundle_package $version: $bundle_http_status"
    exit 1
  else
    bundle_prefix=${bundle_package//-/_}
    find "$state_dir/bundle" -maxdepth 1 -type f -name "${bundle_prefix}-${version}*" -print0 | xargs -0 uv publish
  fi
else
  echo "$package is not vendored-eligible"
fi

if [ "$is_shared_vendored_deps" = "true" ]; then
  if [ "$publish_shared_vendored_deps" != "true" ]; then
    :
  elif [ "$http_status" = "200" ]; then
    :
  elif [ "$DRY_RUN" = "true" ]; then
    echo "Dry run: would publish $package $version"
  else
    bundle_prefix=${package//-/_}
    find "$state_dir/bundle" -maxdepth 1 -type f -name "${bundle_prefix}-${version}*" -print0 | xargs -0 uv publish
  fi
elif [ "$http_status" = "200" ]; then
  :
elif [ "$DRY_RUN" = "true" ]; then
  echo "Dry run: would publish $package $version"
else
  uv publish "$state_dir/standard"/*
fi

for wheel in "$state_dir/standard"/*.whl; do
  cp "$wheel" "$state_dir/artifacts"/standard/
done
bundle_prefix=${package//-/_}
if [ "$is_shared_vendored_deps" != "true" ]; then
  bundle_prefix="${bundle_prefix}_bundle"
fi
for wheel in "$state_dir/bundle"/"${bundle_prefix}-${version}"-*.whl; do
  cp "$wheel" "$state_dir/artifacts"/bundle/
done
