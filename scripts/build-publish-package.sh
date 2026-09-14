#!/usr/bin/env bash
set -euo pipefail

package=${PACKAGE:?}
build_dir=${BUILD_DIR:?}
mkdir -p "$build_dir"

if [ "$package" != "vercel-internal-shared-vendored-deps" ]; then
  uv build --package "$package" --no-sources --out-dir "$build_dir"
  python scripts/verify_dist.py --dist-dir "$build_dir" --package "$package"
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
  python scripts/bundle_release.py plan --package "$package" > "$build_dir/bundle-plan.txt"
  python scripts/bundle_release.py build --package "$package" --out-dir "$build_dir"
  python scripts/bundle_release.py test-wheel --package "$package" --dist-dir "$build_dir"
fi
