#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    printf 'usage: %s <package> <wheel>\n' "$0" >&2
    exit 2
fi

package=$1
wheel_path=$2
extra_wheel_args=

if [ ! -f "$wheel_path" ]; then
    printf 'wheel does not exist: %s\n' "$wheel_path" >&2
    exit 1
fi

wheel_dir=$(dirname "$wheel_path")
for dependency_wheel in "$wheel_dir"/vercel_*.whl; do
    if [ ! -f "$dependency_wheel" ] || [ "$dependency_wheel" = "$wheel_path" ]; then
        continue
    fi
    extra_wheel_args="$extra_wheel_args --with $dependency_wheel"
done

if [ "$package" = "vercel-internal-shared-vendored-deps" ]; then
    package_path=
else
    package_path=$(python - "$package" <<'PY'
import sys

from scripts import workspace

package = sys.argv[1]
try:
    print(workspace.packages()[package].path)
except KeyError:
    raise SystemExit(f"unknown package: {package}") from None
PY
)
fi

temp_dir=${RUNNER_TEMP:-${TMPDIR:-/tmp}}
test_root=$(mktemp -d "$temp_dir/vercel-py-installed-package-tests.XXXXXX")
if [ -n "$package_path" ] && [ -d "$package_path/tests" ]; then
    cp -R "$package_path/tests" "$test_root/tests"
fi
if [ -n "$package_path" ] && [ -d "$package_path/examples" ]; then
    cp -R "$package_path/examples" "$test_root/examples"
fi

if [ "$package" = "vercel-cache" ]; then
    python - "$test_root" <<'PY'
import sys
from pathlib import Path

test_root = Path(sys.argv[1])
for path in (test_root / "tests").rglob("*.py"):
    text = path.read_text(encoding="utf-8")
    rewritten = text.replace("import httpx\n", "from vercel.internal._vendor import httpx\n")
    if rewritten != text:
        path.write_text(rewritten, encoding="utf-8")
PY
fi

case "$package" in
    vercel-queue)
        set -- \
            --with pydantic \
            --with trio \
            --with fastapi \
            --with uvicorn \
            --with respx \
            --with pytest-asyncio
        pytest_args="$test_root/tests/unit/test_queue_typeutils.py $test_root/tests/unit/test_queue_transports.py $test_root/tests/unit/test_queue_config.py"
        pytest_filter='not test_deployment_resolution_and_opt_out_headers'
        ;;
    vercel-celery)
        set -- --with celery --with respx --with pytest-asyncio
        pytest_args=''
        pytest_filter=''
        ;;
    vercel-cache)
        set -- --with respx --with pytest-asyncio
        pytest_args="$test_root/tests"
        pytest_filter='not StrictErrors'
        ;;
    *)
        set -- --with respx --with pytest-asyncio
        if [ -d "$test_root/tests" ]; then
            pytest_args="$test_root/tests"
        else
            pytest_args=''
        fi
        pytest_filter=''
        ;;
esac

python - "$wheel_path" "$test_root" <<'PY'
import sys
import zipfile
from pathlib import Path

wheel = Path(sys.argv[1])
test_root = Path(sys.argv[2])
with zipfile.ZipFile(wheel) as archive:
    names = archive.namelist()
    assert not any("/_vendor/vercel/" in name for name in names), (
        "vercel-* packages must be installed side-by-side as -bundle dependencies, "
        "not copied into another package's vendor tree"
    )
    for member in archive.namelist():
        if member.startswith("vercel/") and not member.endswith("/"):
            archive.extract(member, test_root)
PY

if [ -n "$pytest_args" ]; then
    # shellcheck disable=SC2086
    uv run \
        --no-cache \
        --isolated \
        --directory "$test_root" \
        --with "$wheel_path" \
        --with pytest \
        $extra_wheel_args \
        "$@" \
        pytest \
        -v \
        --tb=short \
        -k "$pytest_filter" \
        $pytest_args
fi
