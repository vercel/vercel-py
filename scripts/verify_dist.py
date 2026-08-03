"""Install every built wheel the way a user would, before it is published.

A wheel can be built, uploaded and completely uninstallable: the metadata is
whatever the build backend wrote, and neither `uv build` nor `uv publish` ever
resolves it. `vercel` 0.8.0 shipped `vercel-sandbox<0.3.0,>=0.3.0` that way and
the first person to find out ran `pip install`.

So run the install here instead. Each wheel goes into a throwaway environment
on its own, because that is the shape of the failure being looked for -- one
distribution whose own requirements cannot be met. Sibling wheels in the same
directory are offered to the resolver, so a package released alongside its
dependencies is testable before any of them exist on the index.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Built by `bundle_release.py`, which already installs and tests them through
# `.github/scripts/test_installed_wheel.sh`. They carry vendored copies of
# their dependencies and are meant to be installed side by side, so resolving
# one on its own does not describe how anyone uses them.
BUNDLE_SUFFIXES = ("_bundle",)
BUNDLE_NAMES = ("vercel_internal_shared_vendored_deps",)


def wheels(dist_dir: Path, *, package: str | None = None) -> list[Path]:
    found = sorted(p for p in dist_dir.glob("*.whl") if not p.name.endswith(".metadata"))
    found = [p for p in found if not _is_bundle(p)]
    if package is None:
        return found
    prefix = f"{package.replace('-', '_')}-"
    return [p for p in found if p.name.startswith(prefix)]


def _is_bundle(wheel: Path) -> bool:
    name = wheel.name.split("-", 1)[0]
    return name.startswith(BUNDLE_NAMES) or name.endswith(BUNDLE_SUFFIXES)


def install(wheel: Path, dist_dir: Path) -> subprocess.CompletedProcess[str]:
    """Resolve and install *wheel* into a fresh environment."""
    venv = Path(tempfile.mkdtemp(prefix="vercel-py-verify-dist."))
    try:
        subprocess.run(
            ["uv", "venv", "--no-config", str(venv)],
            check=True,
            capture_output=True,
            text=True,
        )
        return subprocess.run(
            [
                "uv",
                "pip",
                "install",
                # `--no-config` so the repo's own resolver settings -- an
                # `exclude-newer` cutoff in particular -- do not decide whether
                # a published package is installable.
                "--no-config",
                "--python",
                str(venv),
                # Siblings from this same build satisfy intra-workspace pins
                # before they exist on the index.
                "--find-links",
                str(dist_dir),
                str(wheel),
            ],
            capture_output=True,
            text=True,
        )
    finally:
        shutil.rmtree(venv, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist-dir", type=Path, default=Path("dist"))
    parser.add_argument(
        "--package",
        help="verify only this distribution; the rest of --dist-dir still "
        "backs the resolver, which is how a package is checked against "
        "siblings released in the same run",
    )
    args = parser.parse_args(argv)

    found = wheels(args.dist_dir, package=args.package)
    if not found:
        target = f"{args.package} in " if args.package else ""
        print(f"no wheels to verify for {target}{args.dist_dir}", file=sys.stderr)
        return 1

    failures = []
    for wheel in found:
        result = install(wheel, args.dist_dir)
        status = "ok" if result.returncode == 0 else "FAILED"
        print(f"{status:>6}  {wheel.name}", flush=True)
        if result.returncode != 0:
            failures.append((wheel, result))

    for wheel, result in failures:
        print(f"\n─── {wheel.name} ───\n{result.stderr.strip()}", file=sys.stderr)

    print(f"\n{len(found) - len(failures)}/{len(found)} wheels install")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
