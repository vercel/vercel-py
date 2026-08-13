"""``python -m vercel.workflow`` -- what an outside tool runs to get a manifest.

The observability UI reads a manifest off disk, and the app itself only writes
one once it handles a workflow message, which is too late for a developer who
starts their server and opens the UI. Nothing in Python plays the part the
TypeScript builder plays there, so the thing that fills the gap is a command:
the `workflow` CLI (or a build step) runs it in the project, and the app is
imported for exactly as long as it takes to read its registries.

The app is named the way the Vercel Python builder already names it --
``module:attr``, from ``[[tool.vercel.workflows]] entrypoint`` in
``pyproject.toml`` -- so with a project that deploys, the command needs no
arguments and its caller needs no knowledge of the app's layout.
"""

from __future__ import annotations

import argparse
import importlib
import json
import pathlib
import sys
from typing import Any

from . import core, runtime, world as w

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover -- exercised on 3.10 only
    import tomli as tomllib

PYPROJECT = "pyproject.toml"
PYTHON = sys.executable


class CommandError(Exception):
    pass


def _declared_entrypoints(project: pathlib.Path) -> list[str]:
    path = project / PYPROJECT
    try:
        with path.open("rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        raise CommandError(
            f"no {PYPROJECT} in {project}, so there is nothing to read the app from. "
            f"Name it instead: {PYTHON} -m vercel.workflow manifest module:app"
        ) from None
    except tomllib.TOMLDecodeError as error:
        raise CommandError(f"could not read {path}: {error}") from None

    vercel = config.get("tool", {}).get("vercel", {})
    specs = [
        entry["entrypoint"]
        for entry in vercel.get("workflows", [])
        if isinstance(entry, dict) and entry.get("entrypoint")
    ]
    if not specs:
        raise CommandError(
            f"{path} declares no entrypoint under [[tool.vercel.workflows]], so there "
            f"is nothing to import. Name the app instead: "
            f"{PYTHON} -m vercel.workflow manifest module:app"
        )
    return specs


def _registries(spec: str) -> list[core.Workflows]:
    module_name, _, attr = spec.partition(":")
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        # Anything the app raises on import, which is the caller's to fix and
        # not ours to hide -- but as a message, since a traceback through
        # importlib says less than the name of the app that failed.
        raise CommandError(f"could not import {module_name!r} from {spec!r}: {error}") from None

    if attr:
        try:
            found = getattr(module, attr)
        except AttributeError:
            raise CommandError(f"{module_name!r} has no attribute {attr!r}") from None
        if not isinstance(found, core.Workflows):
            raise CommandError(f"{spec!r} is a {type(found).__name__}, not a Workflows registry")
        return [found]

    registries = [value for value in vars(module).values() if isinstance(value, core.Workflows)]
    if not registries:
        raise CommandError(f"{module_name!r} defines no Workflows registry")
    return registries


def _manifest(specs: list[str]) -> dict[str, Any]:
    registries: list[core.Workflows] = []
    for spec in specs:
        for registry in _registries(spec):
            # Two specs in one module, or a module that exports the same
            # registry twice, should not count it twice.
            if not any(registry is seen for seen in registries):
                registries.append(registry)
    return runtime.build_manifest(*registries)


def _manifest_command(args: argparse.Namespace) -> int:
    specs = args.app or _declared_entrypoints(pathlib.Path(args.project))
    manifest = _manifest(specs)

    if args.stdout:
        print(json.dumps(manifest, indent=2))
        return 0

    world = w.get_world()
    try:
        path = world.write_manifest(manifest)
    except OSError as error:
        raise CommandError(f"could not write the manifest: {error}") from None
    if path is None:
        raise CommandError(
            f"{type(world).__name__} has nowhere to keep a manifest. This writes the "
            f"file a local observability UI reads; set WORKFLOW_TARGET_WORLD=local, "
            f"or use --stdout and put it where you need it."
        )
    print(path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=f"{PYTHON} -m vercel.workflow",
        description="Utilities for a Vercel Workflow app.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    manifest = subcommands.add_parser(
        "manifest",
        help="write the app's workflow manifest",
        description=(
            "Import the app and write its manifest where a local observability UI "
            "reads one -- manifest.json in the workflow data directory."
        ),
    )
    manifest.add_argument(
        "app",
        nargs="*",
        help=(
            "the app, as module or module:attr. Defaults to the entrypoints "
            f"declared in {PYPROJECT} under [[tool.vercel.workflows]]."
        ),
    )
    manifest.add_argument(
        "--project",
        default=".",
        metavar="DIR",
        help=f"where to look for {PYPROJECT} (default: the current directory)",
    )
    manifest.add_argument(
        "--stdout",
        action="store_true",
        help="print the manifest instead of writing it",
    )
    manifest.set_defaults(func=_manifest_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except CommandError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
