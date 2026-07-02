from __future__ import annotations

import os
import subprocess
import sys
from collections import OrderedDict

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    import tomli as tomllib


def uv_no_color(*args: str) -> list[str]:
    env = os.environ.copy()
    for name in ("FORCE_COLOR", "CLICOLOR_FORCE", "PY_COLORS"):
        env.pop(name, None)
    env["NO_COLOR"] = "1"
    output = subprocess.check_output(("uv", *args), env=env, text=True)
    return output.splitlines()


def workspace_packages() -> list[tuple[str, str]]:
    return list(
        zip(
            uv_no_color("workspace", "list"),
            uv_no_color("workspace", "list", "--paths"),
            strict=True,
        )
    )


def first_task_cwd(package_path: str, task_name: str) -> str:
    if not task_name:
        return package_path
    pyproject = os.path.join(package_path, "pyproject.toml")
    try:
        with open(pyproject, "rb") as file:
            tasks = tomllib.load(file).get("tool", {}).get("poe", {}).get("tasks", {})
    except FileNotFoundError:
        return package_path

    def first_cwd(name: str) -> str | None:
        task = tasks.get(name, {})
        if not isinstance(task, dict):
            return None
        if "cmd" in task:
            return task.get("cwd")
        for child in task.get("sequence", []):
            cwd = first_cwd(child)
            if cwd is not None:
                return cwd
        return task.get("cwd")

    return os.path.normpath(os.path.join(package_path, first_cwd(task_name) or "."))


def main(argv: list[str]) -> int:
    root = os.getcwd()
    task_name = os.environ.get("WORKSPACE_POE_SCOPE_TASK", "")
    packages = workspace_packages()
    package_paths = OrderedDict(packages)
    package_selected: set[str] = set()
    scoped_paths: dict[str, list[str]] = {}

    if not argv:
        package_selected.update(package_paths)
        package_selected.add("root")

    for arg in argv:
        if arg in package_paths:
            package_selected.add(arg)
            continue
        if arg == "root":
            package_selected.add("root")
            continue

        abs_arg = os.path.abspath(arg)
        owner = "root"
        owner_path = root
        for package, raw_package_path in package_paths.items():
            package_path = os.path.abspath(raw_package_path)
            if abs_arg == package_path or abs_arg.startswith(package_path + os.sep):
                if owner == "root" or len(package_path) > len(owner_path):
                    owner = package
                    owner_path = package_path

        task_cwd = root if owner == "root" else first_task_cwd(owner_path, task_name)
        scoped_paths.setdefault(owner, []).append(os.path.relpath(abs_arg, task_cwd))

    for package, package_path in packages:
        if package in package_selected:
            print(f"{package}\t{package_path}")
        elif package in scoped_paths:
            print("\t".join((package, package_path, *scoped_paths[package])))

    if "root" in package_selected:
        print(f"root\t{root}")
    elif "root" in scoped_paths:
        print("\t".join(("root", root, *scoped_paths["root"])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
