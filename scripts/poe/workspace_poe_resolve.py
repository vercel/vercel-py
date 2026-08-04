from __future__ import annotations

import os
import subprocess
import sys
from collections import OrderedDict
from collections.abc import Iterable

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


ROOT_TASKS = {
    "fix": "fix-root",
    "lint": "lint-root",
    "test": "test-root",
    "typecheck": "typecheck-root",
    "test-examples": "test-examples-root",
}
OPT_IN_TASKS = frozenset({"test-examples"})


def local_tasks(package_path: str) -> dict[str, object]:
    pyproject = os.path.join(package_path, "pyproject.toml")
    try:
        with open(pyproject, "rb") as file:
            tasks = tomllib.load(file).get("tool", {}).get("poe", {}).get("tasks", {})
    except FileNotFoundError:
        return {}
    return tasks if isinstance(tasks, dict) else {}


def local_task_declared(package_path: str, task_name: str) -> bool:
    return task_name in local_tasks(package_path)


def task_for_scope(package: str, task_name: str) -> str:
    return ROOT_TASKS.get(task_name, task_name) if package == "root" else task_name


def task_is_supported(package: str, package_path: str, task_name: str) -> bool:
    if task_name not in OPT_IN_TASKS:
        return True
    return local_task_declared(package_path, task_for_scope(package, task_name))


def opt_in_packages(task_name: str, packages: Iterable[tuple[str, str]], root: str) -> set[str]:
    selected = {
        package
        for package, package_path in packages
        if task_is_supported(package, package_path, task_name)
    }
    if task_is_supported("root", root, task_name):
        selected.add("root")
    return selected


def require_task_support(package: str, package_path: str, task_name: str) -> None:
    if task_is_supported(package, package_path, task_name):
        return
    declared_task = task_for_scope(package, task_name)
    raise SystemExit(
        f"workspace task {task_name!r} is not declared by {package!r}; "
        f"add [tool.poe.tasks.{declared_task}] to {os.path.join(package_path, 'pyproject.toml')}"
    )


def first_task_cwd(package_path: str, task_name: str) -> str:
    if not task_name:
        return package_path
    tasks = local_tasks(package_path)

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
        if task_name in OPT_IN_TASKS:
            package_selected.update(opt_in_packages(task_name, packages, root))
        else:
            package_selected.update(package_paths)
            package_selected.add("root")

    for arg in argv:
        if arg in package_paths:
            require_task_support(arg, package_paths[arg], task_name)
            package_selected.add(arg)
            continue
        if arg == "root":
            require_task_support("root", root, task_name)
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
        require_task_support(owner, owner_path, task_name)
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
