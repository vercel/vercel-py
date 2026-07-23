import os
import sys

from vercel.sandbox import __main__


def test_missing_npx_reports_install_help(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(__main__.shutil, "which", lambda executable: None)

    assert __main__.main() == 1
    stderr = capsys.readouterr().err
    assert "'npx' is not available" in stderr
    assert "from vercel import sandbox, session" in stderr


def test_cli_delegates_all_arguments(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    delegated: list[tuple[str, list[str]]] = []

    def execvp(executable: str, arguments: list[str]) -> None:
        delegated.append((executable, arguments))
        raise RuntimeError("exec intercepted")

    monkeypatch.setattr(__main__.shutil, "which", lambda executable: "/usr/bin/npx")
    monkeypatch.setattr(os, "execvp", execvp)
    monkeypatch.setattr(sys, "argv", ["vercel-sandbox", "run", "--region", "iad1"])

    try:
        __main__.main()
    except RuntimeError as exc:
        assert str(exc) == "exec intercepted"
    else:  # pragma: no cover - the monkeypatched exec always raises
        raise AssertionError("execvp was not called")

    assert delegated == [("npx", ["npx", "sandbox", "run", "--region", "iad1"])]
