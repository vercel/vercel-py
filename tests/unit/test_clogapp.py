from __future__ import annotations

import asyncio
from contextlib import nullcontext

import pytest
from rich.text import Text
from textual.widgets import Label

from scripts import clogedit, release


def test_clogedit_updates_selection_state() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    app.toggle("pkg")
    app.set_kind("pkg", "feature")
    app.add("pkg")

    assert selection.drafts() == [("pkg", "feature")]


def test_clogedit_action_next_opens_type_step(monkeypatch: pytest.MonkeyPatch) -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )
    monkeypatch.setattr(app, "save_package_selection", lambda: None)

    async def noop_render() -> None:
        return None

    monkeypatch.setattr(app, "render_step", noop_render)

    asyncio.run(app.action_next())

    assert app.step is clogedit.WizardStep.TYPES
    assert app.type_packages == ["pkg"]
    assert app.type_index == 0


def test_clogedit_action_next_finishes_after_type_step(monkeypatch: pytest.MonkeyPatch) -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
                kind="feature",
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )
    app.step = clogedit.WizardStep.TYPES
    app.type_packages = ["pkg"]
    monkeypatch.setattr(app, "save_current_type", lambda: None)
    seen = []
    monkeypatch.setattr(app, "finish", lambda: seen.append("finished"))

    asyncio.run(app.action_next())

    assert seen == ["finished"]


def test_clogedit_statusbar_text_is_minimal() -> None:
    assert clogedit.PACKAGE_STATUS == (
        "packages  arrows move  space toggles  enter next  ctrl-c/ctrl-d exit"
    )
    assert clogedit.TYPE_STATUS == (
        "change level  arrows move  space selects  enter edit  esc back  ctrl-c/ctrl-d exit"
    )
    assert "Button" not in clogedit.ChangelogApp.CSS
    assert "Header" not in clogedit.ChangelogApp.CSS


def test_clogedit_type_label_is_color_coded() -> None:
    app = clogedit.ChangelogApp(
        clogedit.ChangelogSelection(),
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    label = app.type_label("feature", selected=True)

    assert isinstance(label, Text)
    assert label.plain == "[*] feature - Features (minor)"
    assert any(span.style == "green" for span in label.spans)


def test_clogedit_finish_suspends_for_editor(monkeypatch: pytest.MonkeyPatch) -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
                kind="docs",
            )
        }
    )
    editor_calls: list[tuple[str, str]] = []

    def edit_news_fragment(package: str, kind: str) -> str:
        editor_calls.append((package, kind))
        return f"{package}:{kind}"

    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
        edit_news_fragment=edit_news_fragment,
    )
    monkeypatch.setattr(app, "suspend", lambda: nullcontext())
    seen: list[list[clogedit.NewsFragmentDraft]] = []
    monkeypatch.setattr(app, "exit", lambda result: seen.append(result))

    app.type_packages = ["pkg"]
    assert app.edit_current_package() is True
    app.finish()

    assert editor_calls == [("pkg", "docs")]
    assert seen == [[clogedit.NewsFragmentDraft("pkg", "docs", "pkg:docs")]]


async def test_clogedit_arrows_drive_widgets(monkeypatch: pytest.MonkeyPatch) -> None:
    selection = clogedit.ChangelogSelection(
        {
            "first": clogedit.PackageNewsState(
                name="first",
                changed=True,
                covered=False,
                selected=True,
            ),
            "second": clogedit.PackageNewsState(
                name="second",
                changed=True,
                covered=False,
                selected=False,
            ),
        }
    )
    editor_calls: list[tuple[str, str]] = []

    def edit_news_fragment(package: str, kind: str) -> str:
        editor_calls.append((package, kind))
        return f"{package}:{kind}"

    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
        edit_news_fragment=edit_news_fragment,
    )
    monkeypatch.setattr(app, "suspend", lambda: nullcontext())

    async with app.run_test() as pilot:
        await pilot.press("down", "space", "enter")
        assert selection.drafts() == [("first", "bugfix"), ("second", "bugfix")]

        await pilot.press("up", "space", "enter")
        assert selection.packages["first"].kind == "feature"
        assert editor_calls == [("first", "feature")]
        assert app.type_index == 1

        await pilot.press("down", "space", "enter")

    assert pilot.app.return_value == [
        clogedit.NewsFragmentDraft("first", "feature", "first:feature"),
        clogedit.NewsFragmentDraft("second", "docs", "second:docs"),
    ]


async def test_clogedit_edits_each_package_after_level_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selection = clogedit.ChangelogSelection(
        {
            "first": clogedit.PackageNewsState(
                name="first",
                changed=True,
                covered=False,
                selected=True,
            ),
            "second": clogedit.PackageNewsState(
                name="second",
                changed=True,
                covered=False,
                selected=True,
            ),
        }
    )
    events: list[str] = []

    def edit_news_fragment(package: str, kind: str) -> str:
        events.append(f"edit:{package}:{kind}:index={app.type_index}")
        return f"{package}:{kind}"

    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
        edit_news_fragment=edit_news_fragment,
    )
    monkeypatch.setattr(app, "suspend", lambda: nullcontext())

    async with app.run_test() as pilot:
        await pilot.press("enter")
        assert app.type_index == 0
        await pilot.press("enter")
        assert events == ["edit:first:bugfix:index=0"]
        assert app.type_index == 1
        await pilot.press("enter")

    assert events == ["edit:first:bugfix:index=0", "edit:second:bugfix:index=1"]


async def test_clogedit_type_title_bolds_package_name() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        await pilot.press("enter")
        label = app.query_one(Label)
        renderable = label.content
        assert isinstance(renderable, Text)
        assert renderable.plain == "Choose change level for pkg (1/1)"
        assert any(span.style == "bold" for span in renderable.spans)
        await pilot.press("ctrl+d")

    assert pilot.app.return_value == []


async def test_clogedit_kind_marker_follows_highlight() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        await pilot.press("enter")
        assert selection.packages["pkg"].kind == "bugfix"
        # Selecting one kind and then moving the highlight must not leave a
        # stale [*] marker: the kind tracks the highlight, which is what
        # enter saves.
        await pilot.press("space", "down")
        assert selection.packages["pkg"].kind == "docs"
        assert app.type_list is not None
        marked = app.type_list.get_option("docs").prompt
        assert isinstance(marked, Text)
        assert marked.plain.startswith("[*]")
        unmarked = app.type_list.get_option("bugfix").prompt
        assert isinstance(unmarked, Text)
        assert unmarked.plain.startswith("[ ]")
        await pilot.press("ctrl+d")

    assert pilot.app.return_value == []


async def test_clogedit_covered_packages_are_disabled() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            ),
            "done": clogedit.PackageNewsState(
                name="done",
                changed=True,
                covered=True,
                selected=False,
            ),
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        assert app.package_list is not None
        assert app.package_list.get_option_at_index(0).disabled is False
        assert app.package_list.get_option_at_index(1).disabled is True
        await pilot.press("ctrl+d")

    assert pilot.app.return_value == []


async def test_clogedit_escape_returns_to_previous_step() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        await pilot.press("enter")
        assert app.step is clogedit.WizardStep.TYPES
        await pilot.press("escape")
        assert app.step is clogedit.WizardStep.PACKAGES
        await pilot.press("ctrl+d")

    assert pilot.app.return_value == []


async def test_clogedit_single_escape_on_package_step_arms_exit() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        await pilot.press("escape")
        assert app.step is clogedit.WizardStep.PACKAGES
        assert app.escape_exit_timer is not None
        assert app.status == clogedit.ESC_EXIT_STATUS
        await pilot.press("ctrl+d")

    assert pilot.app.return_value == []


async def test_clogedit_double_escape_on_package_step_exits() -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        await pilot.press("escape", "escape")

    assert pilot.app.return_value == []


async def test_clogedit_escape_exit_timeout_resets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(clogedit, "ESC_EXIT_SECONDS", 0.01)
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
    )

    async with app.run_test() as pilot:
        await pilot.press("escape")
        await pilot.pause(0.03)
        assert app.escape_exit_timer is None
        assert app.status == clogedit.PACKAGE_STATUS
        monkeypatch.setattr(clogedit, "ESC_EXIT_SECONDS", 0.5)
        await pilot.press("escape")
        assert app.escape_exit_timer is not None
        await pilot.press("ctrl+d")

    assert pilot.app.return_value == []


async def test_clogedit_editor_failure_returns_to_type_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selection = clogedit.ChangelogSelection(
        {
            "pkg": clogedit.PackageNewsState(
                name="pkg",
                changed=True,
                covered=False,
                selected=True,
            )
        }
    )
    calls = 0

    def edit_news_fragment(package: str, kind: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SystemExit("editor exited with status 1")
        return f"{package}:{kind}"

    app = clogedit.ChangelogApp(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
        edit_news_fragment=edit_news_fragment,
    )
    monkeypatch.setattr(app, "suspend", lambda: nullcontext())

    async with app.run_test() as pilot:
        await pilot.press("enter", "enter")
        assert app.step is clogedit.WizardStep.TYPES
        assert app.type_index == 0
        assert app.status.startswith("editor exited with status 1")

        await pilot.press("enter")

    assert calls == 2
    assert pilot.app.return_value == [clogedit.NewsFragmentDraft("pkg", "bugfix", "pkg:bugfix")]
