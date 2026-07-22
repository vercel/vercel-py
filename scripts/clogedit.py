from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import (
    Label,
    OptionList,
    SelectionList,
    Static,
)
from textual.widgets.option_list import Option
from textual.widgets.selection_list import Selection

try:
    from scripts import release, workspace
except ImportError:  # pragma: no cover - script execution path
    import release  # type: ignore[no-redef]
    import workspace  # type: ignore[no-redef]

NewsFragmentDraft = release.NewsFragmentDraft


class WizardStep(Enum):
    PACKAGES = "packages"
    TYPES = "types"


PACKAGE_STATUS = "packages  arrows move  space toggles  enter next  ctrl-c/ctrl-d exit"
TYPE_STATUS = "change level  arrows move  space selects  enter edit  esc back  ctrl-c/ctrl-d exit"
ESC_EXIT_STATUS = "packages  esc again exits  arrows move  space toggles  enter next"
ESC_EXIT_SECONDS = 0.5
TYPE_STYLES = {
    "breaking": "red",
    "feature": "green",
    "bugfix": "cyan",
    "docs": "blue",
    "internal": "dim",
}


@dataclass(frozen=True)
class PackageNewsState:
    name: str
    changed: bool
    covered: bool
    selected: bool
    kind: str = "bugfix"


@dataclass
class ChangelogSelection:
    packages: dict[str, PackageNewsState] = field(default_factory=dict)

    def drafts(self) -> list[tuple[str, str]]:
        return [
            (state.name, state.kind)
            for state in self.packages.values()
            if state.selected and not state.covered
        ]


class ChangeTypeList(OptionList):
    BINDINGS = [Binding("space", "select", "Select", show=False)]


class ChangelogApp(App[list[NewsFragmentDraft]]):
    CSS = """
    Screen { padding: 1 2 0 2; }
    #body { height: 1fr; }
    #title { height: auto; margin-bottom: 1; }
    #status { dock: bottom; height: 1; color: $text-muted; }
    SelectionList, OptionList {
        height: 1fr;
        border: none;
        background: transparent;
    }
    """
    BINDINGS = [
        Binding("enter", "next", "Next", show=False, priority=True),
        Binding("escape", "back", "Back", show=False, priority=True),
        Binding("ctrl+c,ctrl+d", "cancel", "Cancel", show=False, priority=True),
    ]

    def __init__(
        self,
        selection: ChangelogSelection,
        *,
        fragment_types: Mapping[str, str],
        type_bumps: Mapping[str, str],
        edit_news_fragment: Callable[[str, str], str] | None = None,
    ) -> None:
        super().__init__()
        self.selection = selection
        self.fragment_types = fragment_types
        self.type_bumps = type_bumps
        self.edit_news_fragment = edit_news_fragment
        self.step = WizardStep.PACKAGES
        self.type_index = 0
        self.type_packages: list[str] = []
        self.package_list: SelectionList[str] | None = None
        self.type_list: ChangeTypeList | None = None
        self.status = PACKAGE_STATUS
        self.draft_texts: dict[str, str] = {}
        self.escape_exit_timer: Timer | None = None

    def compose(self) -> ComposeResult:
        yield Vertical(id="body")
        yield Static("", id="status")

    async def on_mount(self) -> None:
        await self.render_step()

    async def action_next(self) -> None:
        if self.step is WizardStep.PACKAGES:
            self.clear_escape_exit()
            self.save_package_selection()
            self.type_packages = [package for package, _kind in self.selection.drafts()]
            if not self.type_packages:
                self.exit([])
                return
            self.type_index = 0
            self.step = WizardStep.TYPES
            await self.render_step()
            return

        self.save_current_type()
        if self.edit_current_package():
            await self.advance_after_edit()

    async def action_back(self) -> None:
        if self.step is WizardStep.PACKAGES:
            if self.escape_exit_timer is None:
                self.arm_escape_exit()
            else:
                self.exit([])
            return
        self.clear_escape_exit()
        self.save_current_type()
        if self.type_index > 0:
            self.type_index -= 1
        else:
            self.step = WizardStep.PACKAGES
        await self.render_step()

    def action_cancel(self) -> None:
        self.exit([])

    def arm_escape_exit(self) -> None:
        self.status = ESC_EXIT_STATUS
        self.update_status()
        self.escape_exit_timer = self.set_timer(
            ESC_EXIT_SECONDS,
            self.clear_escape_exit,
            name="escape-exit",
        )

    def clear_escape_exit(self) -> None:
        if self.escape_exit_timer is not None:
            self.escape_exit_timer.stop()
            self.escape_exit_timer = None
        if self.step is WizardStep.PACKAGES and self.status == ESC_EXIT_STATUS:
            self.status = PACKAGE_STATUS
            self.update_status()

    async def render_step(self) -> None:
        body = self.query_one("#body", Vertical)
        await body.remove_children()
        if self.step is WizardStep.PACKAGES:
            await body.mount(*self.package_widgets())
            self.status = PACKAGE_STATUS
            self.focus_packages()
        else:
            await body.mount(*self.type_widgets())
            self.status = TYPE_STATUS
            self.focus_types()
        self.update_status()

    def update_status(self) -> None:
        self.query_one("#status", Static).update(self.status)

    def focus_packages(self) -> None:
        if self.package_list is not None:
            self.package_list.focus()

    def focus_types(self) -> None:
        if self.type_list is not None:
            self.type_list.focus()

    def package_widgets(self) -> list[Widget]:
        selections = [
            Selection(
                self.package_label(state),
                state.name,
                state.selected and not state.covered,
                disabled=state.covered,
            )
            for state in self.selection.packages.values()
        ]
        self.package_list = SelectionList[str](*selections)
        return [
            Label("Select packages for news fragments"),
            self.package_list,
        ]

    def type_widgets(self) -> list[Widget]:
        package = self.type_packages[self.type_index]
        state = self.selection.packages[package]
        progress = f"{self.type_index + 1}/{len(self.type_packages)}"
        options = [
            Option(self.type_label(kind, selected=kind == state.kind), id=kind)
            for kind in self.fragment_types
        ]
        self.type_list = ChangeTypeList(*options)
        self.type_list.highlighted = self.type_list.get_option_index(state.kind)
        return [
            Label(Text.assemble("Choose change level for ", (package, "bold"), f" ({progress})")),
            self.type_list,
        ]

    def package_label(self, state: PackageNewsState) -> str:
        flags = []
        if state.changed:
            flags.append("changed")
        if state.covered:
            flags.append("covered")
        suffix = f" ({', '.join(flags)})" if flags else ""
        return f"{state.name}{suffix}"

    def type_label(self, kind: str, *, selected: bool = False) -> Text:
        title = self.fragment_types[kind]
        bump = self.type_bumps[kind]
        marker = "*" if selected else " "
        style = TYPE_STYLES.get(kind, "")
        return Text.assemble(
            f"[{marker}] ",
            (kind, style),
            " - ",
            (title, style),
            " ",
            (f"({bump})", "dim"),
        )

    def save_package_selection(self) -> None:
        if self.package_list is None:
            return
        selected = set(self.package_list.selected)
        for name, state in list(self.selection.packages.items()):
            self.replace_state(name, selected=not state.covered and name in selected)

    def save_current_type(self) -> None:
        if self.step is not WizardStep.TYPES:
            return
        if self.type_list is None:
            return
        highlighted = self.type_list.highlighted
        if highlighted is None:
            return
        option = self.type_list.get_option_at_index(highlighted)
        if option.id is None:
            return
        self.set_kind(self.type_packages[self.type_index], option.id)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list is not self.type_list:
            return
        event.stop()
        if event.option_id is None:
            return
        self.set_kind(self.type_packages[self.type_index], event.option_id)
        self.refresh_type_options()

    def on_option_list_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        # Keep the [*] marker in lockstep with the highlight: enter saves the
        # highlighted kind, so a marker left on another option would lie.
        if event.option_list is not self.type_list or event.option_id is None:
            return
        package = self.type_packages[self.type_index]
        if self.selection.packages[package].kind == event.option_id:
            return
        self.set_kind(package, event.option_id)
        self.refresh_type_options()

    def refresh_type_options(self) -> None:
        if self.type_list is None:
            return
        package = self.type_packages[self.type_index]
        selected_kind = self.selection.packages[package].kind
        for kind in self.fragment_types:
            self.type_list.replace_option_prompt(
                kind, self.type_label(kind, selected=kind == selected_kind)
            )

    def toggle(self, name: str) -> None:
        state = self.state(name)
        self.replace_state(name, selected=not state.selected)

    def add(self, name: str) -> None:
        self.state(name)
        self.replace_state(name, selected=True)

    def set_kind(self, name: str, kind: str) -> None:
        if kind not in self.fragment_types:
            expected = ", ".join(self.fragment_types)
            raise ValueError(f"invalid news fragment type {kind!r}; expected one of: {expected}")
        self.state(name)
        self.replace_state(name, kind=kind)

    def finish(self) -> None:
        if self.edit_news_fragment is None:
            self.exit([])
            return
        self.exit(self.drafts())

    def edit_current_package(self) -> bool:
        if self.edit_news_fragment is None:
            return True
        package = self.type_packages[self.type_index]
        kind = self.selection.packages[package].kind
        with self.suspend():
            try:
                text = self.edit_news_fragment(package, kind)
            except SystemExit as exc:
                self.status = self.editor_error_status(exc)
                self.update_status()
                self.focus_types()
                return False
        self.draft_texts[package] = text
        return True

    async def advance_after_edit(self) -> None:
        if self.type_index + 1 < len(self.type_packages):
            self.type_index += 1
            await self.render_step()
        else:
            self.finish()

    def drafts(self) -> list[NewsFragmentDraft]:
        return [
            NewsFragmentDraft(package=package, kind=kind, text=self.draft_texts[package])
            for package, kind in self.selection.drafts()
        ]

    def editor_error_status(self, error: SystemExit) -> str:
        message = str(error.code) if error.code not in (None, 1) else "editor failed"
        return f"{message}  enter retries  esc back  ctrl-c/ctrl-d exit"

    def replace_state(self, name: str, **updates: Any) -> None:
        self.selection.packages[name] = replace(self.state(name), **updates)

    def state(self, name: str) -> PackageNewsState:
        try:
            return self.selection.packages[name]
        except KeyError as exc:
            expected = ", ".join(self.selection.packages)
            raise ValueError(f"Unknown package {name!r}; expected one of: {expected}") from exc


def run_changelog_selection_app(
    selection: ChangelogSelection,
    *,
    fragment_types: Mapping[str, str],
    type_bumps: Mapping[str, str],
    edit_news_fragment: Callable[[str, str], str],
) -> list[NewsFragmentDraft]:
    app = ChangelogApp(
        selection,
        fragment_types=fragment_types,
        type_bumps=type_bumps,
        edit_news_fragment=edit_news_fragment,
    )
    try:
        result = app.run(mouse=True)
    except KeyboardInterrupt:
        return []
    return result or []


def changelog(diff: str = "tracked") -> int:
    packages_by_name = workspace.packages()
    changed = release.packages_for_paths(
        packages_by_name,
        release.collect_changelog_diff_paths(diff),
        code_only=True,
    )
    fragments = release.parse_fragments(set(packages_by_name))
    covered = {fragment.package for fragment in fragments}
    selection = initial_changelog_selection(packages_by_name, changed, covered)

    drafts = run_changelog_ui(selection, packages_by_name=packages_by_name, diff=diff)
    if not drafts:
        print("No news fragments selected.")
        return 0

    for draft in drafts:
        path = release.write_news_fragment(draft)
        print(f"Created {path.relative_to(release.ROOT)}")
    return 0


def initial_changelog_selection(
    packages_by_name: dict[str, workspace.Package], changed: set[str], covered: set[str]
) -> ChangelogSelection:
    ordered = workspace.topological_names(packages_by_name)
    return ChangelogSelection(
        {
            name: PackageNewsState(
                name=name,
                changed=name in changed,
                covered=name in covered,
                selected=name in changed and name not in covered,
            )
            for name in ordered
        }
    )


def run_changelog_ui(
    selection: ChangelogSelection,
    *,
    packages_by_name: dict[str, workspace.Package] | None = None,
    diff: str = "tracked",
) -> list[NewsFragmentDraft]:
    return run_changelog_selection_app(
        selection,
        fragment_types=release.FRAGMENT_TYPES,
        type_bumps=release.TYPE_BUMPS,
        edit_news_fragment=lambda package, kind: release.edit_news_fragment(
            package,
            kind,
            package_path=None if packages_by_name is None else packages_by_name[package].path,
            diff=diff,
        ),
    )
