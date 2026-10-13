"""Tests for coding-agent detection from inherited environment variables."""

import pytest
from hypothesis import given, strategies as st

from vercel._internal.core.detect_agent import detect_agent_name


@pytest.mark.parametrize(
    ("environment", "agent"),
    [
        ({"CURSOR_TRACE_ID": "trace"}, "cursor"),
        ({"CURSOR_AGENT": "1"}, "cursor-cli"),
        ({"CURSOR_EXTENSION_HOST_ROLE": "agent-exec"}, "cursor-cli"),
        ({"GEMINI_CLI": "1"}, "gemini"),
        ({"CODEX_SANDBOX": "1"}, "codex"),
        ({"CODEX_CI": "1"}, "codex"),
        ({"CODEX_THREAD_ID": "thread"}, "codex"),
        ({"ANTIGRAVITY_AGENT": "1"}, "antigravity"),
        ({"AUGMENT_AGENT": "1"}, "augment-cli"),
        ({"OPENCODE_CLIENT": "1"}, "opencode"),
        ({"CLAUDECODE": "1"}, "claude"),
        ({"CLAUDE_CODE": "1", "CLAUDE_CODE_IS_COWORK": "1"}, "cowork"),
        ({"REPL_ID": "repl"}, "replit"),
        ({"COPILOT_MODEL": "model"}, "github-copilot"),
        ({"COPILOT_ALLOW_ALL": "1"}, "github-copilot"),
        ({"COPILOT_GITHUB_TOKEN": "token"}, "github-copilot"),
    ],
)
def test_known_agent_environment_markers(environment: dict[str, str], agent: str) -> None:
    assert detect_agent_name(environment) == agent


@pytest.mark.parametrize(
    ("declared", "agent"),
    [
        ("fx", "fx"),
        ("eve", "eve"),
        ("v0", "v0"),
        ("github-copilot", "github-copilot"),
        ("github-copilot-cli", "github-copilot"),
        (" custom-agent@2 ", "custom-agent@2"),
    ],
)
def test_ai_agent_declares_the_agent(declared: str, agent: str) -> None:
    assert detect_agent_name({"AI_AGENT": declared}) == agent


@given(
    declared=st.text(min_size=1).filter(lambda name: name.strip() not in {"", "github-copilot-cli"})
)
def test_ai_agent_takes_precedence_over_known_markers(declared: str) -> None:
    environment = {"AI_AGENT": declared, "CODEX_THREAD_ID": "thread", "CLAUDECODE": "1"}
    assert detect_agent_name(environment) == declared.strip()


def test_empty_ai_agent_falls_back_to_known_marker() -> None:
    assert detect_agent_name({"AI_AGENT": "  ", "CLAUDE_CODE": "1"}) == "claude"


def test_no_agent_when_environment_has_no_matching_marker() -> None:
    assert detect_agent_name({"AI_AGENT": " ", "CURSOR_AGENT": ""}) is None


def test_first_matching_known_marker_wins() -> None:
    assert detect_agent_name({"CURSOR_TRACE_ID": "trace", "CODEX_THREAD_ID": "thread"}) == (
        "cursor"
    )
