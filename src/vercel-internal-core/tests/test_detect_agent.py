"""Tests for coding-agent detection from inherited environment variables."""

import pytest

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


def test_ai_agent_takes_precedence_and_preserves_custom_names() -> None:
    assert detect_agent_name({"AI_AGENT": " custom-agent@2 ", "CODEX_THREAD_ID": "thread"}) == (
        "custom-agent@2"
    )


def test_copilot_cli_name_is_normalized() -> None:
    assert detect_agent_name({"AI_AGENT": "github-copilot-cli"}) == "github-copilot"


def test_empty_ai_agent_falls_back_to_known_marker() -> None:
    assert detect_agent_name({"AI_AGENT": "  ", "CLAUDE_CODE": "1"}) == "claude"


def test_no_agent_when_environment_has_no_matching_marker() -> None:
    assert detect_agent_name({"AI_AGENT": " ", "CURSOR_AGENT": ""}) is None


def test_first_matching_known_marker_wins() -> None:
    assert detect_agent_name({"CURSOR_TRACE_ID": "trace", "CODEX_THREAD_ID": "thread"}) == (
        "cursor"
    )
