"""Detect coding agents from the environment of the current process.

The checks mirror the environment-variable rules in @vercel/detect-agent 1.2.5.
Its filesystem-based Devin check is intentionally outside this utility's scope.
"""

from __future__ import annotations

import os
from collections.abc import Mapping


def detect_agent_name(environ: Mapping[str, str] | None = None) -> str | None:
    """Return the agent driving this process, if its environment identifies one."""
    env = os.environ if environ is None else environ

    declared = env.get("AI_AGENT", "").strip()
    if declared:
        if declared in {"github-copilot", "github-copilot-cli"}:
            return "github-copilot"
        return declared

    if env.get("CURSOR_TRACE_ID"):
        return "cursor"
    if env.get("CURSOR_AGENT") or env.get("CURSOR_EXTENSION_HOST_ROLE") == "agent-exec":
        return "cursor-cli"
    if env.get("GEMINI_CLI"):
        return "gemini"
    if any(env.get(name) for name in ("CODEX_SANDBOX", "CODEX_CI", "CODEX_THREAD_ID")):
        return "codex"
    if env.get("ANTIGRAVITY_AGENT"):
        return "antigravity"
    if env.get("AUGMENT_AGENT"):
        return "augment-cli"
    if env.get("OPENCODE_CLIENT"):
        return "opencode"
    if env.get("CLAUDECODE") or env.get("CLAUDE_CODE"):
        return "cowork" if env.get("CLAUDE_CODE_IS_COWORK") else "claude"
    if env.get("REPL_ID"):
        return "replit"
    if any(
        env.get(name) for name in ("COPILOT_MODEL", "COPILOT_ALLOW_ALL", "COPILOT_GITHUB_TOKEN")
    ):
        return "github-copilot"
    return None
