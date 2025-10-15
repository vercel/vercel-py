from __future__ import annotations

from typing import TypedDict
from dataclasses import dataclass


class ProjectInfo(TypedDict):
    projectId: str
    teamId: str | None


@dataclass
class VercelTokenResponse:
    token: str


@dataclass
class Credentials:
    token: str
    project_id: str
    team_id: str
