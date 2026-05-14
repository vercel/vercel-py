from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict


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
