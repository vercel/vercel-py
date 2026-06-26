#!/usr/bin/env python3
from __future__ import annotations

from workspace_packages import workspace_packages


for package in workspace_packages():
    print(package)
